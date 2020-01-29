use crate::common::utils::pretty_print_bytes;
use crate::protocol::{RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec, BinSafeStr};
use atomic_option::AtomicOption;
use futures::channel::oneshot;
use futures::{Future, FutureExt, select};
use futures_timer::Delay;
use std::str;
use std::sync::atomic;
use std::sync::Arc;
use std::time::Duration;
use std::pin::Pin;

pub async fn keep_connecting_and_sending_cmd_with_cached_client<F: RedisClientFactory, Func>(
    client: Option<F::Client>,
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<BinSafeStr>,
    interval: Duration,
    handle_result: Func,
) -> Result<Option<F::Client>, RedisClientError>
    where
        Func: Clone + Fn(RespVec) -> Result<(), RedisClientError>,
{
    let mut client = client;
    loop {
        let mut c = if let Some(c) = client.take() {
            c
        } else {
            client_factory.create_client(address.clone()).await?
        };
        match keep_sending_cmd(&mut c, cmd.clone(), interval, handle_result.clone()).await {
            Ok(()) => {
                client = Some(c);
            },
            Err(RedisClientError::Done) => return Err(RedisClientError::Done),
            Err(err) => {
                error!(
                    "failed to send commands {:?} {:?}. Try again.",
                    err,
                    cmd.iter()
                        .map(|b| pretty_print_bytes(&b))
                        .collect::<Vec<String>>()
                );
            }
        }
    }
}

pub async fn keep_connecting_and_sending_cmd<F: RedisClientFactory, Func>(
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<Vec<u8>>,
    interval: Duration,
    handle_result: Func,
) -> Result<Option<F::Client>, RedisClientError>
    where
        Func: Clone + Fn(RespVec) -> Result<(), RedisClientError>,
{
    keep_connecting_and_sending_cmd_with_cached_client(
        None,
        client_factory,
        address,
        cmd,
        interval,
        handle_result,
    ).await
}

pub async fn keep_sending_cmd<C: RedisClient, Func>(
    client: &mut C,
    cmd: Vec<BinSafeStr>,
    interval: Duration,
    handle_result: Func,
) -> Result<(), RedisClientError>
    where
        Func: Fn(RespVec) -> Result<(), RedisClientError>,
{
    loop {
        let response = match client.execute(cmd.clone()).await {
            Ok(response) => response,
            Err(err) => return Err(err),
        };
        handle_result(response)?;
        Delay::new(interval).await;
    }
}

pub fn retry_handle_func(response: RespVec) -> Result<(), RedisClientError> {
    if let Resp::Error(err) = response {
        let err_str = str::from_utf8(&err)
            .map(ToString::to_string)
            .unwrap_or_else(|_| format!("{:?}", err));
        error!("error reply: {}", err_str);
    }
    Ok(())
}

pub async fn keep_connecting_and_sending<T: Send + Clone, F: RedisClientFactory, Func>(
    data: T,
    client_factory: Arc<F>,
    address: String,
    interval: Duration,
    send_func: Func,
) -> Result<T, RedisClientError>
    where
        Func: Clone
        + Send
        + Fn(T, &'static mut F::Client) -> Pin<Box<dyn Future<Output = Result<T, RedisClientError>> + Send>>,
{
    let mut data = data;
    loop {
        let mut client = match client_factory.create_client(address.clone()).await {
            Ok(client) => client,
            Err(err) => {
                Delay::new(interval).await;
                continue
            }
        };
        data = match keep_sending(data.clone(), &mut client, interval, send_func.clone()).await {
            Ok(data) => data,
            Err((_, RedisClientError::Done)) => return Err(RedisClientError::Done),
            Err((data, e)) => {
                error!("failed to send {:?}. Try again.", e);
                data
            }
        };
        Delay::new(interval).await;
    }
}

pub async fn keep_sending<T: Clone + Send, C: RedisClient, Func>(
    data: T,
    client: &'static mut C,
    interval: Duration,
    send_func: Func,
) -> Result<T, (T, RedisClientError)>
    where
        Func: Send + Fn(T, &'static mut C) -> Pin<Box<dyn Future<Output = Result<T, RedisClientError>> + Send>>,
{
    let mut data = data;
    loop {
        data = match send_func(data, client).await {
            Ok(data) => data,
            Err(err) => return Err((data, err)),
        };
        Delay::new(interval).await;
    }
}

pub struct I64Retriever<F: RedisClientFactory> {
    data: Arc<atomic::AtomicI64>,
    stop_signal_sender: AtomicOption<oneshot::Sender<()>>,
    stop_signal_receiver: AtomicOption<oneshot::Receiver<()>>,
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<Vec<u8>>,
    interval: Duration,
}

impl<F: RedisClientFactory> I64Retriever<F>  {
    pub fn new(
        init_data: i64,
        client_factory: Arc<F>,
        address: String,
        cmd: Vec<String>,
        interval: Duration,
    ) -> Self {
        let (sender, receiver) = oneshot::channel();
        let data = Arc::new(atomic::AtomicI64::new(init_data));

        let stop_signal_sender = AtomicOption::new(Box::new(sender));
        let stop_signal_receiver = AtomicOption::new(Box::new(receiver));
        Self {
            data,
            stop_signal_sender,
            stop_signal_receiver,
            client_factory,
            address,
            cmd: cmd.into_iter().map(|e| e.into_bytes()).collect(),
            interval,
        }
    }

    pub fn get_data(&self) -> i64 {
        self.data.load(atomic::Ordering::SeqCst)
    }

    pub fn start<Func>(
        &self,
        handle_func: Func,
    ) -> Option<Pin<Box<dyn Future<Output = Result<(), RedisClientError>> + Send>>>
    where
        Func: Fn(RespVec, &Arc<atomic::AtomicI64>) -> Result<(), RedisClientError>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        if let Some(stop_signal_receiver) = self.stop_signal_receiver.take(atomic::Ordering::SeqCst)
        {
            let data_clone = self.data.clone();
            let handle_result = move |resp: RespVec| -> Result<(), RedisClientError> {
                handle_func(resp, &data_clone)
            };
            let sending = keep_connecting_and_sending_cmd(
                self.client_factory.clone(),
                self.address.clone(),
                self.cmd.clone(),
                self.interval,
                handle_result,
            );
            let fut = async {
                select! {
                    res = sending.fuse() => res.map(|_| ()),
                    _ = stop_signal_receiver.fuse() => Err(RedisClientError::Canceled),
                }
            };
            Some(Box::pin(fut))
        } else {
            None
        }
    }

    pub fn stop(&self) -> bool {
        if !self.try_stop() {
            debug!("Failed to stop I64Retriever. Maybe it has been stopped.");
            false
        } else {
            true
        }
    }

    pub fn try_stop(&self) -> bool {
        match self.stop_signal_sender.take(atomic::Ordering::SeqCst) {
            Some(sender) => sender.send(()).is_ok(),
            None => false,
        }
    }
}

impl<F: RedisClientFactory> Drop for I64Retriever<F> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use futures::future;
    use tokio::runtime::Runtime;
    use crate::common::utils::ThreadSafe;
    use crate::protocol::BinSafeStr;
    use crate::protocol::Resp;

    #[derive(Debug)]
    struct Counter {
        pub count: AtomicUsize,
        pub max_count: usize,
    }

    impl Counter {
        fn new(max_count: usize) -> Self {
            Self {
                max_count,
                count: AtomicUsize::new(0),
            }
        }
    }

    #[derive(Debug)]
    struct DummyRedisClient {
        counter: Arc<Counter>,
    }

    impl DummyRedisClient {
        fn new(counter: Arc<Counter>) -> Self {
            Self { counter }
        }
    }

    impl ThreadSafe for DummyRedisClient {}

    impl RedisClient for DummyRedisClient {
        fn execute(
            &mut self,
            _command: Vec<BinSafeStr>,
        ) -> Pin<Box<dyn Future<Output = Result<RespVec, RedisClientError>> + Send>>
        {
            let client = self;
            if client.counter.count.load(Ordering::SeqCst) < client.counter.max_count {
                client.counter.count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {Ok(Resp::Simple("OK".to_string().into_bytes()))})
            } else {
                Box::pin(async {Err(RedisClientError::Closed)})
            }
        }
    }

    struct DummyClientFactory {
        counter: Arc<Counter>,
    }

    impl DummyClientFactory {
        fn new(counter: Arc<Counter>) -> Self {
            Self { counter }
        }
    }

    impl ThreadSafe for DummyClientFactory {}

    impl RedisClientFactory for DummyClientFactory {
        type Client = DummyRedisClient;

        fn create_client(
            &self,
            _address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>>
        {
            Box::pin(future::ok(DummyRedisClient::new(self.counter.clone())))
        }
    }

    #[test]
    fn test_keep_sending_cmd() {
        let interval = Duration::new(0, 0);
        let counter = Arc::new(Counter::new(3));
        let mut client = DummyRedisClient::new(counter.clone());
        let mut rt = Runtime::new().expect("test_keep_sending_cmd");
        let fut = keep_sending_cmd(
            &mut client,
            vec![],
            interval,
            retry_handle_func,
        );
        let res = rt.block_on(fut);
        assert!(res.is_err());
        assert_eq!(counter.count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_keep_connecting_and_sending() {
        let interval = Duration::new(0, 0);
        let counter = Arc::new(Counter::new(3));
        let retry_counter = Arc::new(Counter::new(2));
        let retry_counter_clone = retry_counter.clone();
        let handler = move |_result| {
            if retry_counter.count.load(Ordering::SeqCst) < retry_counter.max_count {
                retry_counter.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            } else {
                Err(RedisClientError::Done)
            }
        };
        let factory = Arc::new(DummyClientFactory::new(counter.clone()));
        let mut rt = Runtime::new().expect("test_keep_sending_cmd");
        let fut = keep_connecting_and_sending_cmd(
            factory,
            "host:port".to_string(),
            vec![],
            interval,
            handler,
        );
        let res = rt.block_on(fut);
        assert!(res.is_ok());
        assert_eq!(counter.count.load(Ordering::SeqCst), 3);
        assert_eq!(retry_counter_clone.count.load(Ordering::SeqCst), 2);
    }
}
