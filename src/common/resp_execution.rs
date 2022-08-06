use crate::common::utils::pretty_print_bytes;
use crate::protocol::{
    BinSafeStr, OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec,
};
use futures::channel::oneshot;
use futures::{select, Future, FutureExt};
use parking_lot::Mutex;
use std::pin::Pin;
use std::str;
use std::sync::atomic;
use std::sync::Arc;
use std::time::Duration;

pub async fn keep_connecting_and_sending_cmd_with_cached_client<F: RedisClientFactory, Func>(
    client: Option<F::Client>,
    client_factory: Arc<F>,
    address: String,
    opt_multi_cmd: OptionalMulti<Vec<BinSafeStr>>,
    interval: Duration,
    handle_result: Func,
) -> F::Client
where
    Func: Clone + Fn(OptionalMulti<RespVec>) -> Result<(), RedisClientError>,
{
    let mut client = client;
    loop {
        let mut c = if let Some(c) = client.take() {
            c
        } else {
            match client_factory.create_client(address.clone()).await {
                Ok(c) => c,
                Err(err) => {
                    error!("failed to create client: {:?}", err);
                    tokio::time::sleep(interval).await;
                    continue;
                }
            }
        };

        match keep_sending_cmd(
            &mut c,
            opt_multi_cmd.clone(),
            interval,
            handle_result.clone(),
        )
        .await
        {
            Ok(()) => {
                client = Some(c);
            }
            Err(RedisClientError::Done) => return c,
            Err(err) => {
                let debug_cmd = opt_multi_cmd.clone().map(|cmd| {
                    cmd.iter()
                        .map(|b| pretty_print_bytes(b))
                        .collect::<Vec<String>>()
                });
                error!(
                    "failed to send commands {:?} {:?}. Try again.",
                    err, debug_cmd
                );
            }
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn keep_connecting_and_sending_cmd<F: RedisClientFactory, Func>(
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<Vec<u8>>,
    interval: Duration,
    handle_result: Func,
) where
    Func: Clone + Fn(RespVec) -> Result<(), RedisClientError>,
{
    let handler = move |opt_multi_cmd| match opt_multi_cmd {
        OptionalMulti::Single(r) => handle_result(r),
        OptionalMulti::Multi(v) => {
            error!("unexpected multiple replies: {:?}", v);
            Err(RedisClientError::InvalidReply)
        }
    };
    keep_connecting_and_sending_cmd_with_cached_client(
        None,
        client_factory,
        address,
        OptionalMulti::Single(cmd),
        interval,
        handler,
    )
    .await;
}

pub async fn keep_sending_cmd<C: RedisClient, Func>(
    client: &mut C,
    opt_mul_cmd: OptionalMulti<Vec<BinSafeStr>>,
    interval: Duration,
    handle_result: Func,
) -> Result<(), RedisClientError>
where
    Func: Fn(OptionalMulti<RespVec>) -> Result<(), RedisClientError>,
{
    loop {
        let response = match client.execute(opt_mul_cmd.clone()).await {
            Ok(response) => response,
            Err(err) => return Err(err),
        };
        handle_result(response)?;
        tokio::time::sleep(interval).await;
    }
}

pub fn retry_handle_func(response: OptionalMulti<RespVec>) -> Result<(), RedisClientError> {
    response.map(|resp| {
        if let Resp::Error(err) = resp {
            let err_str = str::from_utf8(&err)
                .map(ToString::to_string)
                .unwrap_or_else(|_| format!("{:?}", err));
            error!("error reply: {}", err_str);
        }
    });
    Ok(())
}

pub async fn keep_connecting_and_sending<T: Send + Clone, F: RedisClientFactory, Func>(
    data: T,
    client_factory: Arc<F>,
    address: String,
    interval: Duration,
    send_func: Func,
) -> T
// dyn Trait has default 'static lifetime.
// '_ would use the lifetime of &mut F::Client instead.
where
    Func: Clone
        + Send
        + Fn(
            T,
            &mut F::Client,
        ) -> Pin<Box<dyn Future<Output = Result<T, RedisClientError>> + Send + '_>>,
{
    let mut data = data;
    loop {
        let mut client = match client_factory.create_client(address.clone()).await {
            Ok(client) => client,
            Err(err) => {
                error!("failed to create redis client: {:?}", err);
                tokio::time::sleep(interval).await;
                continue;
            }
        };
        loop {
            data = match send_func(data.clone(), &mut client).await {
                Ok(d) => d,
                Err(RedisClientError::Done) => return data.clone(),
                Err(err) => {
                    error!("failed to send: {:?}. Try again", err);
                    break;
                }
            };
            tokio::time::sleep(interval).await;
        }
        tokio::time::sleep(interval).await;
    }
}

type RetrieverFut = Pin<Box<dyn Future<Output = Result<(), RedisClientError>> + Send>>;

pub struct I64Retriever<F: RedisClientFactory> {
    data: Arc<atomic::AtomicI64>,
    stop_signal_sender: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    stop_signal_receiver: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> I64Retriever<F> {
    pub fn new(init_data: i64, client_factory: Arc<F>) -> Self {
        let (sender, receiver) = oneshot::channel();
        let data = Arc::new(atomic::AtomicI64::new(init_data));

        let stop_signal_sender = Arc::new(Mutex::new(Some(sender)));
        let stop_signal_receiver = Arc::new(Mutex::new(Some(receiver)));
        Self {
            data,
            stop_signal_sender,
            stop_signal_receiver,
            client_factory,
        }
    }

    pub fn get_data(&self) -> i64 {
        self.data.load(atomic::Ordering::SeqCst)
    }

    pub fn start<Func>(
        &self,
        handle_func: Func,
        address: String,
        cmd: Vec<String>,
        interval: Duration,
    ) -> Option<RetrieverFut>
    where
        Func: Fn(RespVec, &Arc<atomic::AtomicI64>) -> Result<(), RedisClientError>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        if let Some(stop_signal_receiver) = self.stop_signal_receiver.lock().take() {
            let data_clone = self.data.clone();
            let handle_result = move |resp: RespVec| -> Result<(), RedisClientError> {
                handle_func(resp, &data_clone)
            };
            let cmd: Vec<Vec<u8>> = cmd.into_iter().map(|e| e.into_bytes()).collect();
            let sending = keep_connecting_and_sending_cmd(
                self.client_factory.clone(),
                address,
                cmd,
                interval,
                handle_result,
            );
            // For `select!`
            #[allow(clippy::panic)]
            let fut = async {
                select! {
                    () = sending.fuse() => Ok(()),
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
        match self.stop_signal_sender.lock().take() {
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
    use crate::protocol::BinSafeStr;
    use crate::protocol::{OptionalMulti, Resp};
    use futures::future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio;

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

    impl RedisClient for DummyRedisClient {
        fn execute<'s>(
            &'s mut self,
            _command: OptionalMulti<Vec<BinSafeStr>>,
        ) -> Pin<
            Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
        > {
            let client = self;
            if client.counter.count.load(Ordering::SeqCst) < client.counter.max_count {
                client.counter.count.fetch_add(1, Ordering::SeqCst);
                // Only works for single command
                Box::pin(async {
                    Ok(OptionalMulti::Single(Resp::Simple(
                        "OK".to_string().into_bytes(),
                    )))
                })
            } else {
                Box::pin(async { Err(RedisClientError::Closed) })
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

    impl RedisClientFactory for DummyClientFactory {
        type Client = DummyRedisClient;

        fn create_client(
            &self,
            _address: String,
        ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
            Box::pin(future::ok(DummyRedisClient::new(self.counter.clone())))
        }
    }

    #[tokio::test]
    async fn test_keep_sending_cmd() {
        let interval = Duration::new(0, 0);
        let counter = Arc::new(Counter::new(3));
        let mut client = DummyRedisClient::new(counter.clone());
        let res = keep_sending_cmd(
            &mut client,
            OptionalMulti::Single(vec![]),
            interval,
            retry_handle_func,
        )
        .await;
        assert!(res.is_err());
        assert_eq!(counter.count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_keep_connecting_and_sending() {
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
        keep_connecting_and_sending_cmd(
            factory,
            "host:port".to_string(),
            vec![],
            interval,
            handler,
        )
        .await;
        assert_eq!(counter.count.load(Ordering::SeqCst), 3);
        assert_eq!(retry_counter_clone.count.load(Ordering::SeqCst), 2);
    }
}
