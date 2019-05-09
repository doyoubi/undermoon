use futures::{future, stream, Future, Stream};
use futures_timer::Delay;
use protocol::{RedisClient, RedisClientError, RedisClientFactory, Resp};
use std::iter;
use std::str;
use std::sync::Arc;
use std::time::Duration;

pub fn keep_retrying_and_sending<F: RedisClientFactory>(
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<String>,
    interval: Duration,
) -> impl Future<Item = (), Error = RedisClientError> {
    keep_connecting_and_sending(client_factory, address, cmd, interval, retry_handle_func)
}

pub fn keep_connecting_and_sending<F: RedisClientFactory, Func>(
    client_factory: Arc<F>,
    address: String,
    cmd: Vec<String>,
    interval: Duration,
    handle_result: Func,
) -> impl Future<Item = (), Error = RedisClientError>
where
    Func: Clone + Fn(Resp) -> Result<(), RedisClientError>,
{
    let infinite_stream = stream::iter_ok(iter::repeat(()));
    infinite_stream.for_each(move |()| {
        let client_fut = client_factory.create_client(address.clone());
        let cmd_clone = cmd.clone();
        let cmd_clone2 = cmd.clone();
        let interval_clone = interval;
        let handle_result_clone = handle_result.clone();
        client_fut
            .and_then(move |client| {
                keep_sending_cmd(client, cmd_clone, interval_clone, handle_result_clone)
            })
            .map_err(move |e| {
                match e {
                    RedisClientError::Done => {
                        info!("stop keep sending commands {:?} {:?}", e, cmd_clone2)
                    }
                    _ => error!("failed to keep sending commands {:?} {:?}", e, cmd_clone2),
                }
                e
            })
    })
}

pub fn keep_sending_cmd<C: RedisClient, Func>(
    client: C,
    cmd: Vec<String>,
    interval: Duration,
    handle_result: Func,
) -> impl Future<Item = (), Error = RedisClientError>
where
    Func: Clone + Fn(Resp) -> Result<(), RedisClientError>,
{
    let infinite_stream = stream::iter_ok(iter::repeat(()));
    infinite_stream
        .fold(client, move |client, ()| {
            let byte_cmd = cmd.iter().map(|s| s.clone().into_bytes()).collect();
            let delay = Delay::new(interval).map_err(RedisClientError::Io);
            // debug!("sending cmd {:?}", cmd);
            let handle_result_clone = handle_result.clone();
            let exec_fut = client
                .execute(byte_cmd)
                .map_err(|e| {
                    error!("failed to send: {}", e);
                    e
                })
                .and_then(move |(client, response)| {
                    future::result(handle_result_clone(response).map(|()| client))
                });
            exec_fut.join(delay).map(move |(client, ())| client)
        })
        .map(|_| ())
}

fn retry_handle_func(response: Resp) -> Result<(), RedisClientError> {
    if let Resp::Error(err) = response {
        let err_str = str::from_utf8(&err)
            .map(ToString::to_string)
            .unwrap_or_else(|_| format!("{:?}", err));
        error!("error reply: {}", err_str);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::common::utils::ThreadSafe;
    use ::protocol::BinSafeStr;
    use futures::future;

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
            self,
            _command: Vec<BinSafeStr>,
        ) -> Box<dyn Future<Item = (Self, Resp), Error = RedisClientError> + Send + 'static>
        {
            let client = self;
            if client.counter.count.load(Ordering::SeqCst) < client.counter.max_count {
                client.counter.count.fetch_add(1, Ordering::SeqCst);
                Box::new(future::ok((
                    client,
                    Resp::Simple("OK".to_string().into_bytes()),
                )))
            } else {
                Box::new(future::err(RedisClientError::Closed))
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
        ) -> Box<dyn Future<Item = Self::Client, Error = RedisClientError> + Send + 'static>
        {
            Box::new(future::ok(DummyRedisClient::new(self.counter.clone())))
        }
    }

    #[test]
    fn test_keep_sending_cmd() {
        let interval = Duration::new(0, 0);
        let counter = Arc::new(Counter::new(3));
        let res = keep_sending_cmd(
            DummyRedisClient::new(counter.clone()),
            vec![],
            interval,
            retry_handle_func,
        )
        .wait();
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
                Err(RedisClientError::Closed)
            }
        };
        let factory = Arc::new(DummyClientFactory::new(counter.clone()));
        let res = keep_connecting_and_sending(
            factory,
            "host:port".to_string(),
            vec![],
            interval,
            handler,
        )
        .wait();
        assert!(res.is_err());
        assert_eq!(counter.count.load(Ordering::SeqCst), 3);
        assert_eq!(retry_counter_clone.count.load(Ordering::SeqCst), 2);
    }
}
