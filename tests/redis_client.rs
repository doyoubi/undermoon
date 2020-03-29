extern crate undermoon;

use std::pin::Pin;
use futures::{Future, future};
use undermoon::protocol::{RedisClient, OptionalMulti, BinSafeStr, RespVec, RedisClientError, Resp, RedisClientFactory};

pub struct DummyOkRedisClient {}

impl DummyOkRedisClient {
}

impl RedisClient for DummyOkRedisClient {
    fn execute<'s>(
        &'s mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<
        Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>,
    > {
        let ok = Resp::Simple(b"OK".to_vec());
        let res = command.map(|_| ok.clone());
        Box::pin(async {Ok(res)})
    }
}

pub struct DummyOkClientFactory {}

impl RedisClientFactory for DummyOkClientFactory {
    type Client = DummyOkRedisClient;

    fn create_client(
        &self,
        _address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
        Box::pin(future::ok(DummyOkRedisClient{}))
    }
}