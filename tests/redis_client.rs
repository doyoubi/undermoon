extern crate undermoon;

use futures::{future, Future};
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use undermoon::protocol::{
    BinSafeStr, OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, RespVec,
};

pub struct DummyRedisClient {
    handle_func: Arc<dyn Fn(&str) -> RespVec + Send + Sync + 'static>,
}

impl DummyRedisClient {
    fn gen_reply(&self, cmd: Vec<BinSafeStr>) -> RespVec {
        let cmd_name = str::from_utf8(cmd[0].as_slice()).unwrap().to_uppercase();
        (self.handle_func)(cmd_name.as_str())
    }
}

impl RedisClient for DummyRedisClient {
    fn execute<'s>(
        &'s mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>>
    {
        let res = command.map(|cmd| self.gen_reply(cmd));
        Box::pin(async { Ok(res) })
    }
}

pub struct DummyClientFactory {
    handle_func: Arc<dyn Fn(&str) -> RespVec + Send + Sync + 'static>,
}

impl DummyClientFactory {
    pub fn new(handle_func: Arc<dyn Fn(&str) -> RespVec + Send + Sync + 'static>) -> Self {
        Self { handle_func }
    }
}

impl RedisClientFactory for DummyClientFactory {
    type Client = DummyRedisClient;

    fn create_client(
        &self,
        _address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
        let handle_func = self.handle_func.clone();
        Box::pin(future::ok(DummyRedisClient { handle_func }))
    }
}
