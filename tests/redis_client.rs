extern crate undermoon;

use futures::{future, Future};
use std::pin::Pin;
use std::str;
use undermoon::protocol::{BinSafeStr, OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec, BulkStr};

pub struct DummyRedisClient {}

impl DummyRedisClient {
    fn gen_reply(cmd: Vec<BinSafeStr>) -> RespVec {
        let cmd_name = str::from_utf8(cmd[0].as_slice()).unwrap().to_uppercase();
        match cmd_name.as_str() {
            "EXISTS" => Resp::Integer(b"0".to_vec()),
            "DUMP" => Resp::Bulk(BulkStr::Str(b"binary_format_xxx".to_vec())),
            "RESTORE" => Resp::Simple(b"OK".to_vec()),
            "PTTL" => Resp::Integer(b"-1".to_vec()),
            "UMCTL" => Resp::Simple(b"OK".to_vec()),
            _ => Resp::Simple(b"OK".to_vec()),
        }
    }
}

impl RedisClient for DummyRedisClient {
    fn execute<'s>(
        &'s mut self,
        command: OptionalMulti<Vec<BinSafeStr>>,
    ) -> Pin<Box<dyn Future<Output = Result<OptionalMulti<RespVec>, RedisClientError>> + Send + 's>>
    {
        let res = command.map(|cmd| Self::gen_reply(cmd));
        Box::pin(async { Ok(res) })
    }
}

pub struct DummyClientFactory {}

impl RedisClientFactory for DummyClientFactory {
    type Client = DummyRedisClient;

    fn create_client(
        &self,
        _address: String,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Client, RedisClientError>> + Send>> {
        Box::pin(future::ok(DummyRedisClient {}))
    }
}
