extern crate undermoon;

use futures::channel::mpsc;
use futures::{Future, SinkExt, StreamExt, TryStreamExt};
use std::net::SocketAddr;
use std::str;
use std::sync::Arc;
use tokio::macros::support::Pin;
use undermoon::protocol::{Array, BulkStr, Resp, RespPacket, RespVec};
use undermoon::proxy::backend::{
    BackendError, ConnFactory, ConnSink, ConnStream, CreateConnResult,
};

pub struct DummyOkConnFactory {
    handle_func: Arc<dyn Fn(Vec<String>) -> RespVec + Send + Sync + 'static>,
}

impl DummyOkConnFactory {
    pub fn new(handle_func: Arc<dyn Fn(Vec<String>) -> RespVec + Send + Sync + 'static>) -> Self {
        Self { handle_func }
    }
}

impl ConnFactory for DummyOkConnFactory {
    type Pkt = RespPacket;

    fn create_conn(
        &self,
        _addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>> {
        let (sender, receiver) = mpsc::unbounded();
        let handle_func = self.handle_func.clone();
        let receiver = receiver.map(move |packet: RespPacket| {
            let cmd: Vec<String> = match packet.to_resp_vec() {
                Resp::Arr(Array::Arr(resps)) => resps
                    .iter()
                    .map(|resp| match resp {
                        Resp::Bulk(BulkStr::Str(s)) => str::from_utf8(s).unwrap().to_string(),
                        _ => panic!(),
                    })
                    .collect(),
                _ => panic!(),
            };
            let resp = handle_func(cmd);
            Ok::<_, ()>(RespPacket::Data(resp))
        });
        let sink: ConnSink<RespPacket> = Box::pin(sender.sink_map_err(|_| BackendError::Canceled));
        let stream: ConnStream<RespPacket> = Box::pin(receiver.map_err(|_| BackendError::Canceled));
        Box::pin(async { Ok((sink, stream)) })
    }
}
