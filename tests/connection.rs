extern crate undermoon;

use futures::channel::mpsc;
use futures::{Future, SinkExt, StreamExt, TryStreamExt};
use std::net::SocketAddr;
use tokio::macros::support::Pin;
use undermoon::protocol::{Resp, RespPacket};
use undermoon::proxy::backend::{
    BackendError, ConnFactory, ConnSink, ConnStream, CreateConnResult,
};

pub struct DummyOkConnFactory {}

impl ConnFactory for DummyOkConnFactory {
    type Pkt = RespPacket;

    fn create_conn(
        &self,
        _addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = CreateConnResult<Self::Pkt>> + Send>> {
        let (sender, receiver) = mpsc::unbounded();
        let receiver =
            receiver.map(|_| Ok::<_, ()>(RespPacket::Data(Resp::Simple(b"OK".to_vec()))));
        let sink: ConnSink<RespPacket> = Box::pin(sender.sink_map_err(|_| BackendError::Canceled));
        let stream: ConnStream<RespPacket> = Box::pin(receiver.map_err(|_| BackendError::Canceled));
        Box::pin(async { Ok((sink, stream)) })
    }
}
