extern crate undermoon;

mod connection;
mod redis_client;


#[cfg(test)]
mod tests {
    use super::*;

    use tokio;
    use redis_client::DummyOkClientFactory;
    use connection::DummyOkConnFactory;
    use arc_swap::ArcSwap;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, RwLock};
    use std::sync::atomic::AtomicI64;
    use std::convert::TryFrom;
    use std::str;
    use undermoon::proxy::manager::MetaManager;
    use undermoon::proxy::service::ServerProxyConfig;
    use undermoon::proxy::manager::MetaMap;
    use undermoon::common::track::TrackedFutureRegistry;
    use undermoon::proxy::session::CmdCtx;
    use undermoon::common::cluster::ClusterName;
    use undermoon::proxy::command::{Command, new_command_pair, CmdReplyReceiver};
    use undermoon::protocol::{RespPacket, Resp, Array, BulkStr};
    use undermoon::common::response::ERR_CLUSTER_NOT_FOUND;

    fn gen_config() -> ServerProxyConfig {
        ServerProxyConfig {
            address: "localhost:5299".to_string(),
            announce_address: "localhost:5299".to_string(),
            auto_select_cluster: true,
            slowlog_len: NonZeroUsize::new(1024).unwrap(),
            slowlog_log_slower_than: AtomicI64::new(0),
            thread_number: NonZeroUsize::new(2).unwrap(),
            session_channel_size: 1024,
            backend_channel_size: 1024,
            backend_conn_num: NonZeroUsize::new(2).unwrap(),
            backend_batch_min_time: 10000,
            backend_batch_max_time: 10000,
            backend_batch_buf: NonZeroUsize::new(50).unwrap(),
            session_batch_min_time: 10000,
            session_batch_max_time: 10000,
            session_batch_buf: NonZeroUsize::new(50).unwrap(),
        }
    }

    fn gen_testing_manager() -> MetaManager<DummyOkClientFactory, DummyOkConnFactory> {
        let config = Arc::new(gen_config());
        let client_factory = Arc::new(DummyOkClientFactory{});
        let conn_factory = Arc::new(DummyOkConnFactory{});
        let meta_map = Arc::new(ArcSwap::new(Arc::new(MetaMap::new())));
        let future_registry = Arc::new(TrackedFutureRegistry::default());
        MetaManager::new(config, client_factory, conn_factory, meta_map, future_registry)
    }

    fn gen_set_command() -> (CmdCtx, CmdReplyReceiver) {
        let cluster_name = Arc::new(RwLock::new(ClusterName::try_from("test_cluster").unwrap()));
        let resp = RespPacket::Data(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"SET".to_vec())),
            Resp::Bulk(BulkStr::Str(b"key".to_vec())),
            Resp::Bulk(BulkStr::Str(b"value".to_vec())),
        ])));
        let command = Command::new(Box::new(resp));
        let (s, r) = new_command_pair(&command);
        let cmd_ctx = CmdCtx::new(cluster_name, command, s, 233);
        (cmd_ctx, r)
    }

    #[tokio::test]
    async fn test_cluster_not_found() {
        let manager = gen_testing_manager();
        let (cmd_ctx, reply_receiver) = gen_set_command();
        manager.send(cmd_ctx);
        let result = reply_receiver.await;
        assert!(result.is_ok());
        let (_, response, _) = result.unwrap().into_inner();
        let resp = response.into_resp_vec();
        let err = match resp {
            Resp::Error(err_str) => err_str,
            other => panic!(format!("unexpected pattern {:?}", other)),
        };
        let err_str = str::from_utf8(&err).unwrap();
        assert!(err_str.starts_with(ERR_CLUSTER_NOT_FOUND));
    }
}