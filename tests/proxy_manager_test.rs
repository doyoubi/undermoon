extern crate undermoon;

mod connection;
mod redis_client;

#[cfg(test)]
mod tests {
    use super::*;

    use arc_swap::ArcSwap;
    use connection::DummyOkConnFactory;
    use futures_timer::Delay;
    use redis_client::DummyClientFactory;
    use std::convert::TryFrom;
    use std::num::NonZeroUsize;
    use std::str;
    use std::sync::atomic::AtomicI64;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio;
    use undermoon::common::cluster::{
        ClusterName, MigrationMeta, MigrationTaskMeta, Range, RangeList, SlotRange, SlotRangeTag,
    };
    use undermoon::common::proto::ProxyClusterMeta;
    use undermoon::common::response::{
        ERR_BACKEND_CONNECTION, ERR_CLUSTER_NOT_FOUND, ERR_MOVED, OK_REPLY,
    };
    use undermoon::common::track::TrackedFutureRegistry;
    use undermoon::common::utils::pretty_print_bytes;
    use undermoon::common::version::UNDERMOON_MIGRATION_VERSION;
    use undermoon::migration::task::{MgrSubCmd, MigrationState, SwitchArg};
    use undermoon::protocol::{Array, BinSafeStr, BulkStr, Resp, RespPacket, RespVec, VFunctor};
    use undermoon::proxy::command::{new_command_pair, CmdReplyReceiver, Command};
    use undermoon::proxy::manager::MetaManager;
    use undermoon::proxy::manager::MetaMap;
    use undermoon::proxy::service::ServerProxyConfig;
    use undermoon::proxy::session::CmdCtx;

    const TEST_CLUSTER: &str = "test_cluster";
    type TestMetaManager = MetaManager<DummyClientFactory, DummyOkConnFactory>;

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
            // Should only be 1 so that when `wait_backend_ready` is done,
            // the whole backend is ready.
            backend_conn_num: NonZeroUsize::new(1).unwrap(),
            backend_batch_min_time: 10000,
            backend_batch_max_time: 10000,
            backend_batch_buf: NonZeroUsize::new(50).unwrap(),
            session_batch_min_time: 10000,
            session_batch_max_time: 10000,
            session_batch_buf: NonZeroUsize::new(50).unwrap(),
        }
    }

    fn gen_testing_manager(
        handle_func: Arc<dyn Fn(&str) -> RespVec + Send + Sync + 'static>,
    ) -> TestMetaManager {
        let config = Arc::new(gen_config());
        let client_factory = Arc::new(DummyClientFactory::new(handle_func.clone()));
        let conn_factory = Arc::new(DummyOkConnFactory::new(handle_func));
        let meta_map = Arc::new(ArcSwap::new(Arc::new(MetaMap::empty())));
        let future_registry = Arc::new(TrackedFutureRegistry::default());
        MetaManager::new(
            config,
            client_factory,
            conn_factory,
            meta_map,
            future_registry,
        )
    }

    fn gen_set_command(key: BinSafeStr) -> (CmdCtx, CmdReplyReceiver) {
        let cluster_name = Arc::new(RwLock::new(ClusterName::try_from(TEST_CLUSTER).unwrap()));
        let resp = RespPacket::Data(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"SET".to_vec())),
            Resp::Bulk(BulkStr::Str(key)),
            Resp::Bulk(BulkStr::Str(b"value".to_vec())),
        ])));
        let command = Command::new(Box::new(resp));
        let (s, r) = new_command_pair(&command);
        let cmd_ctx = CmdCtx::new(cluster_name, command, s, 233);
        (cmd_ctx, r)
    }

    fn gen_proxy_cluster_meta() -> ProxyClusterMeta {
        let mut iter = "1 NOFLAGS test_cluster 127.0.0.1:6379 1 0-16383"
            .split(' ')
            .map(|s| s.to_string())
            .peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    #[tokio::test]
    async fn test_cluster_not_found() {
        let manager = gen_testing_manager(Arc::new(always_ok));
        let (cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());

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

    async fn wait_backend_ready(manager: &TestMetaManager) {
        loop {
            let (cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());
            manager.send(cmd_ctx);

            let result = reply_receiver.await;
            let (_, response, _) = result.unwrap().into_inner();
            let resp = response.into_resp_vec();
            match resp {
                Resp::Error(err_str)
                    if str::from_utf8(err_str.as_slice())
                        .unwrap()
                        .starts_with(ERR_BACKEND_CONNECTION) =>
                {
                    // The connection future is not ready.
                    Delay::new(Duration::from_millis(1)).await;
                    continue;
                }
                _ => break,
            };
        }
    }

    pub fn always_ok(_: &str) -> RespVec {
        Resp::Simple(b"OK".to_vec())
    }

    pub fn handle_migration_command(cmd_name: &str) -> RespVec {
        match cmd_name {
            "EXISTS" => Resp::Integer(b"0".to_vec()),
            "DUMP" => Resp::Bulk(BulkStr::Str(b"binary_format_xxx".to_vec())),
            "RESTORE" => Resp::Simple(b"OK".to_vec()),
            "PTTL" => Resp::Integer(b"-1".to_vec()),
            "UMCTL" => Resp::Simple(b"OK".to_vec()),
            _ => Resp::Simple(b"OK".to_vec()),
        }
    }

    #[tokio::test]
    async fn test_data_command() {
        let meta = gen_proxy_cluster_meta();
        let manager = gen_testing_manager(Arc::new(always_ok));

        manager.set_meta(meta).unwrap();
        wait_backend_ready(&manager).await;

        let (cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());
        manager.send(cmd_ctx);

        let result = reply_receiver.await;
        let (_, response, _) = result.unwrap().into_inner();
        let resp = response.into_resp_vec();
        let s = match resp {
            Resp::Simple(s) => s,
            other => panic!(format!(
                "unexpected pattern {:?}",
                other.map(|b| pretty_print_bytes(b.as_slice()))
            )),
        };
        assert_eq!(s, OK_REPLY.as_bytes());
    }

    fn gen_migration_cluster_meta(is_migrating: bool) -> ProxyClusterMeta {
        let s = if is_migrating {
            "233 NOFLAGS \
            test_cluster 127.0.0.1:6379 1 0-8000 \
            test_cluster 127.0.0.1:6379 migrating 1 8001-16383 233 127.0.0.1:5299 127.0.0.1:6379 127.0.0.1:6000 127.0.0.1:7000 \
            PEER \
            test_cluster 127.0.0.1:6000 importing 1 8001-16383 233 127.0.0.1:5299 127.0.0.1:6379 127.0.0.1:6000 127.0.0.1:7000"
        } else {
            "233 NOFLAGS \
            test_cluster 127.0.0.1:7000 importing 1 8001-16383 233 127.0.0.1:5299 127.0.0.1:6379 127.0.0.1:6000 127.0.0.1:7000 \
            PEER \
            test_cluster 127.0.0.1:5299 1 0-8000 \
            test_cluster 127.0.0.1:5299 migrating 1 8001-16383 233 127.0.0.1:5299 127.0.0.1:6379 127.0.0.1:6000 127.0.0.1:7000"
        };
        let mut iter = s.split(' ').map(|s| s.to_string()).peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    fn resp_contains(resp: &RespVec, pat: &str) -> bool {
        match resp {
            Resp::Arr(Array::Arr(resps)) => resps.iter().any(|resp| resp_contains(resp, pat)),
            Resp::Bulk(BulkStr::Str(s)) => str::from_utf8(s.as_slice()).unwrap().contains(pat),
            _ => false,
        }
    }

    #[tokio::test]
    async fn test_manager_migration() {
        let src_manager = gen_testing_manager(Arc::new(handle_migration_command));
        let dst_manager = gen_testing_manager(Arc::new(handle_migration_command));

        // dst proxy will be set first by coordinator.
        dst_manager
            .set_meta(gen_migration_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta(true))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let switch_arg = SwitchArg {
            version: UNDERMOON_MIGRATION_VERSION.to_string(),
            meta: MigrationTaskMeta {
                cluster_name: ClusterName::try_from("test_cluster").unwrap(),
                slot_range: SlotRange {
                    range_list: RangeList::from_single_range(Range(8001, 16383)),
                    tag: SlotRangeTag::Migrating(MigrationMeta {
                        epoch: 233,
                        src_proxy_address: "127.0.0.1:5299".to_string(),
                        src_node_address: "127.0.0.1:6379".to_string(),
                        dst_proxy_address: "127.0.0.1:6000".to_string(),
                        dst_node_address: "127.0.0.1:7000".to_string(),
                    }),
                },
            },
        };
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreCheck)
            .unwrap();
        dst_manager
            .handle_switch(switch_arg, MgrSubCmd::PreSwitch)
            .unwrap();

        loop {
            Delay::new(Duration::from_millis(1)).await;
            let info = src_manager.info();
            if !resp_contains(&info, MigrationState::Scanning.to_string().as_str()) {
                continue;
            }
            let info = dst_manager.info();
            if !resp_contains(&info, MigrationState::PreSwitch.to_string().as_str()) {
                continue;
            }
            break;
        }

        {
            // Slots 0-8000 for source proxy should process normally.
            // The slot of key `b` is 3300.
            let (cmd_ctx, reply_receiver) = gen_set_command(b"b".to_vec());
            src_manager.send(cmd_ctx);

            let result = reply_receiver.await;
            let (_, response, _) = result.unwrap().into_inner();
            let resp = response.into_resp_vec();
            let s = match resp {
                Resp::Simple(s) => s,
                other => panic!(format!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                )),
            };
            assert_eq!(s, OK_REPLY.as_bytes());
        }
        {
            // Slots 8001-16383 for source proxy should trigger redirection.
            // The slot of key `a` is 15495.
            let (cmd_ctx, reply_receiver) = gen_set_command(b"a".to_vec());
            src_manager.send(cmd_ctx);

            let result = reply_receiver.await;
            let (_, response, _) = result.unwrap().into_inner();
            let resp = response.into_resp_vec();
            let err_msg = match resp {
                Resp::Error(err_msg) => err_msg,
                other => panic!(format!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                )),
            };
            let s = str::from_utf8(err_msg.as_slice()).unwrap();
            assert!(s.starts_with(ERR_MOVED));
        }
        {
            // Slots 0-8000 for destination proxy should trigger redirection.
            // The slot of key `b` is 3300.
            let (cmd_ctx, reply_receiver) = gen_set_command(b"b".to_vec());
            dst_manager.send(cmd_ctx);

            let result = reply_receiver.await;
            let (_, response, _) = result.unwrap().into_inner();
            let resp = response.into_resp_vec();
            let err_msg = match resp {
                Resp::Error(err_msg) => err_msg,
                other => panic!(format!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                )),
            };
            let s = str::from_utf8(err_msg.as_slice()).unwrap();
            assert!(s.starts_with(ERR_MOVED));
        }
        {
            // Slots 8001-16383 for source proxy should be able to be processed.
            // The slot of key `a` is 15495.
            let (cmd_ctx, reply_receiver) = gen_set_command(b"a".to_vec());
            dst_manager.send(cmd_ctx);

            let result = reply_receiver.await;
            let (_, response, _) = result.unwrap().into_inner();
            let resp = response.into_resp_vec();
            let s = match resp {
                Resp::Simple(s) => s,
                other => panic!(format!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                )),
            };
            assert_eq!(s, OK_REPLY.as_bytes());
        }
    }
}
