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
    use std::sync::atomic::{AtomicI64, AtomicU64};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio;
    use undermoon::common::batch::BatchStrategy;
    use undermoon::common::cluster::{
        ClusterName, MigrationMeta, MigrationTaskMeta, Range, RangeList, SlotRange, SlotRangeTag,
    };
    use undermoon::common::proto::{ClusterMapFlags, ProxyClusterMeta, SET_CLUSTER_API_VERSION};
    use undermoon::common::response::{
        ERR_BACKEND_CONNECTION, ERR_CLUSTER_NOT_FOUND, ERR_MOVED, ERR_TOO_MANY_REDIRECTIONS,
        OK_REPLY,
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
    use undermoon::replication::replicator::{MasterMeta, ReplicaMeta, ReplicatorMeta};

    const TEST_CLUSTER: &str = "test_cluster";
    type TestMetaManager = MetaManager<DummyClientFactory, DummyOkConnFactory>;

    fn gen_config() -> ServerProxyConfig {
        ServerProxyConfig {
            address: "127.0.0.1:5299".to_string(),
            announce_address: "127.0.0.1:5299".to_string(),
            announce_host: "127.0.0.1".to_string(),
            slowlog_len: NonZeroUsize::new(1024).unwrap(),
            slowlog_log_slower_than: AtomicI64::new(0),
            slowlog_sample_rate: AtomicU64::new(1),
            thread_number: NonZeroUsize::new(2).unwrap(),
            // Should only be 1 so that when `wait_backend_ready` is done,
            // the whole backend is ready.
            backend_conn_num: NonZeroUsize::new(1).unwrap(),
            active_redirection: false,
            max_redirections: None,
            default_redirection_address: None,
            backend_batch_strategy: BatchStrategy::Fixed,
            backend_flush_size: NonZeroUsize::new(1024).unwrap(),
            backend_low_flush_interval: Duration::from_nanos(200_000),
            backend_high_flush_interval: Duration::from_nanos(800_000),
            backend_timeout: Duration::from_secs(3),
            password: None,
        }
    }

    fn gen_testing_manager(
        handle_func: Arc<dyn Fn(Vec<String>) -> RespVec + Send + Sync + 'static>,
        config: ServerProxyConfig,
    ) -> TestMetaManager {
        let config = Arc::new(config);
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
        let resp = RespPacket::Data(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"SET".to_vec())),
            Resp::Bulk(BulkStr::Str(key)),
            Resp::Bulk(BulkStr::Str(b"value".to_vec())),
        ])));
        let command = Command::new(Box::new(resp));
        let (s, r) = new_command_pair(&command);
        let cmd_ctx = CmdCtx::new(command, s, 233, true);
        (cmd_ctx, r)
    }

    fn gen_proxy_cluster_meta() -> ProxyClusterMeta {
        let args = format!(
            "{} 1 NOFLAGS test_cluster 127.0.0.1:6379 1 0-16383",
            SET_CLUSTER_API_VERSION
        );
        let mut iter = args.split(' ').map(|s| s.to_string()).peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    fn gen_repl_meta_with_both_master_and_replica() -> ReplicatorMeta {
        let cluster_name = ClusterName::try_from(TEST_CLUSTER).unwrap();

        ReplicatorMeta {
            epoch: 233,
            flags: ClusterMapFlags {
                force: false,
                compress: false,
            },
            masters: vec![MasterMeta {
                cluster_name: cluster_name.clone(),
                master_node_address: "127.0.0.1:6379".to_string(),
                replicas: vec![],
            }],
            replicas: vec![ReplicaMeta {
                cluster_name: cluster_name.clone(),
                replica_node_address: "127.0.0.1:7001".to_string(),
                masters: vec![],
            }],
        }
    }

    fn gen_repl_meta_with_all_masters() -> ReplicatorMeta {
        let cluster_name = ClusterName::try_from(TEST_CLUSTER).unwrap();

        ReplicatorMeta {
            epoch: 233,
            flags: ClusterMapFlags {
                force: false,
                compress: false,
            },
            masters: vec![MasterMeta {
                cluster_name: cluster_name.clone(),
                master_node_address: "127.0.0.1:6379".to_string(),
                replicas: vec![],
            }],
            replicas: vec![],
        }
    }

    fn gen_repl_meta_with_all_replicas() -> ReplicatorMeta {
        let cluster_name = ClusterName::try_from(TEST_CLUSTER).unwrap();

        ReplicatorMeta {
            epoch: 233,
            flags: ClusterMapFlags {
                force: false,
                compress: false,
            },
            masters: vec![],
            replicas: vec![ReplicaMeta {
                cluster_name: cluster_name.clone(),
                replica_node_address: "127.0.0.1:7001".to_string(),
                masters: vec![],
            }],
        }
    }

    async fn assert_ok_reply(reply_receiver: CmdReplyReceiver) {
        let result = reply_receiver.await;
        let (_, response, _) = result.unwrap().into_inner();
        let resp = response.into_resp_vec();
        let s = match resp {
            Resp::Simple(s) => s,
            other => {
                println!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                );
                panic!();
            }
        };
        assert_eq!(s, OK_REPLY.as_bytes());
    }

    #[tokio::test]
    async fn test_cluster_not_found() {
        let manager = gen_testing_manager(Arc::new(always_ok), gen_config());
        let (cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());

        manager.send(cmd_ctx);

        let result = reply_receiver.await;
        assert!(result.is_ok());
        let (_, response, _) = result.unwrap().into_inner();
        let resp = response.into_resp_vec();
        let err = match resp {
            Resp::Error(err_str) => err_str,
            other => {
                println!("unexpected pattern {:?}", other);
                panic!();
            }
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

    pub fn always_ok(_: Vec<String>) -> RespVec {
        Resp::Simple(b"OK".to_vec())
    }

    pub fn handle_migration_command(cmd: Vec<String>) -> RespVec {
        let cmd_name = cmd[0].to_uppercase();
        match cmd_name.as_str() {
            "EXISTS" => Resp::Integer(b"0".to_vec()),
            "DUMP" => Resp::Bulk(BulkStr::Str(b"binary_format_xxx".to_vec())),
            "RESTORE" => Resp::Simple(b"OK".to_vec()),
            "PTTL" => Resp::Integer(b"-1".to_vec()),
            "UMCTL" => Resp::Simple(b"OK".to_vec()),
            "SCAN" => Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(b"1".to_vec())),
                Resp::Arr(Array::Arr(vec![Resp::Bulk(BulkStr::Str(
                    b"some_key".to_vec(),
                ))])),
            ])),
            _ => Resp::Simple(b"OK".to_vec()),
        }
    }

    pub fn handle_command_for_finished_migration(cmd: Vec<String>) -> RespVec {
        let cmd_name = cmd[0].to_uppercase();
        match cmd_name.as_str() {
            "EXISTS" => Resp::Integer(b"1".to_vec()),
            "DUMP" => Resp::Bulk(BulkStr::Str(b"binary_format_xxx".to_vec())),
            "RESTORE" => Resp::Simple(b"OK".to_vec()),
            "PTTL" => Resp::Integer(b"-1".to_vec()),
            "UMCTL" => Resp::Simple(b"OK".to_vec()),
            "SCAN" => Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(b"0".to_vec())),
                Resp::Arr(Array::Arr(vec![Resp::Bulk(BulkStr::Str(b"1".to_vec()))])),
            ])),
            _ => Resp::Simple(b"OK".to_vec()),
        }
    }

    #[tokio::test]
    async fn test_data_command() {
        let meta = gen_proxy_cluster_meta();
        let manager = gen_testing_manager(Arc::new(always_ok), gen_config());

        manager.set_meta(meta).unwrap();
        wait_backend_ready(&manager).await;

        let (cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());
        manager.send(cmd_ctx);

        assert_ok_reply(reply_receiver).await;
    }

    fn gen_migration_cluster_meta(is_source_proxy: bool) -> ProxyClusterMeta {
        gen_migration_cluster_meta_helper(is_source_proxy, 233, "127.0.0.1:5299", "127.0.0.1:6000")
    }

    fn gen_migration_comitted_cluster_meta(is_source_proxy: bool) -> ProxyClusterMeta {
        gen_migration_comitted_cluster_meta_helper(
            is_source_proxy,
            666,
            "127.0.0.1:5299",
            "127.0.0.1:6000",
        )
    }

    fn gen_migration_cluster_meta_helper(
        is_source_proxy: bool,
        epoch: u64,
        src_proxy_address: &str,
        dst_proxy_address: &str,
    ) -> ProxyClusterMeta {
        let s = if is_source_proxy {
            format!("{version} {epoch} NOFLAGS test_cluster \
            127.0.0.1:6379 1 0-8000 \
            127.0.0.1:6379 migrating 1 8001-16383 {epoch} {src_proxy_address} 127.0.0.1:6379 {dst_proxy_address} 127.0.0.1:7000 \
            PEER \
            {dst_proxy_address} importing 1 8001-16383 {epoch} {src_proxy_address} 127.0.0.1:6379 {dst_proxy_address} 127.0.0.1:7000",
                    version=SET_CLUSTER_API_VERSION, epoch=epoch, src_proxy_address=src_proxy_address, dst_proxy_address=dst_proxy_address)
        } else {
            format!("{version} {epoch} NOFLAGS test_cluster \
            127.0.0.1:7000 importing 1 8001-16383 {epoch} {src_proxy_address} 127.0.0.1:6379 {dst_proxy_address} 127.0.0.1:7000 \
            PEER \
            {src_proxy_address} 1 0-8000 \
            {src_proxy_address} migrating 1 8001-16383 {epoch} {}src_proxy_address 127.0.0.1:6379 {dst_proxy_address} 127.0.0.1:7000",
                    version=SET_CLUSTER_API_VERSION, epoch=epoch, src_proxy_address=src_proxy_address, dst_proxy_address=dst_proxy_address)
        };
        let mut iter = s.split(' ').map(|s| s.to_string()).peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    fn gen_migration_comitted_cluster_meta_helper(
        is_source_proxy: bool,
        epoch: u64,
        src_proxy_address: &str,
        dst_proxy_address: &str,
    ) -> ProxyClusterMeta {
        let s = if is_source_proxy {
            format!(
                "{version} {epoch} NOFLAGS test_cluster \
            127.0.0.1:6379 1 0-8000 \
            PEER \
            {dst_proxy_address} 1 8001-16383",
                version = SET_CLUSTER_API_VERSION,
                epoch = epoch,
                dst_proxy_address = dst_proxy_address
            )
        } else {
            format!(
                "{version} {epoch} NOFLAGS test_cluster \
            127.0.0.1:7000 1 8001-16383 \
            PEER \
            {src_proxy_address} 1 0-8000",
                version = SET_CLUSTER_API_VERSION,
                epoch = epoch,
                src_proxy_address = src_proxy_address
            )
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

    async fn check_src_request(src_manager: &TestMetaManager) {
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
                other => {
                    println!(
                        "unexpected pattern {:?}",
                        other.map(|b| pretty_print_bytes(b.as_slice()))
                    );
                    panic!();
                }
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
                other => {
                    println!(
                        "unexpected pattern {:?}",
                        other.map(|b| pretty_print_bytes(b.as_slice()))
                    );
                    panic!();
                }
            };
            let s = str::from_utf8(err_msg.as_slice()).unwrap();
            assert!(s.starts_with(ERR_MOVED));
        }
    }

    async fn check_dst_request(dst_manager: &TestMetaManager) {
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
                other => {
                    println!(
                        "unexpected pattern {:?}",
                        other.map(|b| pretty_print_bytes(b.as_slice()))
                    );
                    panic!();
                }
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
                other => {
                    println!(
                        "unexpected pattern {:?}",
                        other.map(|b| pretty_print_bytes(b.as_slice()))
                    );
                    panic!();
                }
            };
            assert_eq!(s, OK_REPLY.as_bytes());
        }
    }

    fn gen_switch_arg() -> SwitchArg {
        gen_switch_arg_helper(233, "127.0.0.1:5299", "127.0.0.1:6000")
    }

    fn gen_switch_arg_helper(
        epoch: u64,
        src_proxy_address: &str,
        dst_proxy_address: &str,
    ) -> SwitchArg {
        SwitchArg {
            version: UNDERMOON_MIGRATION_VERSION.to_string(),
            meta: MigrationTaskMeta {
                cluster_name: ClusterName::try_from("test_cluster").unwrap(),
                slot_range: SlotRange {
                    range_list: RangeList::from_single_range(Range(8001, 16383)),
                    tag: SlotRangeTag::Migrating(MigrationMeta {
                        epoch,
                        src_proxy_address: src_proxy_address.to_string(),
                        src_node_address: "127.0.0.1:6379".to_string(),
                        dst_proxy_address: dst_proxy_address.to_string(),
                        dst_node_address: "127.0.0.1:7000".to_string(),
                    }),
                },
            },
        }
    }

    #[tokio::test]
    async fn test_manager_migration() {
        let src_manager = gen_testing_manager(Arc::new(handle_migration_command), gen_config());
        let dst_manager = gen_testing_manager(Arc::new(handle_migration_command), gen_config());

        // dst proxy will be set first by coordinator.
        dst_manager
            .set_meta(gen_migration_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta(true))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let switch_arg = gen_switch_arg();
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

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;
        assert!(src_manager.get_finished_migration_tasks().is_empty());
        assert!(dst_manager.get_finished_migration_tasks().is_empty());
    }

    #[tokio::test]
    async fn test_manager_migration_with_scanning_done() {
        let src_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );
        let dst_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );

        // dst proxy will be set first by coordinator.
        dst_manager
            .set_meta(gen_migration_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta(true))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let switch_arg = gen_switch_arg();
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreCheck)
            .unwrap();
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreSwitch)
            .unwrap();

        loop {
            Delay::new(Duration::from_millis(1)).await;
            let info = src_manager.info();
            if !resp_contains(&info, MigrationState::SwitchCommitted.to_string().as_str()) {
                continue;
            }
            let info = dst_manager.info();
            if !resp_contains(&info, MigrationState::PreSwitch.to_string().as_str()) {
                continue;
            }
            break;
        }

        dst_manager
            .handle_switch(switch_arg, MgrSubCmd::FinalSwitch)
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;

        assert_eq!(src_manager.get_finished_migration_tasks().len(), 1);
        assert_eq!(dst_manager.get_finished_migration_tasks().len(), 1);

        // dst will be set first.
        dst_manager
            .set_meta(gen_migration_comitted_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_comitted_cluster_meta(true))
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;
        assert!(src_manager.get_finished_migration_tasks().is_empty());
        assert!(dst_manager.get_finished_migration_tasks().is_empty());
    }

    #[tokio::test]
    async fn test_manager_migration_with_src_failover() {
        let src_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );
        let dst_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );

        // dst proxy will be set first by coordinator.
        dst_manager
            .set_meta(gen_migration_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta(true))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let new_src_proxy_address = "127.0.0.1:5199";
        let dst_proxy_address = "127.0.0.1:6000";

        dst_manager
            .set_meta(gen_migration_cluster_meta_helper(
                false,
                300,
                new_src_proxy_address,
                dst_proxy_address,
            ))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta_helper(
                true,
                300,
                new_src_proxy_address,
                dst_proxy_address,
            ))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let switch_arg = gen_switch_arg_helper(300, new_src_proxy_address, dst_proxy_address);
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreCheck)
            .unwrap();
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreSwitch)
            .unwrap();

        loop {
            Delay::new(Duration::from_millis(1)).await;
            let info = src_manager.info();
            if !resp_contains(&info, MigrationState::SwitchCommitted.to_string().as_str()) {
                continue;
            }
            let info = dst_manager.info();
            if !resp_contains(&info, MigrationState::PreSwitch.to_string().as_str()) {
                continue;
            }
            break;
        }

        dst_manager
            .handle_switch(switch_arg, MgrSubCmd::FinalSwitch)
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;

        assert_eq!(src_manager.get_finished_migration_tasks().len(), 1);
        assert_eq!(dst_manager.get_finished_migration_tasks().len(), 1);

        // dst will be set first.
        dst_manager
            .set_meta(gen_migration_comitted_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_comitted_cluster_meta(true))
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;
        assert!(src_manager.get_finished_migration_tasks().is_empty());
        assert!(dst_manager.get_finished_migration_tasks().is_empty());
    }

    #[tokio::test]
    async fn test_manager_migration_with_dst_failover() {
        let src_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );
        let dst_manager = gen_testing_manager(
            Arc::new(handle_command_for_finished_migration),
            gen_config(),
        );

        // dst proxy will be set first by coordinator.
        dst_manager
            .set_meta(gen_migration_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta(true))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let src_proxy_address = "127.0.0.1:5299";
        let new_dst_proxy_address = "127.0.0.1:6099";

        dst_manager
            .set_meta(gen_migration_cluster_meta_helper(
                false,
                300,
                src_proxy_address,
                new_dst_proxy_address,
            ))
            .unwrap();
        src_manager
            .set_meta(gen_migration_cluster_meta_helper(
                true,
                300,
                src_proxy_address,
                new_dst_proxy_address,
            ))
            .unwrap();
        wait_backend_ready(&src_manager).await;
        wait_backend_ready(&dst_manager).await;

        let switch_arg = gen_switch_arg_helper(300, src_proxy_address, new_dst_proxy_address);
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreCheck)
            .unwrap();
        dst_manager
            .handle_switch(switch_arg.clone(), MgrSubCmd::PreSwitch)
            .unwrap();

        loop {
            Delay::new(Duration::from_millis(1)).await;
            let info = src_manager.info();
            if !resp_contains(&info, MigrationState::SwitchCommitted.to_string().as_str()) {
                continue;
            }
            let info = dst_manager.info();
            if !resp_contains(&info, MigrationState::PreSwitch.to_string().as_str()) {
                continue;
            }
            break;
        }

        dst_manager
            .handle_switch(switch_arg, MgrSubCmd::FinalSwitch)
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;

        assert_eq!(src_manager.get_finished_migration_tasks().len(), 1);
        assert_eq!(dst_manager.get_finished_migration_tasks().len(), 1);

        // dst will be set first.
        dst_manager
            .set_meta(gen_migration_comitted_cluster_meta(false))
            .unwrap();
        src_manager
            .set_meta(gen_migration_comitted_cluster_meta(true))
            .unwrap();

        check_src_request(&src_manager).await;
        check_dst_request(&dst_manager).await;
        assert!(src_manager.get_finished_migration_tasks().is_empty());
        assert!(dst_manager.get_finished_migration_tasks().is_empty());
    }

    pub fn handle_active_redirection(cmd: Vec<String>) -> RespVec {
        let cmd_name = cmd[0].to_uppercase();
        match cmd_name.as_str() {
            "SET" => Resp::Simple(b"OK".to_vec()),
            "UMFORWARD" => Resp::Simple(b"OK".to_vec()),
            _ => Resp::Error(b"unexpected command".to_vec()),
        }
    }

    fn gen_active_redirection_proxy1_cluster_meta() -> ProxyClusterMeta {
        let args = format!(
            "{} 1 NOFLAGS test_cluster 127.0.0.1:7001 1 0-8000 peer 127.0.0.1:6002 1 8001-16383",
            SET_CLUSTER_API_VERSION
        );
        let mut iter = args.split(' ').map(|s| s.to_string()).peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    fn gen_active_redirection_proxy2_cluster_meta() -> ProxyClusterMeta {
        let args = format!(
            "{} 1 NOFLAGS test_cluster 127.0.0.1:7002 1 8001-16383 peer 127.0.0.1:6001 1 0-8000",
            SET_CLUSTER_API_VERSION
        );
        let mut iter = args.split(' ').map(|s| s.to_string()).peekable();
        let (meta, extended_args) = ProxyClusterMeta::parse(&mut iter).unwrap();
        assert!(extended_args.is_ok());
        meta
    }

    async fn wait_peer_backend_ready(manager: &TestMetaManager) {
        loop {
            let (mut cmd_ctx, reply_receiver) = gen_set_command(b"key".to_vec());
            assert!(cmd_ctx.wrap_cmd(vec![b"UMFORWARD".to_vec(), b"1".to_vec(),]));
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

    #[tokio::test]
    async fn test_active_redirection() {
        let mut config1 = gen_config();
        config1.active_redirection = true;
        let mut config2 = gen_config();
        config2.active_redirection = true;
        let manager1 = gen_testing_manager(Arc::new(handle_active_redirection), config1);
        let manager2 = gen_testing_manager(Arc::new(handle_active_redirection), config2);

        manager1
            .set_meta(gen_active_redirection_proxy1_cluster_meta())
            .unwrap();
        manager2
            .set_meta(gen_active_redirection_proxy2_cluster_meta())
            .unwrap();
        wait_backend_ready(&manager1).await;
        wait_peer_backend_ready(&manager1).await;
        wait_backend_ready(&manager2).await;
        wait_peer_backend_ready(&manager2).await;

        // key `a` belongs to manager2
        let (cmd_ctx, reply_receiver) = gen_set_command(b"a".to_vec());
        manager1.send(cmd_ctx);
        assert_ok_reply(reply_receiver).await;

        // at the same time send UMFORWARD to manager2
        let (mut cmd_ctx, reply_receiver) = gen_set_command(b"a".to_vec());
        assert!(cmd_ctx.wrap_cmd(vec![b"UMFORWARD".to_vec(), b"1".to_vec(),]));
        manager2.send(cmd_ctx);
        assert_ok_reply(reply_receiver).await;

        // too many redirections
        let (mut cmd_ctx, reply_receiver) = gen_set_command(b"a".to_vec());
        assert!(cmd_ctx.wrap_cmd(vec![b"UMFORWARD".to_vec(), b"0".to_vec(),]));
        cmd_ctx.set_redirection_times(0);

        manager1.send(cmd_ctx);
        let result = reply_receiver.await;
        let (_, response, _) = result.unwrap().into_inner();
        let resp = response.into_resp_vec();
        let s = match resp {
            Resp::Error(s) => s,
            other => {
                println!(
                    "unexpected pattern {:?}",
                    other.map(|b| pretty_print_bytes(b.as_slice()))
                );
                panic!();
            }
        };
        assert_eq!(s, ERR_TOO_MANY_REDIRECTIONS.as_bytes());
    }

    #[tokio::test]
    async fn test_readiness_stable_slots() {
        let manager = gen_testing_manager(Arc::new(always_ok), gen_config());
        assert!(!manager.is_ready());

        let meta = gen_repl_meta_with_both_master_and_replica();
        manager.update_replicators(meta).unwrap();
        assert!(!manager.is_ready());

        let meta = gen_proxy_cluster_meta();
        manager.set_meta(meta).unwrap();
        assert!(manager.is_ready());
    }

    #[tokio::test]
    async fn test_readiness_all_masters() {
        let manager = gen_testing_manager(Arc::new(always_ok), gen_config());
        assert!(!manager.is_ready());

        let meta = gen_repl_meta_with_all_masters();
        manager.update_replicators(meta).unwrap();
        assert!(!manager.is_ready());
    }

    #[tokio::test]
    async fn test_readiness_all_replicas() {
        let manager = gen_testing_manager(Arc::new(always_ok), gen_config());
        assert!(!manager.is_ready());

        let meta = gen_repl_meta_with_all_replicas();
        manager.update_replicators(meta).unwrap();
        assert!(manager.is_ready());
    }
}
