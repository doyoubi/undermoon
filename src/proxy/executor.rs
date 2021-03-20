use super::backend::{CmdTask, CmdTaskFactory, ConnFactory};
use super::cluster::{ClusterMetaError, ClusterTag};
use super::command::{CmdReplyReceiver, CmdType, DataCmdType, TaskResult};
use super::compress::{CmdCompressor, CompressionError, CompressionStrategyMetaMapConfig};
use super::manager::{MetaManager, SharedMetaMap};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxFactory, CmdCtxHandler, CmdReplyFuture};
use super::slowlog::{slowlogs_to_resp, SlowRequestLogger};
use super::table::CommandTable;
use crate::common::cluster::ClusterName;
use crate::common::config::ClusterConfig;
use crate::common::proto::ProxyClusterMeta;
use crate::common::response;
use crate::common::track::TrackedFutureRegistry;
use crate::common::utils::{
    change_bulk_array_element, generate_slot, pretty_print_bytes, same_slot,
    str_ascii_case_insensitive_eq,
};
use crate::common::version::UNDERMOON_VERSION;
use crate::migration::manager::SwitchError;
use crate::migration::task::parse_switch_command;
use crate::migration::task::MgrSubCmd;
use crate::protocol::{
    Array, BulkStr, RFunctor, RedisClientFactory, Resp, RespPacket, RespVec, VFunctor,
};
use crate::replication::replicator::ReplicatorMeta;
use atoi::atoi;
use btoi::btou;
use futures::channel::mpsc;
use futures::future;
use futures_timer::Delay;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str;
use std::sync::{self, Arc};
use std::time::Duration;

type NonBlockingCommandsWithKey = Vec<(Vec<u8>, RespVec)>;

pub struct SharedForwardHandler<F: RedisClientFactory, C: ConnFactory<Pkt=RespPacket>> {
    handler: sync::Arc<ForwardHandler<F, C>>,
}

impl<F, C> Clone for SharedForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
        }
    }
}

impl<F, C> SharedForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<ServerProxyConfig>,
        cluster_config: ClusterConfig,
        client_factory: Arc<F>,
        slow_request_logger: Arc<SlowRequestLogger>,
        meta_map: SharedMetaMap<C>,
        conn_factory: Arc<C>,
        future_registry: Arc<TrackedFutureRegistry>,
        stopped: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            handler: sync::Arc::new(ForwardHandler::new(
                config,
                cluster_config,
                client_factory,
                slow_request_logger,
                meta_map,
                conn_factory,
                future_registry,
                stopped,
            )),
        }
    }
}

impl<F, C> CmdCtxHandler for SharedForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    fn handle_cmd_ctx(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
        session_cluster_name: &parking_lot::RwLock<ClusterName>,
    ) -> CmdReplyFuture {
        self.handler
            .handle_cmd_ctx(cmd_ctx, reply_receiver, session_cluster_name)
    }
}

pub struct ForwardHandler<F: RedisClientFactory, C: ConnFactory<Pkt=RespPacket>> {
    config: Arc<ServerProxyConfig>,
    manager: MetaManager<F, C>,
    slow_request_logger: Arc<SlowRequestLogger>,
    compressor: CmdCompressor<CompressionStrategyMetaMapConfig<C>>,
    future_registry: Arc<TrackedFutureRegistry>,
    stopped: mpsc::UnboundedSender<()>,
    command_table: Arc<CommandTable>,
}

impl<F, C> ForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<ServerProxyConfig>,
        cluster_config: ClusterConfig,
        client_factory: Arc<F>,
        slow_request_logger: Arc<SlowRequestLogger>,
        meta_map: SharedMetaMap<C>,
        conn_factory: Arc<C>,
        future_registry: Arc<TrackedFutureRegistry>,
        stopped: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config: config.clone(),
            manager: MetaManager::new(
                config,
                cluster_config,
                client_factory,
                conn_factory,
                meta_map.clone(),
                future_registry.clone(),
            ),
            slow_request_logger,
            compressor: CmdCompressor::new(CompressionStrategyMetaMapConfig::new(meta_map)),
            future_registry,
            stopped,
            command_table: Arc::new(CommandTable::default()),
        }
    }
}

impl<F, C> ForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    fn handle_info(&self, cmd_ctx: CmdCtx) {
        let flush_size = self.manager.get_batch_stats().get_flush_size();
        let flush_interval = self.manager.get_batch_stats().get_flush_interval();
        let content = format!(
            "version:{}\r\n\r\n# Stats\r\nflush_size:{}\r\nflush_interval:{}\r\n",
            UNDERMOON_VERSION, flush_size, flush_interval,
        );
        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(content.into_bytes()))));
    }

    fn handle_auth(
        &self,
        mut cmd_ctx: CmdCtx,
        session_cluster_name: &parking_lot::RwLock<ClusterName>,
    ) {
        let key = cmd_ctx.get_key();
        let cluster = match key {
            None => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Missing cluster name").into_bytes(),
                )));
            }
            Some(cluster_name) => match str::from_utf8(&cluster_name) {
                Ok(cluster) => cluster.to_string(),
                Err(_) => {
                    return cmd_ctx.set_resp_result(Ok(Resp::Error(
                        String::from("Invalid cluster name").into_bytes(),
                    )));
                }
            },
        };
        let cluster_name = match ClusterName::try_from(cluster.as_str()) {
            Ok(cluster_name) => cluster_name,
            _err => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Cluster name is too long").into_bytes(),
                )));
            }
        };

        *session_cluster_name.write() = cluster_name.clone();
        cmd_ctx.set_cluster_name(cluster_name);
        cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())));
    }

    fn handle_cluster(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        if str_ascii_case_insensitive_eq(&sub_cmd, "nodes") {
            let cluster_nodes = self
                .manager
                .gen_cluster_nodes(cmd_ctx.get_cluster_name().clone());
            cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(cluster_nodes.into_bytes()))))
        } else if str_ascii_case_insensitive_eq(&sub_cmd, "slots") {
            let cluster_slots = self
                .manager
                .gen_cluster_slots(cmd_ctx.get_cluster_name().clone());
            match cluster_slots {
                Ok(resp) => cmd_ctx.set_resp_result(Ok(resp)),
                Err(s) => cmd_ctx.set_resp_result(Ok(Resp::Error(s.into_bytes()))),
            }
        } else if str_ascii_case_insensitive_eq(&sub_cmd, "keyslot") {
            match cmd_ctx.get_cmd().get_command_element(2) {
                Some(key) => {
                    let slot = generate_slot(key);
                    cmd_ctx.set_resp_result(Ok(Resp::Integer(slot.to_string().into_bytes())));
                }
                None => {
                    cmd_ctx
                        .set_resp_result(Ok(Resp::Error(String::from("Missing key").into_bytes())));
                }
            }
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Unsupported sub command").into_bytes(),
            )));
        }
    }

    fn get_sub_command(cmd_ctx: CmdCtx, index: usize) -> Option<(CmdCtx, String)> {
        let sub_cmd = match cmd_ctx.get_cmd().get_command_element(index) {
            None => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Missing sub command").into_bytes(),
                )));
                return None;
            }
            Some(k) => match str::from_utf8(k) {
                Ok(sub_cmd) => sub_cmd.to_string(),
                Err(_) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        String::from("Invalid sub command").into_bytes(),
                    )));
                    return None;
                }
            },
        };
        Some((cmd_ctx, sub_cmd))
    }

    fn handle_umctl(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        let sub_cmd = sub_cmd.to_uppercase();

        if sub_cmd.eq("LISTCLUSTER") {
            let clusters = self.manager.get_clusters();
            let resps = clusters
                .into_iter()
                .map(|cluster_name| Resp::Bulk(BulkStr::Str(cluster_name.to_string().into_bytes())))
                .collect();
            cmd_ctx.set_resp_result(Ok(Resp::Arr(Array::Arr(resps))));
        } else if sub_cmd.eq("SETCLUSTER") {
            self.handle_umctl_set_cluster(cmd_ctx);
        } else if sub_cmd.eq("SETREPL") {
            self.handle_umctl_setrepl(cmd_ctx);
        } else if sub_cmd.eq("INFO") {
            let resp = self.manager.info();
            cmd_ctx.set_resp_result(Ok(resp));
        } else if sub_cmd.eq("INFOREPL") {
            self.handle_umctl_info_repl(cmd_ctx);
        } else if sub_cmd.eq("INFOMGR") {
            self.handle_umctl_info_migration(cmd_ctx);
        } else if sub_cmd.eq(MgrSubCmd::PreCheck.as_str()) {
            self.handle_umctl_mgr_cmd(cmd_ctx, MgrSubCmd::PreCheck);
        } else if sub_cmd.eq(MgrSubCmd::PreSwitch.as_str()) {
            self.handle_umctl_mgr_cmd(cmd_ctx, MgrSubCmd::PreSwitch);
        } else if sub_cmd.eq(MgrSubCmd::FinalSwitch.as_str()) {
            self.handle_umctl_mgr_cmd(cmd_ctx, MgrSubCmd::FinalSwitch);
        } else if sub_cmd.eq("SLOWLOG") {
            self.handle_umctl_slowlog(cmd_ctx);
        } else if sub_cmd.eq("DEBUG") {
            self.handle_umctl_debug(cmd_ctx);
        } else if sub_cmd.eq("GETEPOCH") {
            self.handle_umctl_get_epoch(cmd_ctx);
        } else if sub_cmd.eq("READY") {
            self.handle_umctl_ready(cmd_ctx);
        } else if sub_cmd.eq("SHUTDOWN") {
            self.handle_umctl_shutdown(cmd_ctx);
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid sub command").into_bytes(),
            )));
        }
    }

    fn handle_umctl_set_cluster(&self, cmd_ctx: CmdCtx) {
        let (cluster_meta, extended_res) =
            match ProxyClusterMeta::from_resp(&cmd_ctx.get_cmd().get_resp_slice()) {
                Ok(r) => r,
                Err(_) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        String::from("Invalid arguments").into_bytes(),
                    )));
                    return;
                }
            };

        match self.manager.set_meta(cluster_meta) {
            Ok(()) => match extended_res {
                Ok(()) => {
                    debug!("Successfully update local meta data");
                    cmd_ctx.set_resp_result(Ok(Resp::Simple("OK".to_string().into_bytes())));
                }
                Err(_) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Simple(
                        "WARNING: ignored invalid config".to_string().into_bytes(),
                    )));
                }
            },
            Err(err) => match err {
                ClusterMetaError::OldEpoch => cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::OLD_EPOCH_REPLY.to_string().into_bytes(),
                ))),
                ClusterMetaError::TryAgain => cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::TRY_AGAIN_REPLY.to_string().into_bytes(),
                ))),
                ClusterMetaError::NotMyMeta => cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_MY_META.to_string().into_bytes(),
                ))),
            },
        }
    }

    fn handle_umctl_setrepl(&self, cmd_ctx: CmdCtx) {
        let meta = match ReplicatorMeta::from_resp(&cmd_ctx.get_cmd().get_resp_slice()) {
            Ok(m) => m,
            Err(_) => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Invalid arguments").into_bytes(),
                )));
                return;
            }
        };

        match self.manager.update_replicators(meta) {
            Ok(()) => {
                debug!("Successfully update replicator meta data");
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            Err(e) => {
                //                debug!("Failed to update replicator meta data {:?}", e);
                match e {
                    ClusterMetaError::OldEpoch => cmd_ctx.set_resp_result(Ok(Resp::Error(
                        response::OLD_EPOCH_REPLY.to_string().into_bytes(),
                    ))),
                    ClusterMetaError::TryAgain => cmd_ctx.set_resp_result(Ok(Resp::Error(
                        response::TRY_AGAIN_REPLY.to_string().into_bytes(),
                    ))),
                    ClusterMetaError::NotMyMeta => cmd_ctx.set_resp_result(Ok(Resp::Error(
                        response::ERR_NOT_MY_META.to_string().into_bytes(),
                    ))),
                }
            }
        }
    }

    fn handle_umctl_info_repl(&self, cmd_ctx: CmdCtx) {
        let report = self.manager.get_replication_info();
        cmd_ctx.set_resp_result(Ok(report));
    }

    fn handle_umctl_mgr_cmd(&self, cmd_ctx: CmdCtx, sub_cmd: MgrSubCmd) {
        let switch_arg = match parse_switch_command(&cmd_ctx.get_cmd().get_resp_slice()) {
            Some(switch_meta) => switch_meta,
            None => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    "failed to parse migration switch arguments"
                        .to_string()
                        .into_bytes(),
                )));
                return;
            }
        };
        match self.manager.handle_switch(switch_arg, sub_cmd) {
            Ok(()) => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple("OK".to_string().into_bytes())));
            }
            Err(err) => {
                let err_str = match err {
                    SwitchError::InvalidArg => "Invalid Arg".to_string(),
                    SwitchError::TaskNotFound => response::TASK_NOT_FOUND.to_string(),
                    SwitchError::PeerMigrating => "Peer Not Migrating".to_string(),
                    SwitchError::NotReady => response::NOT_READY_FOR_SWITCHING_REPLY.to_string(),
                    SwitchError::MgrErr(err) => format!("switch failed: {:?}", err),
                };
                cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
            }
        }
    }

    fn handle_umctl_info_migration(&self, cmd_ctx: CmdCtx) {
        let finished_tasks = self.manager.get_finished_migration_tasks();
        let packet: Vec<RespVec> = finished_tasks
            .into_iter()
            .map(|task| task.into_strings().join(" "))
            .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
            .collect();
        cmd_ctx.set_resp_result(Ok(Resp::Arr(Array::Arr(packet))))
    }

    fn handle_umctl_slowlog(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 2) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        let sub_cmd = sub_cmd.to_uppercase();

        if sub_cmd.eq("GET") {
            let limit = cmd_ctx
                .get_cmd()
                .get_command_element(3)
                .and_then(|element| atoi::<usize>(&element));
            let logs = self.slow_request_logger.get(limit);
            let reply = slowlogs_to_resp(logs);
            cmd_ctx.set_resp_result(Ok(reply));
        } else if sub_cmd.eq("RESET") {
            self.slow_request_logger.reset();
            cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())));
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                "invalid slowlog sub-command".to_string().into_bytes(),
            )))
        }
    }

    fn handle_umctl_debug(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 2) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        let sub_cmd = sub_cmd.to_uppercase();

        if sub_cmd.eq("FUTURE") {
            let mut fut_desc_arr = self.future_registry.get_all_futures();
            fut_desc_arr.sort_unstable_by_key(|desc| desc.get_start_time());

            let elements = fut_desc_arr
                .into_iter()
                .map(|desc| Resp::Bulk(BulkStr::Str(format!("{}", desc).into_bytes())))
                .collect();
            let reply = Resp::Arr(Array::Arr(elements));
            cmd_ctx.set_resp_result(Ok(reply));
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                "invalid debug sub-command".to_string().into_bytes(),
            )))
        }
    }

    fn handle_umctl_get_epoch(&self, cmd_ctx: CmdCtx) {
        let epoch = self.manager.get_epoch();
        cmd_ctx.set_resp_result(Ok(Resp::Integer(epoch.to_string().into_bytes())))
    }

    fn handle_umctl_ready(&self, cmd_ctx: CmdCtx) {
        let is_ready = self.manager.is_ready(cmd_ctx.get_cluster());
        let n = if is_ready { 1 } else { 0 };
        cmd_ctx.set_resp_result(Ok(Resp::Integer(n.to_string().into_bytes())))
    }

    fn handle_umctl_shutdown(&self, cmd_ctx: CmdCtx) {
        if let Err(_err) = self.stopped.unbounded_send(()) {
            cmd_ctx.set_resp_result(Ok(Resp::Error(b"failed to send shutdown".to_vec())));
            return;
        }
        cmd_ctx.set_resp_result(Ok(Resp::Simple(
            response::OK_REPLY.to_string().into_bytes(),
        )))
    }

    fn handle_config(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd.to_uppercase()),
            None => return,
        };

        if sub_cmd.eq("GET") {
            let (cmd_ctx, field) = match Self::get_sub_command(cmd_ctx, 2) {
                Some((cmd_ctx, field)) => (cmd_ctx, field),
                None => return,
            };
            let value = match self.config.get_field(&field) {
                Ok(value) => value,
                Err(_) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        format!("config field {} not found", field).into_bytes(),
                    )));
                    return;
                }
            };
            cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(value.into_bytes()))));
        } else if sub_cmd.eq("SET") {
            let (cmd_ctx, field) = match Self::get_sub_command(cmd_ctx, 2) {
                Some((cmd_ctx, field)) => (cmd_ctx, field),
                None => return,
            };
            let (cmd_ctx, value) = match Self::get_sub_command(cmd_ctx, 3) {
                Some((cmd_ctx, value)) => (cmd_ctx, value),
                None => return,
            };
            match self.config.set_value(&field, &value) {
                Ok(()) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
                }
                Err(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(format!("{:?}", err).into_bytes())))
                }
            }
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                "invalid config sub-command".to_string().into_bytes(),
            )))
        }
    }

    fn handle_command_cmd(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
    ) -> CmdReplyFuture {
        let table = self.command_table.clone();
        CmdReplyFuture::Right(Box::pin(async move {
            let res = self.manager.send_to_any_local_node(&cmd_ctx).await;

            let cmds = match res {
                Resp::Arr(Array::Arr(cmds)) => cmds,
                others => {
                    let resp = Resp::Error(
                        format!(
                            "invalid command reply: {:?}",
                            others.as_ref().map(|b| pretty_print_bytes(b.as_slice()))
                        )
                            .into_bytes(),
                    );
                    cmd_ctx.set_resp_result(Ok(resp));
                    return reply_receiver.await;
                }
            };

            let mut filtered = Vec::with_capacity(cmds.capacity());
            for cmd in cmds.into_iter() {
                let cmd_name = match &cmd {
                    Resp::Arr(Array::Arr(elements)) => elements.get(0),
                    _ => None,
                };
                match cmd_name {
                    Some(Resp::Bulk(BulkStr::Str(s))) => {
                        if table.is_supported(s.as_slice()) {
                            filtered.push(cmd);
                        }
                    }
                    _ => {
                        let resp = Resp::Error(b"cannot get command name".to_vec());
                        cmd_ctx.set_resp_result(Ok(resp));
                        return reply_receiver.await;
                    }
                };
            }

            cmd_ctx.set_resp_result(Ok(Resp::Arr(Array::Arr(filtered))));
            reply_receiver.await
        }))
    }

    fn handle_data_cmd(&self, cmd_ctx: CmdCtx, reply_receiver: CmdReplyReceiver) -> CmdReplyFuture {
        match cmd_ctx.get_data_cmd_type() {
            DataCmdType::MGET => {
                CmdReplyFuture::Right(Box::pin(self.handle_mget(cmd_ctx, reply_receiver)))
            }
            DataCmdType::MSET => {
                CmdReplyFuture::Right(Box::pin(self.handle_mset(cmd_ctx, reply_receiver)))
            }
            DataCmdType::MSETNX => {
                CmdReplyFuture::Right(Box::pin(self.handle_msetnx(cmd_ctx, reply_receiver)))
            }
            DataCmdType::DEL if cmd_ctx.get_cmd().get_command_element(2).is_some() => {
                CmdReplyFuture::Right(Box::pin(self.handle_multi_int_cmd(
                    cmd_ctx,
                    reply_receiver,
                    "DEL",
                )))
            }
            DataCmdType::EXISTS if cmd_ctx.get_cmd().get_command_element(2).is_some() => {
                CmdReplyFuture::Right(Box::pin(self.handle_multi_int_cmd(
                    cmd_ctx,
                    reply_receiver,
                    "EXISTS",
                )))
            }
            DataCmdType::BLPOP
            | DataCmdType::BRPOP
            | DataCmdType::BRPOPLPUSH
            | DataCmdType::BZPOPMIN
            | DataCmdType::BZPOPMAX => CmdReplyFuture::Right(Box::pin(
                self.handle_blocking_commands(cmd_ctx, reply_receiver),
            )),
            _ => {
                self.handle_single_key_data_cmd(cmd_ctx);
                CmdReplyFuture::Left(reply_receiver)
            }
        }
    }

    async fn handle_mget(&self, cmd_ctx: CmdCtx, reply_receiver: CmdReplyReceiver) -> TaskResult {
        let arg_len = cmd_ctx.get_cmd().get_command_len().unwrap_or(0);

        if !self.config.active_redirection {
            let in_same_slot =
                same_slot((1..arg_len).filter_map(|i| cmd_ctx.get_cmd().get_command_element(i)));
            if !in_same_slot {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_THE_SAME_SLOT.to_string().into_bytes(),
                )));
                return reply_receiver.await;
            }
        }

        let factory = CmdCtxFactory::default();
        let mut futs = vec![];
        for i in 1.. {
            let key = match cmd_ctx.get_cmd().get_command_element(i) {
                Some(key) => key,
                None => break,
            };
            let resp = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(b"GET".to_vec())),
                Resp::Bulk(BulkStr::Str(key.to_vec())),
            ]));
            let (sub_cmd_ctx, fut) = factory.create_with_ctx(cmd_ctx.get_context(), resp);
            futs.push(fut);
            self.handle_single_key_data_cmd(sub_cmd_ctx);
        }

        if futs.is_empty() {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                b"ERR wrong number of arguments for 'mget' command".to_vec(),
            )));
            return reply_receiver.await;
        }

        let mut values = vec![];
        let res = future::join_all(futs).await;
        for sub_result in res.into_iter() {
            let reply = match sub_result {
                Ok(reply) => reply,
                Err(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(format!("ERR: {}", err).into_bytes())));
                    return Err(err);
                }
            };
            if let Resp::Error(err) = &reply {
                cmd_ctx.set_resp_result(Ok(Resp::Error(err.clone())));
                return reply_receiver.await;
            }
            values.push(reply);
        }

        let resp = Resp::Arr(Array::Arr(values));
        cmd_ctx.set_resp_result(Ok(resp));
        reply_receiver.await
    }

    async fn handle_mset(&self, cmd_ctx: CmdCtx, reply_receiver: CmdReplyReceiver) -> TaskResult {
        let arg_len = cmd_ctx.get_cmd().get_command_len().unwrap_or(0);

        if !self.config.active_redirection {
            let in_same_slot = same_slot(
                (0..(arg_len / 2)).filter_map(|i| cmd_ctx.get_cmd().get_command_element(2 * i + 1)),
            );
            if !in_same_slot {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_THE_SAME_SLOT.to_string().into_bytes(),
                )));
                return reply_receiver.await;
            }
        }

        let factory = CmdCtxFactory::default();
        let mut futs = vec![];
        for i in 0.. {
            let key = match cmd_ctx.get_cmd().get_command_element(2 * i + 1) {
                Some(key) => key,
                None => break,
            };
            let value = match cmd_ctx.get_cmd().get_command_element(2 * i + 2) {
                Some(value) => value,
                None => {
                    // The existing sub commands will be set with Canceled.
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        b"ERR wrong number of arguments for 'mset' command".to_vec(),
                    )));
                    return reply_receiver.await;
                }
            };
            let resp = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(b"SET".to_vec())),
                Resp::Bulk(BulkStr::Str(key.to_vec())),
                Resp::Bulk(BulkStr::Str(value.to_vec())),
            ]));
            let (sub_cmd_ctx, fut) = factory.create_with_ctx(cmd_ctx.get_context(), resp);
            futs.push(fut);
            self.handle_single_key_data_cmd(sub_cmd_ctx);
        }

        if futs.is_empty() {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                b"ERR wrong number of arguments for 'mset' command".to_vec(),
            )));
            return reply_receiver.await;
        }

        let res = future::join_all(futs).await;
        for sub_result in res.into_iter() {
            let reply = match sub_result {
                Ok(reply) => reply,
                Err(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(format!("ERR: {}", err).into_bytes())));
                    return Err(err);
                }
            };
            if let Resp::Error(err) = reply {
                cmd_ctx.set_resp_result(Ok(Resp::Error(err)));
                return reply_receiver.await;
            }
        }

        let resp = Resp::Simple(response::OK_REPLY.to_string().into_bytes());
        cmd_ctx.set_resp_result(Ok(resp));
        reply_receiver.await
    }

    async fn handle_msetnx(&self, cmd_ctx: CmdCtx, reply_receiver: CmdReplyReceiver) -> TaskResult {
        let arg_len = cmd_ctx.get_cmd().get_command_len().unwrap_or(0);

        if !self.config.active_redirection {
            let in_same_slot = same_slot(
                (0..(arg_len / 2)).filter_map(|i| cmd_ctx.get_cmd().get_command_element(2 * i + 1)),
            );
            if !in_same_slot {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_THE_SAME_SLOT.to_string().into_bytes(),
                )));
                return reply_receiver.await;
            }
        }

        let mut slotted_kvs: HashMap<usize, Vec<Resp<Vec<u8>>>> = Default::default();
        for i in 0.. {
            let key = match cmd_ctx.get_cmd().get_command_element(2 * i + 1) {
                Some(key) => key,
                None => break,
            };
            let value = match cmd_ctx.get_cmd().get_command_element(2 * i + 2) {
                Some(value) => value,
                None => {
                    // The existing sub commands will be set with Canceled.
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        b"ERR wrong number of arguments for 'mset' command".to_vec(),
                    )));
                    return reply_receiver.await;
                }
            };
            let target_slot = slotted_kvs
                .entry(generate_slot(key))
                .or_insert_with(|| vec![Resp::Bulk(BulkStr::Str(b"MSETNX".to_vec()))]);
            target_slot.push(Resp::Bulk(BulkStr::Str(key.to_vec())));
            target_slot.push(Resp::Bulk(BulkStr::Str(value.to_vec())));
        }

        let factory = CmdCtxFactory::default();
        let mut futs = vec![];
        for (_, cmd) in slotted_kvs {
            let resp = Resp::Arr(Array::Arr(cmd));
            let (sub_cmd_ctx, fut) = factory.create_with_ctx(cmd_ctx.get_context(), resp);
            futs.push(fut);
            self.handle_single_key_data_cmd(sub_cmd_ctx);
        }

        if futs.is_empty() {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                b"ERR wrong number of arguments for 'mset' command".to_vec(),
            )));
            return reply_receiver.await;
        }

        let res = future::join_all(futs).await;
        let mut count = 0usize;
        for sub_result in res.into_iter() {
            let reply = match sub_result {
                Ok(reply) => reply,
                Err(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(format!("ERR: {}", err).into_bytes())));
                    return Err(err);
                }
            };
            match reply {
                Resp::Error(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(err.clone())));
                    return reply_receiver.await;
                }
                Resp::Integer(data) => {
                    let n = match btou::<usize>(&data) {
                        Ok(n) => n,
                        Err(err) => {
                            let err_str =
                                format!("unexpected reply from MSETNX: {:?} {:?}", data, err);
                            cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
                            return reply_receiver.await;
                        }
                    };
                    count += n;
                }
                others => {
                    let err_str = format!("unexpected reply from MSETNX: {:?}", others);
                    cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
                    return reply_receiver.await;
                }
            }
        }

        let resp = Resp::Integer(count.to_string().into_bytes());
        cmd_ctx.set_resp_result(Ok(resp));
        reply_receiver.await
    }

    // DEL and EXISTS
    async fn handle_multi_int_cmd(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
        cmd_name: &'static str,
    ) -> TaskResult {
        let arg_len = cmd_ctx.get_cmd().get_command_len().unwrap_or(0);

        if !self.config.active_redirection {
            let in_same_slot =
                same_slot((1..arg_len).filter_map(|i| cmd_ctx.get_cmd().get_command_element(i)));
            if !in_same_slot {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_THE_SAME_SLOT.to_string().into_bytes(),
                )));
                return reply_receiver.await;
            }
        }

        let factory = CmdCtxFactory::default();
        let mut futs = vec![];
        for i in 1.. {
            let key = match cmd_ctx.get_cmd().get_command_element(i) {
                Some(key) => key,
                None => break,
            };
            let resp = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(cmd_name.to_string().into_bytes())),
                Resp::Bulk(BulkStr::Str(key.to_vec())),
            ]));
            let (sub_cmd_ctx, fut) = factory.create_with_ctx(cmd_ctx.get_context(), resp);
            futs.push(fut);
            self.handle_single_key_data_cmd(sub_cmd_ctx);
        }

        if futs.is_empty() {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                format!("ERR wrong number of arguments for '{}' command", cmd_name).into_bytes(),
            )));
            return reply_receiver.await;
        }

        let mut count: usize = 0;
        let res = future::join_all(futs).await;
        for sub_result in res.into_iter() {
            let reply = match sub_result {
                Ok(reply) => reply,
                Err(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(format!("ERR: {}", err).into_bytes())));
                    return Err(err);
                }
            };
            match reply {
                Resp::Error(err) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(err.clone())));
                    return reply_receiver.await;
                }
                Resp::Integer(data) => {
                    let n = match btou::<usize>(&data) {
                        Ok(n) => n,
                        Err(err) => {
                            let err_str =
                                format!("unexpected reply from {}: {:?} {:?}", cmd_name, data, err);
                            cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
                            return reply_receiver.await;
                        }
                    };
                    count += n;
                }
                others => {
                    let err_str = format!("unexpected reply from {}: {:?}", cmd_name, others);
                    cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
                    return reply_receiver.await;
                }
            }
        }

        let resp = Resp::Integer(count.to_string().into_bytes());
        cmd_ctx.set_resp_result(Ok(resp));
        reply_receiver.await
    }

    async fn handle_blocking_commands(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
    ) -> TaskResult {
        let data_cmd_type = cmd_ctx.get_data_cmd_type();

        let non_blocking_cmd_name = match Self::get_non_blocking_name(&cmd_ctx, data_cmd_type) {
            Ok(non_blocking_cmd_name) => non_blocking_cmd_name,
            // unexpected command name, return early
            Err(resp) => {
                cmd_ctx.set_resp_result(Ok(resp));
                return reply_receiver.await;
            }
        };

        let arg_len = match Self::get_command_arg_len(&cmd_ctx, data_cmd_type) {
            Ok(len) => len,
            // invalid number of arguments, return early
            Err(resp) => {
                cmd_ctx.set_resp_result(Ok(resp));
                return reply_receiver.await;
            }
        };

        let timeout = match Self::get_blocking_command_timeout(&cmd_ctx) {
            Ok(timeout) => timeout,
            // fail to parse required timeout, return early
            Err(resp) => {
                cmd_ctx.set_resp_result(Ok(resp));
                return reply_receiver.await;
            }
        };

        if !self.config.active_redirection {
            let in_same_slot = same_slot(
                (1..(arg_len - 1)).filter_map(|i| cmd_ctx.get_cmd().get_command_element(i)),
            );
            if !in_same_slot {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    response::ERR_NOT_THE_SAME_SLOT.to_string().into_bytes(),
                )));
                return reply_receiver.await;
            }
        }

        let factory = CmdCtxFactory::default();
        let mut retry_num = 0;
        loop {
            let cmds = match Self::transfer_cmd_from_blocking_to_non_blocking(
                &cmd_ctx,
                data_cmd_type,
                arg_len,
                non_blocking_cmd_name,
            ) {
                Ok(cmds) => cmds,
                Err(resp) => {
                    cmd_ctx.set_resp_result(Ok(resp));
                    return reply_receiver.await;
                }
            };

            for (key, non_blocking_cmd) in cmds.into_iter() {
                let (sub_cmd_ctx, fut) =
                    factory.create_with_ctx(cmd_ctx.get_context(), non_blocking_cmd);
                self.handle_single_key_data_cmd(sub_cmd_ctx);

                let resp = match fut.await {
                    Err(err) => {
                        cmd_ctx.set_result(Err(err));
                        return reply_receiver.await;
                    }
                    Ok(resp) => resp,
                };

                if Self::is_empty_resp(&resp, data_cmd_type)
                    && (timeout == 0 || retry_num < timeout)
                {
                    continue;
                }

                let resp = match data_cmd_type {
                    DataCmdType::BLPOP | DataCmdType::BRPOP => {
                        Self::adjust_lrpop_response(resp, key)
                    }
                    DataCmdType::BZPOPMIN | DataCmdType::BZPOPMAX => {
                        Self::adjust_zpop_response(resp, key)
                    }
                    _ => resp,
                };
                cmd_ctx.set_resp_result(Ok(resp));
                return reply_receiver.await;
            }

            retry_num += 1;
            Delay::new(Duration::from_secs(1)).await;
        }
    }

    fn handle_single_key_data_cmd(&self, cmd_ctx: CmdCtx) {
        let mut cmd_ctx = cmd_ctx;
        match self.compressor.try_compressing_cmd_ctx(&mut cmd_ctx) {
            Ok(())
            | Err(CompressionError::UnsupportedCmdType)
            | Err(CompressionError::Disabled) => (),
            Err(CompressionError::InvalidRequest) | Err(CompressionError::InvalidResp) => {
                return cmd_ctx
                    .set_resp_result(Ok(Resp::Error("invalid command".to_string().into_bytes())));
            }
            Err(CompressionError::RestrictedCmd) => {
                let err_msg = "unsupported string command when compression is enabled";
                return cmd_ctx.set_resp_result(Ok(Resp::Error(err_msg.to_string().into_bytes())));
            }
            Err(CompressionError::Io(err)) => {
                return cmd_ctx.set_resp_result(Ok(Resp::Error(
                    format!("failed to compress data: {:?}", err).into_bytes(),
                )));
            }
        }
        self.manager.send(cmd_ctx);
    }

    fn handle_umforward(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
    ) -> CmdReplyFuture {
        let (mut cmd_ctx, redirection_times) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd.to_uppercase()),
            None => return CmdReplyFuture::Left(reply_receiver),
        };

        let times = match str::parse::<usize>(redirection_times.as_str()) {
            Ok(times) => times,
            Err(_) => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(b"invalid redirection times".to_vec())));
                return CmdReplyFuture::Left(reply_receiver);
            }
        };

        // UMFORWARD <redirection times>
        match cmd_ctx.extract_inner_cmd(2) {
            Some(cmd_len) if cmd_len > 0 => (),
            _ => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(b"missing forwarded command".to_vec())));
                return CmdReplyFuture::Left(reply_receiver);
            }
        }

        cmd_ctx.set_redirection_times(times);
        self.handle_data_cmd(cmd_ctx, reply_receiver)
    }

    fn handle_umsync(&self, cmd_ctx: CmdCtx) {
        self.manager.send_sync_task(cmd_ctx);
    }

    fn get_non_blocking_name(
        cmd_ctx: &CmdCtx,
        data_cmd_type: DataCmdType,
    ) -> Result<&'static str, RespVec> {
        match data_cmd_type {
            DataCmdType::BLPOP => Ok("LPOP"),
            DataCmdType::BRPOP => Ok("RPOP"),
            DataCmdType::BRPOPLPUSH => Ok("RPOPLPUSH"),
            DataCmdType::BZPOPMIN => Ok("ZPOPMIN"),
            DataCmdType::BZPOPMAX => Ok("ZPOPMAX"),
            _ => {
                let cmd_name = cmd_ctx
                    .get_cmd()
                    .get_command_name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::new);
                Err(Resp::Error(
                    format!("ERR unexpected command name '{}'", cmd_name).into_bytes(),
                ))
            }
        }
    }

    fn get_command_arg_len(cmd_ctx: &CmdCtx, data_cmd_type: DataCmdType) -> Result<usize, RespVec> {
        match (data_cmd_type, cmd_ctx.get_cmd().get_command_len()) {
            (DataCmdType::BLPOP, Some(len)) if len > 2 => Ok(len),
            (DataCmdType::BRPOP, Some(len)) if len > 2 => Ok(len),
            (DataCmdType::BRPOPLPUSH, Some(len)) if len == 4 => Ok(len),
            (DataCmdType::BZPOPMIN, Some(len)) if len > 2 => Ok(len),
            (DataCmdType::BZPOPMAX, Some(len)) if len > 2 => Ok(len),
            _ => {
                let cmd_name = cmd_ctx
                    .get_cmd()
                    .get_command_name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::new);
                Err(Resp::Error(
                    format!("ERR invalid argument number for {:?}", cmd_name).into_bytes(),
                ))
            }
        }
    }

    fn is_empty_resp(resp: &RespVec, data_cmd_type: DataCmdType) -> bool {
        let is_list_pop = matches!(
            data_cmd_type,
            DataCmdType::BLPOP | DataCmdType::BRPOP | DataCmdType::BRPOPLPUSH
        );
        let is_zset_pop = matches!(data_cmd_type, DataCmdType::BZPOPMIN | DataCmdType::BZPOPMAX);

        match resp {
            // nil bulk string for LPOP and RPOP
            Resp::Bulk(BulkStr::Nil) if is_list_pop => true,
            // empty array for ZPOPMIN and ZPOPMAX
            Resp::Arr(Array::Arr(arr)) if arr.is_empty() && is_zset_pop => true,
            _ => false,
        }
    }

    fn get_blocking_command_timeout(cmd_ctx: &CmdCtx) -> Result<u64, RespVec> {
        cmd_ctx
            .get_cmd()
            .get_command_last_element()
            .ok_or_else(|| Resp::Error(b"ERR wrong number of arguments".to_vec()))
            .and_then(|last| {
                btoi::btou::<u64>(last)
                    .map_err(|_| Resp::Error(b"ERR invalid timeout argument".to_vec()))
            })
    }

    fn transfer_cmd_from_blocking_to_non_blocking(
        cmd_ctx: &CmdCtx,
        data_cmd_type: DataCmdType,
        arg_len: usize,
        non_blocking_cmd_name: &str,
    ) -> Result<NonBlockingCommandsWithKey, RespVec> {
        let mut cmds = vec![];
        use DataCmdType::*;
        match data_cmd_type {
            BLPOP | BRPOP | BZPOPMIN | BZPOPMAX => {
                // exclude the timeout argument
                for i in 1..(arg_len - 1) {
                    let key = match cmd_ctx.get_cmd().get_command_element(i) {
                        None => break, // invalid state
                        Some(key) => key.to_vec(),
                    };
                    let non_blocking_cmd =
                        vec![non_blocking_cmd_name.to_string().into_bytes(), key.clone()];
                    let arr: Vec<RespVec> = non_blocking_cmd
                        .into_iter()
                        .map(|s| Resp::Bulk(BulkStr::Str(s)))
                        .collect();
                    let resp = Resp::Arr(Array::Arr(arr));
                    cmds.push((key, resp));
                }
            }
            BRPOPLPUSH => {
                let mut resp = cmd_ctx.get_cmd().get_resp_slice().map(|b| b.to_vec());
                change_bulk_array_element(
                    &mut resp,
                    0,
                    non_blocking_cmd_name.to_string().into_bytes(),
                );
                if let Resp::Arr(Array::Arr(ref mut resps)) = resp {
                    resps.pop(); // pop out the timeout argument
                }
                // BRPOPLPUSH does not need to care about key.
                cmds.push((vec![], resp));
            }
            _ => return Err(Resp::Error(b"ERR unsupported blocking command".to_vec())),
        }
        Ok(cmds)
    }

    fn adjust_lrpop_response(resp: RespVec, key: Vec<u8>) -> RespVec {
        match resp {
            Resp::Bulk(BulkStr::Nil) => {
                // BLPOP, BRPOP need to change response to Array::Nil.
                Resp::Arr(Array::Nil)
            }
            Resp::Bulk(BulkStr::Str(s)) => Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(key)),
                Resp::Bulk(BulkStr::Str(s)),
            ])),
            _ => resp,
        }
    }

    fn adjust_zpop_response(resp: RespVec, key: Vec<u8>) -> RespVec {
        match resp {
            Resp::Arr(Array::Arr(arr)) if arr.is_empty() => Resp::Arr(Array::Nil),
            Resp::Arr(Array::Arr(arr)) if arr.len() == 2 => {
                let mut ret = vec![Resp::Bulk(BulkStr::Str(key))];
                ret.extend(arr);
                Resp::Arr(Array::Arr(ret))
            }
            _ => resp,
        }
    }
}

impl<F, C> CmdCtxHandler for ForwardHandler<F, C>
    where
        F: RedisClientFactory,
        C: ConnFactory<Pkt=RespPacket>,
{
    fn handle_cmd_ctx(
        &self,
        cmd_ctx: CmdCtx,
        reply_receiver: CmdReplyReceiver,
        session_cluster_name: &parking_lot::RwLock<ClusterName>,
    ) -> CmdReplyFuture {
        let mut cmd_ctx = cmd_ctx;
        if self.config.auto_select_cluster {
            cmd_ctx = self.manager.try_select_cluster(cmd_ctx);
        }

        let cmd_type = cmd_ctx.get_cmd().get_type();
        match cmd_type {
            CmdType::Ping => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Info => self.handle_info(cmd_ctx),
            CmdType::Auth => self.handle_auth(cmd_ctx, session_cluster_name),
            CmdType::Quit => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Echo => {
                match cmd_ctx
                    .get_cmd()
                    .get_command_element(1)
                    .map(|msg| msg.to_vec())
                {
                    Some(msg) => cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(msg)))),
                    None => cmd_ctx.set_resp_result(Ok(Resp::Error(b"Missing message".to_vec()))),
                }
            }
            CmdType::Select => cmd_ctx.set_resp_result(Ok(Resp::Simple(
                response::OK_REPLY.to_string().into_bytes(),
            ))),
            CmdType::Invalid => cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid command").into_bytes(),
            ))),
            CmdType::UmCtl => self.handle_umctl(cmd_ctx),
            CmdType::UmForward => return self.handle_umforward(cmd_ctx, reply_receiver),
            CmdType::UmSync => self.handle_umsync(cmd_ctx),
            CmdType::Cluster => self.handle_cluster(cmd_ctx),
            CmdType::Config => self.handle_config(cmd_ctx),
            CmdType::Command => return self.handle_command_cmd(cmd_ctx, reply_receiver),
            CmdType::Asking => cmd_ctx.set_resp_result(Ok(Resp::Simple(
                response::OK_REPLY.to_string().into_bytes(),
            ))),
            CmdType::Hello => {
                // Redis 6 clients would use this command to change protocol.
                // Make server proxy act as low version Redis.
                let err_msg = b"ERR unknown command `hello`";
                cmd_ctx.set_resp_result(Ok(Resp::Error(err_msg.to_vec())))
            }
            CmdType::Others => return self.handle_data_cmd(cmd_ctx, reply_receiver),
        };
        CmdReplyFuture::Left(reply_receiver)
    }
}
