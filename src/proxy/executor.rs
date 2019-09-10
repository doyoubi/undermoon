use super::backend::CmdTask;
use super::command::CmdType;
use super::compress::{CmdCompressor, CompressionError};
use super::database::{DBError, DBTag};
use super::manager::{MetaManager, SharedMetaMap};
use super::service::ServerProxyConfig;
use super::session::{CmdCtx, CmdCtxHandler};
use super::slowlog::{slowlogs_to_resp, SlowRequestLogger};
use ::migration::manager::SwitchError;
use ::migration::task::parse_tmp_switch_command;
use atoi::atoi;
use caseless;
use common::db::ProxyDBMeta;
use common::utils::{
    get_element, ThreadSafe, NOT_READY_FOR_SWITCHING_REPLY, OLD_EPOCH_REPLY, TRY_AGAIN_REPLY,
};
use protocol::{Array, BulkStr, RedisClientFactory, Resp};
use replication::replicator::ReplicatorMeta;
use std::str;
use std::sync::{self, Arc};

pub struct SharedForwardHandler<F: RedisClientFactory> {
    handler: sync::Arc<ForwardHandler<F>>,
}

impl<F: RedisClientFactory> ThreadSafe for SharedForwardHandler<F> {}

impl<F: RedisClientFactory> Clone for SharedForwardHandler<F> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
        }
    }
}

impl<F: RedisClientFactory> SharedForwardHandler<F> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<F>,
        slow_request_logger: Arc<SlowRequestLogger>,
        meta_map: SharedMetaMap,
    ) -> Self {
        Self {
            handler: sync::Arc::new(ForwardHandler::new(
                config,
                client_factory,
                slow_request_logger,
                meta_map,
            )),
        }
    }
}

impl<F: RedisClientFactory> CmdCtxHandler for SharedForwardHandler<F> {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        self.handler.handle_cmd_ctx(cmd_ctx)
    }
}

pub struct ForwardHandler<F: RedisClientFactory> {
    config: Arc<ServerProxyConfig>,
    manager: MetaManager<F>,
    slow_request_logger: Arc<SlowRequestLogger>,
    compressor: CmdCompressor,
}

impl<F: RedisClientFactory> ForwardHandler<F> {
    pub fn new(
        config: Arc<ServerProxyConfig>,
        client_factory: Arc<F>,
        slow_request_logger: Arc<SlowRequestLogger>,
        meta_map: SharedMetaMap,
    ) -> Self {
        Self {
            config: config.clone(),
            manager: MetaManager::new(config, client_factory, meta_map.clone()),
            slow_request_logger,
            compressor: CmdCompressor::new(meta_map),
        }
    }
}

impl<F: RedisClientFactory> ForwardHandler<F> {
    fn handle_auth(&self, cmd_ctx: CmdCtx) {
        let key = cmd_ctx.get_cmd().get_key();
        match key {
            None => cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Missing database name").into_bytes(),
            ))),
            Some(db_name) => match str::from_utf8(&db_name) {
                Ok(ref db) => {
                    cmd_ctx.set_db_name(db.to_string());
                    cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
                }
                Err(_) => cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Invalid database name").into_bytes(),
                ))),
            },
        }
    }

    fn handle_cluster(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        if caseless::canonical_caseless_match_str(&sub_cmd, "nodes") {
            let cluster_nodes = self.manager.gen_cluster_nodes(cmd_ctx.get_db_name());
            cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(cluster_nodes.into_bytes()))))
        } else if caseless::canonical_caseless_match_str(&sub_cmd, "slots") {
            let cluster_slots = self.manager.gen_cluster_slots(cmd_ctx.get_db_name());
            match cluster_slots {
                Ok(resp) => cmd_ctx.set_resp_result(Ok(resp)),
                Err(s) => cmd_ctx.set_resp_result(Ok(Resp::Error(s.into_bytes()))),
            }
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Unsupported sub command").into_bytes(),
            )));
        }
    }

    fn get_sub_command(cmd_ctx: CmdCtx, index: usize) -> Option<(CmdCtx, String)> {
        match get_element(cmd_ctx.get_resp(), index) {
            None => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Missing sub command").into_bytes(),
                )));
                None
            }
            Some(ref k) => match str::from_utf8(k) {
                Ok(sub_cmd) => Some((cmd_ctx, sub_cmd.to_string())),
                Err(_) => {
                    cmd_ctx.set_resp_result(Ok(Resp::Error(
                        String::from("Invalid sub command").into_bytes(),
                    )));
                    None
                }
            },
        }
    }

    fn handle_umctl(&self, cmd_ctx: CmdCtx) {
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx, 1) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        let sub_cmd = sub_cmd.to_uppercase();

        if sub_cmd.eq("LISTDB") {
            let dbs = self.manager.get_dbs();
            let resps = dbs
                .into_iter()
                .map(|db| Resp::Bulk(BulkStr::Str(db.into_bytes())))
                .collect();
            cmd_ctx.set_resp_result(Ok(Resp::Arr(Array::Arr(resps))));
        } else if sub_cmd.eq("SETDB") {
            self.handle_umctl_setdb(cmd_ctx);
        } else if sub_cmd.eq("SETREPL") {
            self.handle_umctl_setrepl(cmd_ctx);
        } else if sub_cmd.eq("INFOREPL") {
            self.handle_umctl_info_repl(cmd_ctx);
        } else if sub_cmd.eq("INFOMGR") {
            self.handle_umctl_info_migration(cmd_ctx);
        } else if sub_cmd.eq("TMPSWITCH") {
            self.handle_umctl_tmp_switch(cmd_ctx);
        } else if sub_cmd.eq("SLOWLOG") {
            self.handle_umctl_slowlog(cmd_ctx);
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid sub command").into_bytes(),
            )));
        }
    }

    fn handle_umctl_setdb(&self, cmd_ctx: CmdCtx) {
        let (db_meta, extended_res) = match ProxyDBMeta::from_resp(cmd_ctx.get_cmd().get_resp()) {
            Ok(r) => r,
            Err(_) => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Invalid arguments").into_bytes(),
                )));
                return;
            }
        };

        match self.manager.set_meta(db_meta) {
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
                DBError::OldEpoch => cmd_ctx
                    .set_resp_result(Ok(Resp::Error(OLD_EPOCH_REPLY.to_string().into_bytes()))),
                DBError::TryAgain => cmd_ctx
                    .set_resp_result(Ok(Resp::Error(TRY_AGAIN_REPLY.to_string().into_bytes()))),
            },
        }
    }

    fn handle_umctl_setrepl(&self, cmd_ctx: CmdCtx) {
        let meta = match ReplicatorMeta::from_resp(cmd_ctx.get_cmd().get_resp()) {
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
                    DBError::OldEpoch => cmd_ctx
                        .set_resp_result(Ok(Resp::Error(OLD_EPOCH_REPLY.to_string().into_bytes()))),
                    DBError::TryAgain => cmd_ctx
                        .set_resp_result(Ok(Resp::Error(TRY_AGAIN_REPLY.to_string().into_bytes()))),
                }
            }
        }
    }

    fn handle_umctl_info_repl(&self, cmd_ctx: CmdCtx) {
        let report = self.manager.get_replication_info();
        cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(report.into_bytes()))));
    }

    fn handle_umctl_tmp_switch(&self, cmd_ctx: CmdCtx) {
        let switch_arg = match parse_tmp_switch_command(cmd_ctx.get_resp()) {
            Some(switch_meta) => switch_meta,
            None => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    "failed to parse TMPSWITCH arguments"
                        .to_string()
                        .into_bytes(),
                )));
                return;
            }
        };
        match self.manager.commit_importing(switch_arg) {
            Ok(()) => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple("OK".to_string().into_bytes())));
            }
            Err(err) => {
                let err_str = match err {
                    SwitchError::InvalidArg => "Invalid Arg".to_string(),
                    SwitchError::TaskNotFound => "No Corresponding Task Found".to_string(),
                    SwitchError::PeerMigrating => "Peer Not Migrating".to_string(),
                    SwitchError::NotReady => NOT_READY_FOR_SWITCHING_REPLY.to_string(),
                    SwitchError::MgrErr(err) => format!("switch failed: {:?}", err),
                };
                cmd_ctx.set_resp_result(Ok(Resp::Error(err_str.into_bytes())));
            }
        }
    }

    fn handle_umctl_info_migration(&self, cmd_ctx: CmdCtx) {
        let finished_tasks = self.manager.get_finished_migration_tasks();
        let packet: Vec<Resp> = finished_tasks
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
            let limit =
                get_element(cmd_ctx.get_resp(), 3).and_then(|element| atoi::<usize>(&element));
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

    fn handle_data_cmd(&self, cmd_ctx: CmdCtx) {
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
        self.manager.send(cmd_ctx)
    }
}

impl<F: RedisClientFactory> CmdCtxHandler for ForwardHandler<F> {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        let mut cmd_ctx = cmd_ctx;
        if self.config.auto_select_db {
            cmd_ctx = self.manager.try_select_db(cmd_ctx);
        }

        let cmd_type = cmd_ctx.get_cmd().get_type();
        match cmd_type {
            CmdType::Ping => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Info => cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(
                format!("version:dev\r\n\r\n{}", self.manager.info()).into_bytes(),
            )))),
            CmdType::Auth => self.handle_auth(cmd_ctx),
            CmdType::Quit => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Echo => {
                let req = cmd_ctx.get_cmd().get_resp().clone();
                cmd_ctx.set_resp_result(Ok(req))
            }
            CmdType::Select => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Others => self.handle_data_cmd(cmd_ctx),
            CmdType::Invalid => cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid command").into_bytes(),
            ))),
            CmdType::UmCtl => self.handle_umctl(cmd_ctx),
            CmdType::Cluster => self.handle_cluster(cmd_ctx),
            CmdType::Config => self.handle_config(cmd_ctx),
        };
    }
}
