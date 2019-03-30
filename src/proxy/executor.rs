use super::backend::{CmdTask, RRSenderGroup, RecoverableBackendNode};
use super::command::CmdType;
use super::database::{DBTag, DatabaseMap};
use super::session::{CmdCtx, CmdCtxHandler};
use caseless;
use common::db::HostDBMap;
use common::utils::ThreadSafe;
use protocol::{Array, BulkStr, Resp};
use std::str;
use std::sync;

#[derive(Clone)]
pub struct SharedForwardHandler {
    handler: sync::Arc<ForwardHandler>,
}

impl SharedForwardHandler {
    pub fn new(service_address: String) -> SharedForwardHandler {
        SharedForwardHandler {
            handler: sync::Arc::new(ForwardHandler::new(service_address)),
        }
    }
}

impl ThreadSafe for SharedForwardHandler {}

impl CmdCtxHandler for SharedForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        self.handler.handle_cmd_ctx(cmd_ctx)
    }
}

pub struct ForwardHandler {
    service_address: String,
    db: DatabaseMap<RRSenderGroup<RecoverableBackendNode<CmdCtx>>>,
}

impl ForwardHandler {
    pub fn new(service_address: String) -> ForwardHandler {
        let db = DatabaseMap::new();
        ForwardHandler {
            service_address,
            db,
        }
    }
}

impl ForwardHandler {
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
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        if caseless::canonical_caseless_match_str(&sub_cmd, "nodes") {
            let cluster_nodes = self
                .db
                .gen_cluster_nodes(cmd_ctx.get_db_name(), self.service_address.clone());
            cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(cluster_nodes.into_bytes()))))
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Unsupported sub command").into_bytes(),
            )));
        }
    }

    fn get_sub_command(cmd_ctx: CmdCtx) -> Option<(CmdCtx, String)> {
        match cmd_ctx.get_cmd().get_key() {
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
        let (cmd_ctx, sub_cmd) = match Self::get_sub_command(cmd_ctx) {
            Some((cmd_ctx, sub_cmd)) => (cmd_ctx, sub_cmd),
            None => return,
        };

        if caseless::canonical_caseless_match_str(&sub_cmd, "listdb") {
            let dbs = self.db.get_dbs();
            let resps = dbs
                .into_iter()
                .map(|db| Resp::Bulk(BulkStr::Str(db.into_bytes())))
                .collect();
            cmd_ctx.set_resp_result(Ok(Resp::Arr(Array::Arr(resps))));
        } else if caseless::canonical_caseless_match_str(&sub_cmd, "cleardb") {
            self.db.clear();
            cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())));
        } else if caseless::canonical_caseless_match_str(&sub_cmd, "setdb") {
            self.handle_umctl_setdb(cmd_ctx);
        } else if caseless::canonical_caseless_match_str(&sub_cmd, "setpeer") {
            self.handle_umctl_setpeer(cmd_ctx);
        } else {
            cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid sub command").into_bytes(),
            )));
        }
    }

    fn handle_umctl_setdb(&self, cmd_ctx: CmdCtx) {
        let db_map = match HostDBMap::from_resp(cmd_ctx.get_cmd().get_resp()) {
            Ok(db_map) => db_map,
            Err(_) => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Invalid arguments").into_bytes(),
                )));
                return;
            }
        };

        debug!("local meta data: {:?}", db_map);
        match self.db.set_dbs(db_map) {
            Ok(()) => {
                debug!("Successfully update local meta data");
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())));
            }
            Err(e) => {
                debug!("Failed to update local meta data {:?}", e);
                cmd_ctx.set_resp_result(Ok(Resp::Error(format!("{}", e).into_bytes())))
            }
        }
    }

    fn handle_umctl_setpeer(&self, cmd_ctx: CmdCtx) {
        let db_map = match HostDBMap::from_resp(cmd_ctx.get_cmd().get_resp()) {
            Ok(db_map) => db_map,
            Err(_) => {
                cmd_ctx.set_resp_result(Ok(Resp::Error(
                    String::from("Invalid arguments").into_bytes(),
                )));
                return;
            }
        };

        match self.db.set_peers(db_map) {
            Ok(()) => {
                debug!("Successfully update peer meta data");
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())));
            }
            Err(e) => {
                debug!("Failed to update peer meta data {:?}", e);
                cmd_ctx.set_resp_result(Ok(Resp::Error(format!("{}", e).into_bytes())))
            }
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        //        debug!("get command {:?}", cmd_ctx.get_cmd());
        let cmd_type = cmd_ctx.get_cmd().get_type();
        match cmd_type {
            CmdType::Ping => {
                cmd_ctx.set_resp_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Info => cmd_ctx.set_resp_result(Ok(Resp::Bulk(BulkStr::Str(
                String::from("version:dev\r\n").into_bytes(),
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
            CmdType::Others => {
                let res = self.db.send(cmd_ctx);
                if let Err(e) = res {
                    error!("Failed to foward cmd_ctx: {:?}", e)
                }
            }
            CmdType::Invalid => cmd_ctx.set_resp_result(Ok(Resp::Error(
                String::from("Invalid command").into_bytes(),
            ))),
            CmdType::UmCtl => self.handle_umctl(cmd_ctx),
            CmdType::Cluster => self.handle_cluster(cmd_ctx),
        };
    }
}
