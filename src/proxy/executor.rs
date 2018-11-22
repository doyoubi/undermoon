use std::str;
use protocol::{Resp, BulkStr};
use super::session::{CmdCtxHandler, CmdCtx};
use super::backend::{RecoverableBackendNode, CmdTask};
use super::database::{DatabaseMap, DBTag};
use super::command::{CmdType};

pub struct ForwardHandler {
    task_sender: DatabaseMap<RecoverableBackendNode<CmdCtx>>,
}

impl ForwardHandler {
    pub fn new() -> ForwardHandler {
        let db = DatabaseMap::new();
        ForwardHandler{
            task_sender: db,
        }
    }
}

impl ForwardHandler {
    fn handle_auth(&self, cmd_ctx: CmdCtx) {
        let key = cmd_ctx.get_cmd().get_key();
        match key {
            None => {
                cmd_ctx.set_result(Ok(Resp::Error(String::from("Missing database name").into_bytes())))
            }
            Some(db_name) => {
                match str::from_utf8(&db_name) {
                    Ok(ref db) => {
                        cmd_ctx.set_db_name(db.to_string());
                        cmd_ctx.set_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
                    }
                    Err(_) => {
                        cmd_ctx.set_result(Ok(Resp::Error(String::from("Invalid database name").into_bytes())))
                    }
                }
            }
        }
    }
}

impl CmdCtxHandler for ForwardHandler {
    fn handle_cmd_ctx(&self, cmd_ctx: CmdCtx) {
        let cmd_type = cmd_ctx.get_cmd().get_type();
        match cmd_type {
            CmdType::Ping => {
                cmd_ctx.set_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Info => {
                cmd_ctx.set_result(Ok(Resp::Bulk(BulkStr::Str(String::from("version:dev\r\n").into_bytes()))))
            }
            CmdType::Auth => {
                self.handle_auth(cmd_ctx)
            }
            CmdType::Quit => {
                cmd_ctx.set_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Echo => {
                let req = cmd_ctx.get_cmd().get_resp().clone();
                cmd_ctx.set_result(Ok(req))
            }
            CmdType::Select => {
                cmd_ctx.set_result(Ok(Resp::Simple(String::from("OK").into_bytes())))
            }
            CmdType::Others => {
                let res = self.task_sender.send(cmd_ctx);
                if let Err(e) = res {
                    println!("Failed to foward cmd_ctx: {:?}", e)
                }
            }
            CmdType::Invalid => {
                cmd_ctx.set_result(Ok(Resp::Error(String::from("Invalid command").into_bytes())))
            }
        };
    }
}
