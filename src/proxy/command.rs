use ::common::utils::get_key;
use atomic_option::AtomicOption;
use bytes::BytesMut;
use futures::sync::oneshot;
use futures::{future, Future};
use protocol::{Array, BinSafeStr, BulkStr, Resp, RespPacket};
use std::error::Error;
use std::fmt;
use std::io;
use std::result::Result;
use std::str;
use std::sync::atomic::Ordering;

#[derive(Debug, PartialEq, Clone)]
pub enum CmdType {
    Ping,
    Info,
    Auth,
    Quit,
    Echo,
    Select,
    Others,
    Invalid,
    UmCtl,
    Cluster,
}

#[derive(Debug)]
pub struct Command {
    request: Box<RespPacket>,
}

impl Command {
    pub fn new(request: Box<RespPacket>) -> Self {
        Command { request }
    }

    pub fn drain_packet_data(&self) -> Option<BytesMut> {
        self.request.drain_data()
    }

    pub fn get_resp(&self) -> &Resp {
        self.request.get_resp()
    }

    pub fn get_type(&self) -> CmdType {
        let resps = match self.get_resp() {
            Resp::Arr(Array::Arr(ref resps)) => resps,
            _ => return CmdType::Invalid,
        };

        let first_resp = resps.first();
        let resp = match first_resp {
            Some(ref resp) => resp,
            None => return CmdType::Invalid,
        };

        let first = match resp {
            Resp::Bulk(BulkStr::Str(ref first)) => first,
            _ => return CmdType::Invalid,
        };

        let cmd_name = match str::from_utf8(first) {
            Ok(cmd_name) => cmd_name.to_uppercase(),
            Err(_) => return CmdType::Invalid,
        };

        if cmd_name.eq("PING") {
            CmdType::Ping
        } else if cmd_name.eq("INFO") {
            CmdType::Info
        } else if cmd_name.eq("AUTH") {
            CmdType::Auth
        } else if cmd_name.eq("QUIT") {
            CmdType::Quit
        } else if cmd_name.eq("ECHO") {
            CmdType::Echo
        } else if cmd_name.eq("SELECT") {
            CmdType::Select
        } else if cmd_name.eq("UMCTL") {
            CmdType::UmCtl
        } else if cmd_name.eq("CLUSTER") {
            CmdType::Cluster
        } else {
            CmdType::Others
        }
    }

    pub fn get_key(&self) -> Option<BinSafeStr> {
        get_key(self.get_resp())
    }
}

pub struct CmdReplySender {
    cmd: Command,
    reply_sender: AtomicOption<oneshot::Sender<CommandResult>>,
}

impl fmt::Debug for CmdReplySender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cmd: {:?}", self.cmd)
    }
}

pub struct CmdReplyReceiver {
    reply_receiver: oneshot::Receiver<CommandResult>,
}

pub fn new_command_pair(cmd: Command) -> (CmdReplySender, CmdReplyReceiver) {
    let (s, r) = oneshot::channel::<CommandResult>();
    let reply_sender = CmdReplySender {
        cmd,
        reply_sender: AtomicOption::new(Box::new(s)),
    };
    let reply_receiver = CmdReplyReceiver { reply_receiver: r };
    (reply_sender, reply_receiver)
}

impl CmdReplySender {
    pub fn get_cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn send(&self, res: CommandResult) -> Result<(), CommandError> {
        // Must not send twice.
        match self.reply_sender.take(Ordering::SeqCst) {
            Some(reply_sender) => reply_sender.send(res).map_err(|_| CommandError::Canceled),
            None => {
                error!("unexpected send again");
                Err(CommandError::InnerError)
            }
        }
    }

    pub fn try_send(&self, res: CommandResult) -> Option<Result<(), CommandError>> {
        match self.reply_sender.take(Ordering::SeqCst) {
            Some(reply_sender) => Some(reply_sender.send(res).map_err(|_| CommandError::Canceled)),
            None => None,
        }
    }
}

impl CmdReplyReceiver {
    pub fn wait_response(self) -> impl Future<Item = Box<RespPacket>, Error = CommandError> + Send {
        self.reply_receiver
            .map_err(|_| CommandError::Canceled)
            .and_then(future::result)
    }
}

#[derive(Debug)]
pub enum CommandError {
    Io(io::Error),
    UnexpectedResponse,
    Dropped,
    Canceled,
    InnerError,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for CommandError {
    fn description(&self) -> &str {
        "command error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            CommandError::Io(err) => Some(err),
            _ => None,
        }
    }
}

pub type CommandResult = Result<Box<RespPacket>, CommandError>;
