use super::slowlog::Slowlog;
use ::common::utils::{extract_command_name, get_command_element, get_element};
use atomic_option::AtomicOption;
use bytes::BytesMut;
use futures::sync::oneshot;
use futures::{future, Future};
use protocol::{Resp, RespPacket};
use std::error::Error;
use std::fmt;
use std::io;
use std::result::Result;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone, Copy)]
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
    Config,
}

impl CmdType {
    fn from_cmd_name(cmd_name: &str) -> Self {
        let cmd_name = cmd_name.to_uppercase();
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
        } else if cmd_name.eq("CONFIG") {
            CmdType::Config
        } else {
            CmdType::Others
        }
    }

    pub fn from_resp(resp: &Resp) -> Self {
        let cmd_name = match extract_command_name(resp) {
            Some(cmd_name) => cmd_name,
            None => return CmdType::Invalid,
        };

        CmdType::from_cmd_name(&cmd_name)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DataCmdType {
    APPEND,
    BITCOUNT,
    BITFIELD,
    BITOP,
    BITPOS,
    DECR,
    DECRBY,
    GET,
    GETBIT,
    GETRANGE,
    GETSET,
    INCR,
    INCRBY,
    INCRBYFLOAT,
    MGET,
    MSET,
    MSETNX,
    PSETEX,
    SET,
    SETBIT,
    SETEX,
    SETNX,
    SETRANGE,
    STRLEN,
    EVAL,
    EVALSHA,
    Others,
}

impl DataCmdType {
    fn from_cmd_name(cmd_name: &str) -> Self {
        let cmd_name = cmd_name.to_uppercase();
        match cmd_name.as_str() {
            "APPEND" => DataCmdType::APPEND,
            "BITCOUNT" => DataCmdType::BITCOUNT,
            "BITFIELD" => DataCmdType::BITFIELD,
            "BITOP" => DataCmdType::BITOP,
            "BITPOS" => DataCmdType::BITPOS,
            "DECR" => DataCmdType::DECR,
            "DECRBY" => DataCmdType::DECRBY,
            "GET" => DataCmdType::GET,
            "GETBIT" => DataCmdType::GETBIT,
            "GETRANGE" => DataCmdType::GETRANGE,
            "GETSET" => DataCmdType::GETSET,
            "INCR" => DataCmdType::INCR,
            "INCRBY" => DataCmdType::INCRBY,
            "INCRBYFLOAT" => DataCmdType::INCRBYFLOAT,
            "MGET" => DataCmdType::MGET,
            "MSET" => DataCmdType::MSET,
            "MSETNX" => DataCmdType::MSETNX,
            "PSETEX" => DataCmdType::PSETEX,
            "SET" => DataCmdType::SET,
            "SETBIT" => DataCmdType::SETBIT,
            "SETEX" => DataCmdType::SETEX,
            "SETNX" => DataCmdType::SETNX,
            "SETRANGE" => DataCmdType::SETRANGE,
            "STRLEN" => DataCmdType::STRLEN,
            "EVAL" => DataCmdType::EVAL,
            "EVALSHA" => DataCmdType::EVALSHA,
            _ => DataCmdType::Others,
        }
    }

    pub fn from_resp(resp: &Resp) -> Self {
        let cmd_name = match extract_command_name(resp) {
            Some(cmd_name) => cmd_name,
            None => return DataCmdType::Others,
        };

        DataCmdType::from_cmd_name(&cmd_name)
    }
}

#[derive(Debug)]
pub struct Command {
    request: Box<RespPacket>,
    cmd_type: CmdType,
    data_cmd_type: DataCmdType,
}

impl Command {
    pub fn new(request: Box<RespPacket>) -> Self {
        let cmd_type = CmdType::from_resp(request.get_resp());
        let data_cmd_type = DataCmdType::from_resp(request.get_resp());
        Command {
            request,
            cmd_type,
            data_cmd_type,
        }
    }

    pub fn drain_packet_data(&self) -> Option<BytesMut> {
        self.request.drain_data()
    }

    pub fn get_resp(&self) -> &Resp {
        self.request.get_resp()
    }

    pub fn get_command_element(&self, index: usize) -> Option<&[u8]> {
        get_command_element(self.get_resp(), index)
    }

    pub fn get_command_name(&self) -> Option<&str> {
        extract_command_name(&self.request.get_resp())
    }

    pub fn change_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        self.request.change_bulk_array_element(index, data)
    }

    pub fn get_type(&self) -> CmdType {
        self.cmd_type
    }

    pub fn get_data_cmd_type(&self) -> DataCmdType {
        self.data_cmd_type
    }

    pub fn gen_type(resp: &Resp) -> (CmdType, DataCmdType) {
        let cmd_name = match extract_command_name(resp) {
            Some(cmd_name) => cmd_name,
            None => return (CmdType::Invalid, DataCmdType::Others),
        };

        let cmd_type = CmdType::from_cmd_name(cmd_name);
        if cmd_type != CmdType::Others {
            (cmd_type, DataCmdType::Others)
        } else {
            (cmd_type, DataCmdType::from_cmd_name(cmd_name))
        }
    }

    pub fn get_key(&self) -> Option<&[u8]> {
        match self.data_cmd_type {
            DataCmdType::EVAL | DataCmdType::EVALSHA => get_element(self.get_resp(), 3),
            _ => get_element(self.get_resp(), 1),
        }
    }
}

pub struct TaskReply {
    packet: Box<RespPacket>,
    slowlog: Arc<Slowlog>,
}

impl TaskReply {
    pub fn new(packet: Box<RespPacket>, slowlog: Arc<Slowlog>) -> Self {
        Self { packet, slowlog }
    }

    pub fn into_inner(self) -> (Box<RespPacket>, Arc<Slowlog>) {
        let Self { packet, slowlog } = self;
        (packet, slowlog)
    }

    pub fn into_resp(self) -> Resp {
        let (packet, _) = self.into_inner();
        packet.into_resp()
    }
}

pub type CommandResult = Result<Box<RespPacket>, CommandError>;
pub type TaskResult = Result<Box<TaskReply>, CommandError>;

pub struct CmdReplySender {
    cmd: Command,
    reply_sender: AtomicOption<oneshot::Sender<TaskResult>>,
}

impl fmt::Debug for CmdReplySender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "cmd: {:?}", self.cmd)
    }
}

pub struct CmdReplyReceiver {
    reply_receiver: oneshot::Receiver<TaskResult>,
}

pub fn new_command_pair(cmd: Command) -> (CmdReplySender, CmdReplyReceiver) {
    let (s, r) = oneshot::channel::<TaskResult>();
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

    pub fn get_mut_cmd(&mut self) -> &mut Command {
        &mut self.cmd
    }

    pub fn send(&self, res: TaskResult) -> Result<(), CommandError> {
        // Must not send twice.
        match self.reply_sender.take(Ordering::SeqCst) {
            Some(reply_sender) => reply_sender.send(res).map_err(|_| CommandError::Canceled),
            None => {
                error!("unexpected send again");
                Err(CommandError::InnerError)
            }
        }
    }

    pub fn try_send(&self, res: TaskResult) -> Option<Result<(), CommandError>> {
        match self.reply_sender.take(Ordering::SeqCst) {
            Some(reply_sender) => Some(reply_sender.send(res).map_err(|_| CommandError::Canceled)),
            None => None,
        }
    }
}

impl CmdReplyReceiver {
    pub fn wait_response(self) -> impl Future<Item = Box<TaskReply>, Error = CommandError> + Send {
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

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            CommandError::Io(err) => Some(err),
            _ => None,
        }
    }
}
