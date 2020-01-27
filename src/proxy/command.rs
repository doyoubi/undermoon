use super::slowlog::Slowlog;
use atomic_option::AtomicOption;
use futures::sync::oneshot;
use futures::{future, Future};
use protocol::{RespPacket, RespSlice, RespVec};
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
    Command,
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
        } else if cmd_name.eq("COMMAND") {
            CmdType::Command
        } else {
            CmdType::Others
        }
    }

    pub fn from_packet(packet: &RespPacket) -> Self {
        let cmd_name = match packet.get_command_name() {
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

    pub fn from_packet(packet: &RespPacket) -> Self {
        let cmd_name = match packet.get_command_name() {
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
        let cmd_type = CmdType::from_packet(&request);
        let data_cmd_type = DataCmdType::from_packet(&request);
        Command {
            request,
            cmd_type,
            data_cmd_type,
        }
    }

    pub fn get_packet(&self) -> RespPacket {
        self.request.as_ref().clone()
    }

    pub fn get_resp_slice(&self) -> RespSlice {
        self.request.to_resp_slice()
    }

    pub fn get_command_element(&self, index: usize) -> Option<&[u8]> {
        self.request.get_array_element(index)
    }

    pub fn get_command_name(&self) -> Option<&str> {
        self.request.get_command_name()
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

    pub fn get_key(&self) -> Option<&[u8]> {
        match self.data_cmd_type {
            DataCmdType::EVAL | DataCmdType::EVALSHA => self.get_command_element(3),
            _ => self.get_command_element(1),
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

    pub fn into_resp_vec(self) -> RespVec {
        let (packet, _) = self.into_inner();
        packet.into_resp_vec()
    }
}

pub type CommandResult<T> = Result<Box<T>, CommandError>;
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
