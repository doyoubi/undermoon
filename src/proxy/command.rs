use super::slowlog::Slowlog;
use crate::common::utils::{byte_to_uppercase, generate_slot};
use crate::protocol::{BinSafeStr, RespPacket, RespSlice, RespVec};
use arrayvec::ArrayVec;
use backtrace::Backtrace;
use futures::channel::oneshot;
use futures::task::{Context, Poll};
use futures::Future;
use pin_project::pin_project;
use std::convert::identity;
use std::error::Error;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::result::Result;
use std::str;

const MAX_COMMAND_NAME_LENGTH: usize = 64;

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
    UmForward,
    Cluster,
    Config,
    Command,
}

impl CmdType {
    fn from_cmd_name(cmd_name: &[u8]) -> Self {
        let mut stack_cmd_name = ArrayVec::<[u8; MAX_COMMAND_NAME_LENGTH]>::new();
        for b in cmd_name {
            if let Err(err) = stack_cmd_name.try_push(byte_to_uppercase(*b)) {
                error!("Unexpected long command name: {:?} {:?}", cmd_name, err);
                return CmdType::Others;
            }
        }
        // The underlying `deref` will take the real length intead of the whole MAX_COMMAND_NAME_LENGTH array;
        let cmd_name: &[u8] = &stack_cmd_name;

        match cmd_name {
            b"PING" => CmdType::Ping,
            b"INFO" => CmdType::Info,
            b"AUTH" => CmdType::Auth,
            b"QUIT" => CmdType::Quit,
            b"ECHO" => CmdType::Echo,
            b"SELECT" => CmdType::Select,
            b"UMCTL" => CmdType::UmCtl,
            b"UMFORWARD" => CmdType::UmForward,
            b"CLUSTER" => CmdType::Cluster,
            b"CONFIG" => CmdType::Config,
            b"COMMAND" => CmdType::Command,
            _ => CmdType::Others,
        }
    }

    pub fn from_packet(packet: &RespPacket) -> Self {
        let cmd_name = match packet.get_array_element(0) {
            Some(cmd_name) => cmd_name,
            None => return CmdType::Invalid,
        };

        CmdType::from_cmd_name(cmd_name)
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum DataCmdType {
    // String commands
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
    DEL,
    EXISTS,
    // List commands
    BLPOP,
    BRPOP,
    BRPOPLPUSH,
    Others,
}

impl DataCmdType {
    fn from_cmd_name(cmd_name: &[u8]) -> Self {
        let mut stack_cmd_name = ArrayVec::<[u8; MAX_COMMAND_NAME_LENGTH]>::new();
        for b in cmd_name {
            if let Err(err) = stack_cmd_name.try_push(byte_to_uppercase(*b)) {
                error!(
                    "Unexpected long data command name: {:?} {:?}",
                    cmd_name, err
                );
                return DataCmdType::Others;
            }
        }
        // The underlying `deref` will take the real length intead of the whole MAX_COMMAND_NAME_LENGTH array;
        let cmd_name: &[u8] = &stack_cmd_name;

        match cmd_name {
            b"APPEND" => DataCmdType::APPEND,
            b"BITCOUNT" => DataCmdType::BITCOUNT,
            b"BITFIELD" => DataCmdType::BITFIELD,
            b"BITOP" => DataCmdType::BITOP,
            b"BITPOS" => DataCmdType::BITPOS,
            b"DECR" => DataCmdType::DECR,
            b"DECRBY" => DataCmdType::DECRBY,
            b"GET" => DataCmdType::GET,
            b"GETBIT" => DataCmdType::GETBIT,
            b"GETRANGE" => DataCmdType::GETRANGE,
            b"GETSET" => DataCmdType::GETSET,
            b"INCR" => DataCmdType::INCR,
            b"INCRBY" => DataCmdType::INCRBY,
            b"INCRBYFLOAT" => DataCmdType::INCRBYFLOAT,
            b"MGET" => DataCmdType::MGET,
            b"MSET" => DataCmdType::MSET,
            b"MSETNX" => DataCmdType::MSETNX,
            b"PSETEX" => DataCmdType::PSETEX,
            b"SET" => DataCmdType::SET,
            b"SETBIT" => DataCmdType::SETBIT,
            b"SETEX" => DataCmdType::SETEX,
            b"SETNX" => DataCmdType::SETNX,
            b"SETRANGE" => DataCmdType::SETRANGE,
            b"STRLEN" => DataCmdType::STRLEN,
            b"EVAL" => DataCmdType::EVAL,
            b"EVALSHA" => DataCmdType::EVALSHA,
            b"DEL" => DataCmdType::DEL,
            b"EXISTS" => DataCmdType::EXISTS,
            b"BLPOP" => DataCmdType::BLPOP,
            b"BRPOP" => DataCmdType::BRPOP,
            b"BRPOPLPUSH" => DataCmdType::BRPOPLPUSH,
            _ => DataCmdType::Others,
        }
    }

    pub fn from_packet(packet: &RespPacket) -> Self {
        let cmd_name = match packet.get_array_element(0) {
            Some(cmd_name) => cmd_name,
            None => return DataCmdType::Others,
        };

        DataCmdType::from_cmd_name(cmd_name)
    }
}

#[derive(Debug)]
struct CommandInfo {
    cmd_type: CmdType,
    data_cmd_type: DataCmdType,
    slot: Option<usize>,
}

impl CommandInfo {
    fn new(packet: &RespPacket) -> Self {
        let cmd_type = CmdType::from_packet(&packet);
        let data_cmd_type = DataCmdType::from_packet(&packet);
        let slot = Self::get_key(data_cmd_type, packet).map(generate_slot);
        Self {
            cmd_type,
            data_cmd_type,
            slot,
        }
    }

    fn get_key(data_cmd_type: DataCmdType, packet: &RespPacket) -> Option<&[u8]> {
        match data_cmd_type {
            DataCmdType::EVAL | DataCmdType::EVALSHA => packet.get_array_element(3),
            _ => packet.get_array_element(1),
        }
    }
}

#[derive(Debug)]
pub struct Command {
    request: Box<RespPacket>,
    info: CommandInfo,
}

impl Command {
    pub fn new(request: Box<RespPacket>) -> Self {
        let info = CommandInfo::new(&request);
        Self { request, info }
    }

    pub fn into_packet(self) -> Box<RespPacket> {
        self.request
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

    pub fn get_command_len(&self) -> Option<usize> {
        self.request.get_array_len()
    }

    pub fn get_command_last_element(&self) -> Option<&[u8]> {
        self.request.get_array_last_element()
    }

    pub fn get_command_name(&self) -> Option<&str> {
        self.request.get_command_name()
    }

    pub fn change_element(&mut self, index: usize, data: Vec<u8>) -> bool {
        self.request.change_bulk_array_element(index, data)
    }

    pub fn extract_inner_cmd(&mut self, removed_num: usize) -> Option<usize> {
        let remaining = self.request.left_trim_cmd(removed_num)?;
        self.info = CommandInfo::new(&self.request);
        Some(remaining)
    }

    pub fn wrap_cmd(&mut self, preceding_elements: Vec<BinSafeStr>) -> bool {
        if !self.request.wrap_cmd(preceding_elements) {
            return false;
        }
        self.info = CommandInfo::new(&self.request);
        true
    }

    pub fn get_type(&self) -> CmdType {
        self.info.cmd_type
    }

    pub fn get_data_cmd_type(&self) -> DataCmdType {
        self.info.data_cmd_type
    }

    pub fn get_key(&self) -> Option<&[u8]> {
        CommandInfo::get_key(self.get_data_cmd_type(), &self.request)
    }

    pub fn get_slot(&self) -> Option<usize> {
        self.info.slot
    }
}

pub struct TaskReply {
    request: Box<RespPacket>,
    packet: Box<RespPacket>,
    slowlog: Slowlog,
}

impl TaskReply {
    pub fn new(request: Box<RespPacket>, packet: Box<RespPacket>, slowlog: Slowlog) -> Self {
        Self {
            request,
            packet,
            slowlog,
        }
    }

    pub fn into_inner(self) -> (Box<RespPacket>, Box<RespPacket>, Slowlog) {
        let Self {
            request,
            packet,
            slowlog,
        } = self;
        (request, packet, slowlog)
    }

    pub fn into_resp_vec(self) -> RespVec {
        let (_, packet, _) = self.into_inner();
        packet.into_resp_vec()
    }
}

pub type CommandResult<T> = Result<Box<T>, CommandError>;
pub type TaskResult = Result<Box<TaskReply>, CommandError>;

pub fn new_command_pair(cmd: &Command) -> (CmdReplySender, CmdReplyReceiver) {
    let (s, r) = oneshot::channel::<TaskResult>();
    let reply_sender = CmdReplySender {
        data_cmd_type: cmd.get_data_cmd_type(),
        reply_sender: Some(s),
    };
    let reply_receiver = CmdReplyReceiver { reply_receiver: r };
    (reply_sender, reply_receiver)
}

pub struct CmdReplySender {
    data_cmd_type: DataCmdType,
    reply_sender: Option<oneshot::Sender<TaskResult>>,
}

impl fmt::Debug for CmdReplySender {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CmdReplySender")
    }
}

impl CmdReplySender {
    pub fn send(&mut self, res: TaskResult) -> Result<(), CommandError> {
        // Must not send twice.
        match self.try_send(res) {
            Some(res) => res,
            None => {
                error!("unexpected send again");
                Err(CommandError::InnerError)
            }
        }
    }

    fn try_send(&mut self, res: TaskResult) -> Option<Result<(), CommandError>> {
        // Must not send twice.
        match self.reply_sender.take() {
            Some(reply_sender) => {
                if let Err(CommandError::Dropped) = &res {
                    match self.data_cmd_type {
                        DataCmdType::BLPOP | DataCmdType::BRPOP | DataCmdType::BRPOPLPUSH => {
                            error!("blocking command is dropped")
                        }
                        _ => error!("command is dropped {:?}", Backtrace::new()),
                    }
                }
                Some(reply_sender.send(res).map_err(|_| CommandError::Canceled))
            }
            None => None,
        }
    }
}

// Make sure that result will always be sent back
impl Drop for CmdReplySender {
    fn drop(&mut self) {
        self.try_send(Err(CommandError::Dropped));
    }
}

#[pin_project]
pub struct CmdReplyReceiver {
    #[pin]
    reply_receiver: oneshot::Receiver<TaskResult>,
}

impl Future for CmdReplyReceiver {
    type Output = TaskResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().reply_receiver.poll(cx).map(|result| {
            result
                .map_err(|_| CommandError::Canceled)
                .and_then(identity)
        })
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

impl Clone for CommandError {
    fn clone(&self) -> Self {
        match self {
            Self::Io(ioerr) => {
                let err = io::Error::from(ioerr.kind());
                Self::Io(err)
            }
            Self::UnexpectedResponse => Self::UnexpectedResponse,
            Self::Dropped => Self::Dropped,
            Self::Canceled => Self::Canceled,
            Self::InnerError => Self::InnerError,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Array, BulkStr, Resp};

    #[test]
    fn test_parse_cmd_type() {
        assert_eq!(CmdType::from_cmd_name(b"pInG"), CmdType::Ping);
        assert_eq!(CmdType::from_cmd_name(b"get"), CmdType::Others);
    }

    #[test]
    fn test_parse_data_cmd_type() {
        assert_eq!(DataCmdType::from_cmd_name(b"aPPend"), DataCmdType::APPEND);
        assert_eq!(DataCmdType::from_cmd_name(b"get"), DataCmdType::GET);
        assert_eq!(DataCmdType::from_cmd_name(b"eVaL"), DataCmdType::EVAL);
        assert_eq!(DataCmdType::from_cmd_name(b"HMGET"), DataCmdType::Others);
    }

    #[test]
    fn test_umforward() {
        let request = RespPacket::Data(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(b"GET".to_vec())),
            Resp::Bulk(BulkStr::Str(b"somekey".to_vec())),
        ])));
        let mut cmd = Command::new(Box::new(request));
        assert_eq!(cmd.get_type(), CmdType::Others);
        assert_eq!(cmd.get_data_cmd_type(), DataCmdType::GET);

        assert!(cmd.wrap_cmd(vec![b"UMFORWARD".to_vec(), b"233".to_vec()]));
        assert_eq!(cmd.get_type(), CmdType::UmForward);
        assert_eq!(cmd.get_data_cmd_type(), DataCmdType::Others);

        let remaining = cmd.extract_inner_cmd(2).unwrap();
        assert_eq!(remaining, 2);
        assert_eq!(cmd.get_type(), CmdType::Others);
        assert_eq!(cmd.get_data_cmd_type(), DataCmdType::GET);
    }
}
