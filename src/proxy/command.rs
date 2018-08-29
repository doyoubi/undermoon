use std::io;
use std::fmt;
use std::error::Error;
use std::result::Result;
use futures::{future, Future};
use futures::sync::oneshot;
use protocol::Resp;

pub struct Command {
    request: Resp
}

impl Command {
    pub fn new(request: Resp) -> Self {
        Command{
            request: request,
        }
    }

    pub fn get_resp(&self) -> &Resp {
        &self.request
    }
}

pub struct CmdReplySender {
    cmd: Command,
    reply_sender: oneshot::Sender<CommandResult>,
}

pub struct CmdReplyReceiver {
    reply_receiver: oneshot::Receiver<CommandResult>,
}


pub fn new_command_pair(cmd: Command) -> (CmdReplySender, CmdReplyReceiver) {
    let (s, r) = oneshot::channel::<CommandResult>();
    let reply_sender = CmdReplySender{
        cmd: cmd,
        reply_sender: s,
    };
    let reply_receiver = CmdReplyReceiver{
        reply_receiver: r,
    };
    (reply_sender, reply_receiver)
}

impl CmdReplySender {
    pub fn get_cmd(&self) -> &Command {
        &self.cmd
    }

    pub fn send(self, res: CommandResult) -> Result<(), CommandError> {
        self.reply_sender.send(res)
            .map_err(|_| CommandError::Canceled)
    }
}

impl CmdReplyReceiver {
    pub fn wait_response(self) -> impl Future<Item = Resp, Error = CommandError> + Send {
        self.reply_receiver
            .map_err(|_| CommandError::Canceled)
            .and_then(|result: CommandResult| {
                future::result(result)
            })
    }
}

#[derive(Debug)]
pub enum CommandError {
    Io(io::Error),
    Canceled,
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

pub type CommandResult = Result<Resp, CommandError>;
