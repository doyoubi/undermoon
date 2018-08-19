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

pub struct CmdReplySender {
    cmd: Command,
    reply_sender: oneshot::Sender<CommandResult>,
}

pub struct CmdReplyReceiver {
    reply_receiver: oneshot::Receiver<CommandResult>,
}


pub fn new_command_task(cmd: Command) -> (CmdReplySender, CmdReplyReceiver) {
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
    fn get_request(&self) -> &Resp {
        &self.request
    }
}

impl CmdReplyReceiver {
    fn wait_response(self) -> impl Future<Item = Resp, Error = CommandError> + Send {
        self.reply_receiver
            .map_err(CommandError::Canceled)
            .and_then(|result: CommandResult| {
                future::result(result)
            })
    }
}

#[derive(Debug)]
pub enum CommandError {
    Io(io::Error),
    Canceled(oneshot::Canceled),
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

type CommandResult = Result<Resp, CommandError>;
