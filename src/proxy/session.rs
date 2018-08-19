use std::io;
use std::iter;
use std::fmt;
use std::error::Error;
use std::result::Result;
use futures::{future, Future, stream, Stream};
use futures::executor::{spawn, Spawn};
use futures::sync::mpsc;
use futures::Sink;
use tokio::net::TcpStream;
use tokio::io::{write_all, AsyncRead};
use protocol::{Resp, Array, BulkStr, decode_resp, DecodeError};
use super::command::{CmdReplySender, CmdReplyReceiver, CommandResult, Command, new_command_task};

pub struct Session {
}

impl Session {
    pub fn handle_conn(&self, sock: TcpStream) {
        let (reader, writer) = sock.split();
        let reader = io::BufReader::new(reader);

        let (tx, rx) = mpsc::channel(1024);

        let reader_stream = stream::iter_ok(iter::repeat(()));
        let reader_handler = reader_stream.fold((tx, reader), move |(tx, reader), _| {
            decode_resp(reader)
                .then(|res| {
                    match res {
                        Ok((reader, resp)) => {
                            let (reply_sender, reply_receiver) = new_command_task(Command::new(resp));
                            // TODO: move it executor
                            let reply = Resp::Bulk(BulkStr::Str(String::from("done").into_bytes()));
                            reply_sender.send(Ok(reply)).unwrap();
                            tx.send(reply_receiver)
                                .map(move |tx| (tx, reader))
                                .map_err(|e| {
                                    println!("rx closed");
                                    SessionError::Canceled
                                })
                                .boxed()
                        },
                        Err(DecodeError::InvalidProtocol) => {
                            let (reply_sender, reply_receiver) = new_command_task(Command::new(Resp::Arr(Array::Nil)));
                            // TODO: move it executor
                            let reply = Resp::Error(String::from("Err invalid protocol").into_bytes());
                            reply_sender.send(Ok(reply)).unwrap();
                            tx.send(reply_receiver)
                                .map_err(|e| {
                                    println!("rx closed");
                                    SessionError::Canceled
                                })
                                .and_then(move |_tx| future::err(SessionError::InvalidProtocol))
                                .boxed()
                        },
                        Err(DecodeError::Io(e)) => {
                            println!("io error: {:?}", e);
                            future::err(SessionError::Io(e)).boxed()
                        },
                    }
                })
        });

        let writer_handler = rx
            .map_err(|e| SessionError::Canceled)
            .fold(writer, |writer, reply_receiver| {
                reply_receiver.wait_response()
                    .map_err(|e| SessionError::Other)
                    .then(|res| {
                        match res {
                            Ok(resp) => {
                                write_all(writer, "+done\r\n")
                                    .map(move |(writer, _)| writer)
                                    .map_err(SessionError::Io)
                                    .boxed()
                            },
                            Err(e) => {
                                write_all(writer, "-Err cmd error\r\n")
                                    .map(move |(writer, _)| writer)
                                    .map_err(SessionError::Io)
                                    .boxed()
                            },
                        }
                    })
            });

        let handler = reader_handler
            .map(|_| ())
            .select(writer_handler.map(|_| ()));

        spawn(handler.then(move |_| {
            println!("Connection closed.");
            Result::Ok::<(), SessionError>(())
        }));
    }
}

#[derive(Debug)]
pub enum SessionError {
    Io(io::Error),
    InvalidProtocol,
    Canceled,
    Other,  // TODO: remove this
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for SessionError {
    fn description(&self) -> &str {
        "session error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            SessionError::Io(err) => Some(err),
            _ => None,
        }
    }
}
