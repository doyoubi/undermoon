extern crate undermoon;
extern crate tokio;
extern crate futures;

use std::iter;
use std::io;
use futures::{future, Future, stream, Stream};
use tokio::net::TcpListener;
use tokio::io::{write_all, AsyncRead};
use undermoon::protocol::{decode_resp, DecodeError};

fn main() {
    let addr = "127.0.0.1:5299".parse().unwrap();
    let listener = TcpListener::bind(&addr)
        .expect("unable to bind TCP listener");

    let server = listener.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(|sock| {
            println!("accept conn {:?}", sock);
            let (reader, writer) = sock.split();
            let reader = io::BufReader::new(reader);
            let stream = stream::iter_ok(iter::repeat(()));
            let handler = stream
                .fold((reader, writer), |(reader, writer), _| {
                    println!("try decoding");
                    decode_resp(reader)
                        .then(|res| {
                            println!("result: {:?}", res);
                            match res {
                                Ok((reader, resp)) => {
                                    write_all(writer, "+Done\r\n")
                                        .map(move |(writer, s)| (reader, writer))
                                        .map_err(DecodeError::Io)
                                        .boxed()
                                },
                                Err(DecodeError::InvalidProtocol) => {
                                    write_all(writer, "-Err invalid protocol\r\n")
                                        .map_err(DecodeError::Io)
                                        .and_then(move |(_writer, _s)| future::err(DecodeError::InvalidProtocol))
                                        .boxed()
                                },
                                Err(e) => {
                                    println!("io error: {:?}", e);
                                    future::err(e).boxed()
                                },
                            }
                        })

                });
            let handle_conn = handler.map(|amt| {
                println!("wrote {:?} bytes", amt)
            }).map_err(|err: DecodeError| {
                eprintln!("IO error {:?}", err)
            });
            tokio::spawn(handle_conn)
        });

    tokio::run(server);
}