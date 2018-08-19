extern crate undermoon;
extern crate tokio;
extern crate futures;

use std::iter;
use std::io;
use futures::{future, Future, stream, Stream};
use tokio::net::TcpListener;
use tokio::io::{write_all, AsyncRead};
use undermoon::protocol::{decode_resp, DecodeError};
use undermoon::proxy::session::Session;

fn main() {
    let addr = "127.0.0.1:5299".parse().unwrap();
    let listener = TcpListener::bind(&addr)
        .expect("unable to bind TCP listener");

    let server = listener.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(|sock| {
            println!("accept conn {:?}", sock);
            let handle_conn = Session{}.handle_conn(sock)
                .map_err(|err| {
                    eprintln!("IO error {:?}", err)
                });
            tokio::spawn(handle_conn)
        });

    tokio::run(server);
}