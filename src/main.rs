extern crate undermoon;
extern crate tokio;
extern crate futures;

use futures::{Future, Stream};
use tokio::net::TcpListener;
use undermoon::proxy::session::{Session, handle_conn};
use undermoon::proxy::executor::ForwardHandler;

fn main() {
    let addr = "127.0.0.1:5299".parse().unwrap();
    let listener = TcpListener::bind(&addr)
        .expect("unable to bind TCP listener");

    let server = listener.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(|sock| {
            println!("accept conn {:?}", sock);
            let handle_conn = handle_conn(Session::new(ForwardHandler::new()), sock)
                .map_err(|err| {
                    eprintln!("IO error {:?}", err)
                });
            tokio::spawn(handle_conn)
        });

    tokio::run(server);
}