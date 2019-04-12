extern crate arc_swap;
extern crate atomic_option;
extern crate bytes;
extern crate caseless;
extern crate crc16;
extern crate futures;
extern crate futures_timer;
extern crate reqwest;
extern crate serde;
extern crate tokio;
extern crate tokio_core;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;
#[macro_use(defer)]
extern crate scopeguard;
extern crate btoi;
extern crate itertools;

mod common;
pub mod coordinator;
pub mod protocol;
pub mod proxy;
pub mod replication;
