extern crate tokio;
extern crate futures;
extern crate atomic_option;
extern crate crc16;
extern crate caseless;
extern crate arc_swap;
extern crate reqwest;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

pub mod protocol;
pub mod proxy;
pub mod coordinator;
mod common;