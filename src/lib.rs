#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use(defer)]
extern crate scopeguard;

pub mod broker;
pub mod common;
pub mod coordinator;
pub mod migration;
pub mod protocol;
pub mod proxy;
pub mod replication;

pub use self::migration::MAX_REDIRECTIONS;
