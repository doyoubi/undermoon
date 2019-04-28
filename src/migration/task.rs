use ::common::cluster::{SlotRange, SlotRangeTag};
use ::proxy::backend::CmdTask;
use ::proxy::database::DBSendError;
use futures::Future;
use protocol::{RedisClientError, RedisClientFactory};
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MigrationTaskKey {
    db_name: String,
    slot_range_start: usize,
    slot_range_end: usize,
}

#[derive(Debug)]
pub struct MigrationTaskMeta {
    epoch: u64,
    db_name: String,
    src_node_address: String,
    src_proxy_address: String,
    dst_node_address: String,
    dst_proxy_address: String,
    slot_range: SlotRange,
}

#[derive(Debug, Copy, Clone)]
pub enum MigrationState {
    TransferringData = 0,
    SwitchStarted = 1,
    SwitchCommitted = 2,
}

#[derive(Debug)]
pub struct AtomicMigrationState {
    inner: AtomicU8,
}

impl AtomicMigrationState {
    pub fn new() -> Self {
        Self {
            inner: AtomicU8::new(MigrationState::TransferringData as u8),
        }
    }

    pub fn set_state(&self, state: MigrationState) {
        self.inner.store(state as u8, Ordering::SeqCst);
    }

    pub fn get_state(&self) -> MigrationState {
        match self.inner.load(Ordering::SeqCst) {
            0 => MigrationState::TransferringData,
            1 => MigrationState::SwitchStarted,
            _ => MigrationState::SwitchCommitted,
        }
    }
}

pub trait MigratingTask {
    type Task: CmdTask;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>>;
}

pub trait ImportingTask {
    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
}

#[derive(Debug)]
pub enum MigrationError {
    IncompatibleVersion,
    InvalidAddress,
    AlreadyStarted,
    AlreadyEnded,
    Canceled,
    RedisError(RedisClientError),
    Io(io::Error),
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for MigrationError {
    fn description(&self) -> &str {
        "migration error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            MigrationError::Io(err) => Some(err),
            _ => None,
        }
    }
}
