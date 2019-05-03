use ::common::cluster::SlotRange;
use ::common::utils::ThreadSafe;
use ::proxy::backend::CmdTask;
use ::proxy::database::DBSendError;
use futures::Future;
use protocol::RedisClientError;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MigrationTaskMeta {
    pub db_name: String,
    pub slot_range: SlotRange,
}

#[derive(Debug, PartialEq, Copy, Clone)]
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

pub trait MigratingTask: ThreadSafe {
    type Task: CmdTask;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>>;
}

pub trait ImportingTask: ThreadSafe {
    type Task: CmdTask;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>>;
    fn commit(&self) -> Result<(), MigrationError>;
}

pub struct MigrationConfig {
    lag_threshold: AtomicU64,
    replication_timeout: AtomicU64, // milliseconds
    block_time: AtomicU64,          // in milliseconds
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            lag_threshold: AtomicU64::new(50000),
            replication_timeout: AtomicU64::new(10 * 60 * 1000), // 10 minutes
            block_time: AtomicU64::new(10),                      // 10ms
        }
    }
}

impl MigrationConfig {
    pub fn new(lag_threshold: u64, replication_timeout: u64, block_time: u64) -> Self {
        Self {
            lag_threshold: AtomicU64::new(lag_threshold),
            replication_timeout: AtomicU64::new(replication_timeout),
            block_time: AtomicU64::new(block_time),
        }
    }

    pub fn set_lag_threshold(&self, lag_threshold: u64) {
        self.lag_threshold.store(lag_threshold, Ordering::SeqCst)
    }
    pub fn set_replication_timeout(&self, replication_timeout: u64) {
        self.replication_timeout
            .store(replication_timeout, Ordering::SeqCst)
    }
    pub fn set_block_time(&self, block_time: u64) {
        self.block_time.store(block_time, Ordering::SeqCst)
    }
    pub fn get_lag_threshold(&self) -> u64 {
        self.lag_threshold.load(Ordering::SeqCst)
    }
    pub fn get_replication_timeout(&self) -> u64 {
        self.replication_timeout.load(Ordering::SeqCst)
    }
    pub fn get_block_time(&self) -> u64 {
        self.block_time.load(Ordering::SeqCst)
    }
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
