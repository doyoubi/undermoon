use ::common::cluster::MigrationTaskMeta;
use ::common::utils::{get_resp_strings, ThreadSafe};
use ::protocol::Resp;
use ::proxy::backend::CmdTask;
use ::proxy::database::DBSendError;
use futures::Future;
use replication::replicator::ReplicatorError;
use std::error::Error;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MigrationState {
    TransferringData = 0,
    Blocking = 1,
    SwitchStarted = 2,
    SwitchCommitted = 3,
    // The states above are for migration protocol v2.
    PreCheck = 4,
    PreBlocking = 5,
    PreSwitch = 6,
    Scanning = 7,
    FinalSwitch = 8,
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
            1 => MigrationState::Blocking,
            2 => MigrationState::SwitchStarted,
            _ => MigrationState::SwitchCommitted,
        }
    }
}

pub trait MigratingTask: ThreadSafe {
    type Task: CmdTask;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>>;
    fn get_state(&self) -> MigrationState;
}

pub trait ImportingTask: ThreadSafe {
    type Task: CmdTask;

    fn start(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn stop(&self) -> Box<dyn Future<Item = (), Error = MigrationError> + Send>;
    fn send(&self, cmd_task: Self::Task) -> Result<(), DBSendError<Self::Task>>;
    fn commit(&self, switch_arg: SwitchArg) -> Result<(), MigrationError>;
}

pub struct SwitchArg {
    pub version: String,
    pub meta: MigrationTaskMeta,
}

impl SwitchArg {
    pub fn into_strings(self) -> Vec<String> {
        let SwitchArg { version, meta } = self;
        let mut strs = vec![version];
        strs.extend(meta.into_strings().into_iter());
        strs
    }

    pub fn from_strings<It>(it: &mut It) -> Option<Self>
    where
        It: Iterator<Item = String>,
    {
        let version = it.next()?;
        let meta = MigrationTaskMeta::from_strings(it)?;
        Some(Self { version, meta })
    }
}

pub fn parse_tmp_switch_command(resp: &Resp) -> Option<SwitchArg> {
    let command = get_resp_strings(resp)?;
    let mut it = command.into_iter();
    // Skip UMCTL TMPSWITCH
    it.next()?;
    it.next()?;
    SwitchArg::from_strings(&mut it)
}

#[derive(Debug)]
pub enum MigrationError {
    IncompatibleVersion,
    AlreadyStarted,
    AlreadyEnded,
    Canceled,
    NotReady,
    ReplError(ReplicatorError),
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

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            MigrationError::Io(err) => Some(err),
            MigrationError::ReplError(err) => Some(err),
            _ => None,
        }
    }
}
