use ::common::cluster::MigrationTaskMeta;
use ::common::utils::{get_resp_bytes, get_resp_strings, get_slot, ThreadSafe};
use ::proxy::backend::CmdTask;
use ::proxy::database::DBSendError;
use futures::Future;
use itertools::Itertools;
use protocol::{Array, BinSafeStr, BulkStr, RedisClientError, Resp};
use replication::replicator::ReplicatorError;
use std::error::Error;
use std::fmt;
use std::io;
use std::str;
use std::sync::atomic::{AtomicU16, Ordering};

#[derive(Debug)]
pub enum MgrSubCmd {
    PreCheck,
    PreSwitch,
    FinalSwitch,
}

impl MgrSubCmd {
    pub fn as_str(&self) -> &str {
        match self {
            &Self::PreCheck => "PRECHECK",
            &Self::PreSwitch => "PRESWITCH",
            &Self::FinalSwitch => "FINALSWITCH",
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum MigrationState {
    TransferringData = 0,
    Blocking = 1,
    SwitchStarted = 2,
    SwitchCommitted = 3,
    // The states below are for migration protocol v2.
    PreCheck = 4,
    PreBlocking = 5,
    PreSwitch = 6,
    Scanning = 7,
    FinalSwitch = 8,
}

#[derive(Debug)]
pub struct AtomicMigrationState {
    inner: AtomicU16,
}

impl AtomicMigrationState {
    pub fn new() -> Self {
        Self {
            inner: AtomicU16::new(MigrationState::PreCheck as u16),
        }
    }

    pub fn set_state(&self, state: MigrationState) {
        self.inner.store(state as u16, Ordering::SeqCst);
    }

    pub fn get_state(&self) -> MigrationState {
        match self.inner.load(Ordering::SeqCst) {
            0 => MigrationState::TransferringData,
            1 => MigrationState::Blocking,
            2 => MigrationState::SwitchStarted,
            3 => MigrationState::SwitchCommitted,
            4 => MigrationState::PreCheck,
            5 => MigrationState::PreBlocking,
            6 => MigrationState::PreSwitch,
            7 => MigrationState::Scanning,
            8 => MigrationState::FinalSwitch,
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
    fn get_state(&self) -> MigrationState;
    fn handle_switch(
        &self,
        switch_arg: SwitchArg,
        sub_cmd: MgrSubCmd,
    ) -> Result<(), MigrationError>;
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

pub fn parse_switch_command(resp: &Resp) -> Option<SwitchArg> {
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
    RedisClient(RedisClientError),
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
            MigrationError::RedisClient(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct SlotRangeArray {
    pub ranges: Vec<(usize, usize)>,
}

impl SlotRangeArray {
    pub fn is_key_inside(&self, key: &[u8]) -> bool {
        let slot = get_slot(key);
        for (start, end) in self.ranges.iter() {
            if slot >= *start && slot <= *end {
                return true;
            }
        }
        false
    }

    pub fn info(&self) -> String {
        self.ranges
            .iter()
            .map(|(start, end)| format!("{}-{}", start, end))
            .join(",")
    }
}

pub struct ScanResponse {
    pub next_index: u64,
    pub keys: Vec<BinSafeStr>,
}

impl ScanResponse {
    pub fn parse_scan(resp: Resp) -> Option<ScanResponse> {
        match resp {
            Resp::Arr(Array::Arr(ref resps)) => {
                let index_data = resps.get(0).and_then(|resp| match resp {
                    Resp::Bulk(BulkStr::Str(ref s)) => Some(s.clone()),
                    Resp::Simple(ref s) => Some(s.clone()),
                    _ => None,
                })?;
                let next_index = str::from_utf8(index_data.as_slice()).ok()?.parse().ok()?;
                let keys = get_resp_bytes(resps.get(1)?)?;
                Some(ScanResponse { next_index, keys })
            }
            _ => None,
        }
    }
}
