use super::command::Command;
use arc_swap::ArcSwapOption;
use arr_macro::arr;
use chrono::Utc;
use protocol::BinSafeStr;
use protocol::{Array, BulkStr, Resp};
use std::sync::atomic;
use std::sync::Arc;

#[repr(u8)]
#[derive(Debug)]
pub enum TaskEvent {
    Created = 0,
    SentToWritingQueue = 1,
    WritingQueueReceived = 2,
    SentToBackend = 3,
    ReceivedFromBackend = 4,
    WaitDone = 5,
}

const EVENT_NUMBER: usize = 6;
const LOG_ELEMENT_NUMBER: usize = 5;

#[derive(Debug)]
struct RequestEventMap {
    events: [atomic::AtomicI64; EVENT_NUMBER],
}

impl RequestEventMap {
    fn set_event_time(&self, event: TaskEvent, timestamp: i64) {
        self.events[event as usize].store(timestamp, atomic::Ordering::SeqCst)
    }

    fn get_event_time(&self, event: TaskEvent) -> i64 {
        self.events[event as usize].load(atomic::Ordering::SeqCst)
    }
}

impl Default for RequestEventMap {
    fn default() -> Self {
        Self {
            events: arr![atomic::AtomicI64::new(0); 6],
        }
    }
}

#[derive(Debug)]
pub struct Slowlog {
    event_map: RequestEventMap,
    command: Vec<BinSafeStr>,
}

impl Slowlog {
    pub fn from_command(command: &Command) -> Self {
        let command = Self::get_brief_command(command);
        Slowlog {
            event_map: RequestEventMap::default(),
            command,
        }
    }

    fn get_brief_command(command: &Command) -> Vec<BinSafeStr> {
        let resps = match command.get_resp() {
            Resp::Arr(Array::Arr(ref resps)) => resps,
            others => return vec![format!("{:?}", others).into_bytes()],
        };
        resps
            .iter()
            .take(LOG_ELEMENT_NUMBER)
            .map(|element| match element {
                Resp::Bulk(BulkStr::Str(s)) => s.clone(),
                others => format!("{:?}", others).into_bytes(),
            })
            .collect()
    }

    pub fn log_event(&self, event: TaskEvent) {
        self.event_map
            .set_event_time(event, Utc::now().timestamp_nanos())
    }
}

pub struct SlowRequestLogger {
    slowlogs: Vec<ArcSwapOption<Slowlog>>,
    curr_index: atomic::AtomicUsize,
}

impl SlowRequestLogger {
    pub fn new(log_queue_size: usize) -> Self {
        let mut slowlogs = Vec::new();
        while slowlogs.len() != log_queue_size {
            slowlogs.push(ArcSwapOption::new(None));
        }
        Self {
            slowlogs,
            curr_index: atomic::AtomicUsize::new(0),
        }
    }

    pub fn add(&self, log: Arc<Slowlog>) {
        let index = self.curr_index.fetch_add(1, atomic::Ordering::SeqCst) % self.slowlogs.len();
        if let Some(log_slot) = self.slowlogs.get(index) {
            log_slot.store(Some(log))
        }
    }

    pub fn get(&self, num: usize) -> Vec<Arc<Slowlog>> {
        self.slowlogs
            .iter()
            .filter_map(arc_swap::ArcSwapAny::load)
            .take(num)
            .collect()
    }

    pub fn reset(&self) {
        for log_slot in self.slowlogs.iter() {
            log_slot.store(None)
        }
    }
}
