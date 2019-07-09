use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use enum_map::{Enum, EnumMap};
use protocol::BinSafeStr;
use std::sync::atomic;
use std::sync::Arc;

#[derive(Debug, Enum)]
pub enum RequestEvent {
    Created,
    SentToWritingQueue,
    WritingQueueReceived,
    SentToBackend,
    ReadingQueueReceived,
    ReceivedFromBackend,
    WaitDone,
    WrittenToClient,
}

struct Slowlog {
    eventMap: EnumMap<RequestEvent, Option<DateTime<Utc>>>,
    command: Vec<BinSafeStr>,
}

impl Slowlog {
    fn new() -> Self {
        Slowlog {
            eventMap: EnumMap::new(),
            command: Vec::new(),
        }
    }

    fn log_event(&mut self, event: RequestEvent) {
        self.eventMap[event] = Some(Utc::now());
    }
}

pub struct SlowRequestLogger {
    slowlogs: Vec<ArcSwapOption<Slowlog>>,
    curr_index: atomic::AtomicUsize,
}

impl SlowRequestLogger {
    fn new(log_queue_size: usize) -> Self {
        let mut slowlogs = Vec::new();
        while slowlogs.len() != log_queue_size {
            slowlogs.push(ArcSwapOption::new(None));
        }
        Self {
            slowlogs,
            curr_index: atomic::AtomicUsize::new(0),
        }
    }

    fn add(&self, log: Slowlog) {
        let index = self.curr_index.fetch_add(1, atomic::Ordering::SeqCst) % self.slowlogs.len();
        self.slowlogs
            .get(index)
            .map(|log_slot| log_slot.store(Some(Arc::new(log))));
    }

    fn get(&self, num: usize) -> Vec<Arc<Slowlog>> {
        self.slowlogs
            .iter()
            .filter_map(|log_slot| log_slot.load())
            .take(num)
            .collect()
    }

    fn reset(&self) {
        self.slowlogs.iter().map(|log_slot| log_slot.store(None));
    }
}
