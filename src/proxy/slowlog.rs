use super::command::Command;
use super::service::ServerProxyConfig;
use crate::protocol::{Array, BulkStr, Resp, RespVec};
use arc_swap::ArcSwapOption;
use arr_macro::arr;
use chrono::{naive, DateTime, Utc};
use std::str;
use std::sync::atomic;
use std::sync::Arc;

// try letting the element and postfix fit into 128 bytes.
const MAX_ELEMENT_LENGTH: usize = 100;

#[repr(u8)]
#[derive(Debug)]
pub enum TaskEvent {
    Created = 0,

    SentToMigrationDB = 1,
    SentToDB = 2,

    SentToWritingQueue = 3,
    WritingQueueReceived = 4,
    SentToBackend = 5,
    ReceivedFromBackend = 6,
    WaitDone = 7,
}

const EVENT_NUMBER: usize = 8;
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

    fn get_used_time(&self, event: TaskEvent) -> i64 {
        let t = self.get_event_time(event);
        if t == 0 {
            0
        } else {
            let created_time = self.get_event_time(TaskEvent::Created);
            t - created_time
        }
    }
}

impl Default for RequestEventMap {
    fn default() -> Self {
        Self {
            events: arr![atomic::AtomicI64::new(0); 8],
        }
    }
}

#[derive(Debug)]
pub struct Slowlog {
    event_map: RequestEventMap,
    command: Vec<String>,
    session_id: usize,
}

impl Slowlog {
    pub fn from_command(command: &Command, session_id: usize) -> Self {
        let command = Self::get_brief_command(command);
        Slowlog {
            event_map: RequestEventMap::default(),
            command,
            session_id,
        }
    }

    fn get_brief_command(command: &Command) -> Vec<String> {
        let resp_slice = command.get_resp_slice();
        let resps = match resp_slice {
            Resp::Arr(Array::Arr(ref resps)) => resps,
            others => return vec![format!("{:?}", others)],
        };
        resps
            .iter()
            .take(LOG_ELEMENT_NUMBER)
            .map(|element| match element {
                Resp::Bulk(BulkStr::Str(data)) => match str::from_utf8(&data) {
                    Ok(s) => s.to_string(),
                    _ => format!("{:?}", data),
                },
                others => format!("{:?}", others),
            })
            .map(|mut s| {
                let real_len = s.len();
                s.truncate(MAX_ELEMENT_LENGTH);
                if real_len > MAX_ELEMENT_LENGTH {
                    let postfix = format!("({}bytes)", real_len);
                    s.push_str(&postfix)
                }
                s
            })
            .collect()
    }

    pub fn log_event(&self, event: TaskEvent) {
        self.event_map
            .set_event_time(event, Utc::now().timestamp_nanos())
    }

    pub fn get_session_id(&self) -> usize {
        self.session_id
    }
}

pub struct SlowRequestLogger {
    slowlogs: Vec<ArcSwapOption<Slowlog>>,
    curr_index: atomic::AtomicUsize,
    config: Arc<ServerProxyConfig>,
}

impl SlowRequestLogger {
    pub fn new(config: Arc<ServerProxyConfig>) -> Self {
        let mut slowlogs = Vec::new();
        while slowlogs.len() != config.slowlog_len {
            slowlogs.push(ArcSwapOption::new(None));
        }
        Self {
            slowlogs,
            curr_index: atomic::AtomicUsize::new(0),
            config,
        }
    }

    pub fn add_slow_log(&self, log: Arc<Slowlog>) {
        let dt = log.event_map.get_used_time(TaskEvent::WaitDone);
        let threshold = self
            .config
            .slowlog_log_slower_than
            .load(atomic::Ordering::SeqCst);
        // ms to ns
        if dt > threshold * 1000 {
            self.add(log);
        }
    }

    pub fn add(&self, log: Arc<Slowlog>) {
        let index = self.curr_index.fetch_add(1, atomic::Ordering::SeqCst) % self.slowlogs.len();
        if let Some(log_slot) = self.slowlogs.get(index) {
            log_slot.store(Some(log))
        }
    }

    pub fn get(&self, limit: Option<usize>) -> Vec<Arc<Slowlog>> {
        let num = limit.unwrap_or_else(|| self.slowlogs.len());
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

pub fn slowlogs_to_resp(logs: Vec<Arc<Slowlog>>) -> RespVec {
    let elements = logs
        .into_iter()
        .map(|log| slowlog_to_report(&(*log)))
        .collect();
    Resp::Arr(Array::Arr(elements))
}

fn slowlog_to_report(log: &Slowlog) -> RespVec {
    let start = log.event_map.get_event_time(TaskEvent::Created);
    let start_date = match naive::NaiveDateTime::from_timestamp_opt(
        start / 1_000_000_000,
        (start % 1_000_000_000) as u32,
    ) {
        Some(naive_datetime) => {
            let datetime = DateTime::<Utc>::from_utc(naive_datetime, Utc);
            datetime.to_rfc3339()
        }
        None => start.to_string(),
    };
    let elements = vec![
        format!("session_id: {}", log.session_id),
        format!("created: {}", start_date),
        format!(
            "sent_to_migration_db: {}",
            log.event_map.get_used_time(TaskEvent::SentToMigrationDB)
        ),
        format!(
            "sent_to_db: {}",
            log.event_map.get_used_time(TaskEvent::SentToDB)
        ),
        format!(
            "sent_to_queue: {}",
            log.event_map.get_used_time(TaskEvent::SentToWritingQueue)
        ),
        format!(
            "queue_received: {}",
            log.event_map.get_used_time(TaskEvent::WritingQueueReceived)
        ),
        format!(
            "sent_to_backend: {}",
            log.event_map.get_used_time(TaskEvent::SentToBackend)
        ),
        format!(
            "received_from_backend: {}",
            log.event_map.get_used_time(TaskEvent::ReceivedFromBackend)
        ),
        format!(
            "wait_done: {}",
            log.event_map.get_used_time(TaskEvent::WaitDone)
        ),
        format!("command: {}", log.command.join(" ")),
    ];
    Resp::Arr(Array::Arr(
        elements
            .into_iter()
            .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
            .collect(),
    ))
}
