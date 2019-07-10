use super::command::Command;
use arc_swap::ArcSwapOption;
use arr_macro::arr;
use chrono::{naive, DateTime, Utc};
use protocol::{Array, BulkStr, Resp};
use std::str;
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

    fn get_used_time(&self, event: TaskEvent) -> i64 {
        let t =  self.get_event_time(event);
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
            events: arr![atomic::AtomicI64::new(0); 6],
        }
    }
}

#[derive(Debug)]
pub struct Slowlog {
    event_map: RequestEventMap,
    command: Vec<String>,
}

impl Slowlog {
    pub fn from_command(command: &Command) -> Self {
        let command = Self::get_brief_command(command);
        Slowlog {
            event_map: RequestEventMap::default(),
            command,
        }
    }

    fn get_brief_command(command: &Command) -> Vec<String> {
        let resps = match command.get_resp() {
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

pub fn slowlogs_to_resp(logs: Vec<Arc<Slowlog>>) -> Resp {
    let elements = logs
        .into_iter()
        .map(|log| slowlog_to_report(&(*log)))
        .collect();
    Resp::Arr(Array::Arr(elements))
}

fn slowlog_to_report(log: &Slowlog) -> Resp {
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
        format!("created: {}", start_date),
        format!("send_to_queue: {}", log.event_map.get_used_time(TaskEvent::SentToWritingQueue)),
        format!("queue_received: {}", log.event_map.get_used_time(TaskEvent::WritingQueueReceived)),
        format!("sent_to_backend: {}", log.event_map.get_used_time(TaskEvent::SentToBackend)),
        format!("received_from_backend: {}", log.event_map.get_used_time(TaskEvent::ReceivedFromBackend)),
        format!("wait_done: {}", log.event_map.get_used_time(TaskEvent::WaitDone)),
        format!("command: {}", log.command.join(" ")),
    ];
    Resp::Arr(Array::Arr(elements.into_iter().map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes()))).collect()))
}
