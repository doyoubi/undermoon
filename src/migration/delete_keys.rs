use super::task::{ScanResponse, SlotRangeArray};
use crate::common::cluster::SlotRange;
use crate::common::config::AtomicMigrationConfig;
use crate::common::db::HostDBMap;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::keep_connecting_and_sending;
use crate::migration::task::MigrationError;
use crate::protocol::{RedisClient, RedisClientError, RedisClientFactory, Resp};
use atomic_option::AtomicOption;
use futures::{Future, FutureExt};
use itertools::Itertools;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub struct DeleteKeysTaskMap {
    task_map: HashMap<String, HashMap<String, Arc<DeleteKeysTask>>>,
}

impl DeleteKeysTaskMap {
    pub fn new() -> Self {
        Self {
            task_map: HashMap::new(),
        }
    }

    pub fn info(&self) -> String {
        let tasks: Vec<String> = self
            .task_map
            .iter()
            .map(|(db, nodes)| {
                nodes
                    .iter()
                    .map(|(address, task)| {
                        format!("{}-{}-({})", db, address, task.slot_ranges.info())
                    })
                    .join(",")
            })
            .collect();
        format!("deleting_tasks:{}", tasks.join(","))
    }

    pub fn update_from_old_task_map<F: RedisClientFactory>(
        &self,
        local_db_map: &HostDBMap,
        left_slots_after_change: HashMap<String, HashMap<String, Vec<SlotRange>>>,
        config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<F>,
    ) -> (Self, Vec<Arc<DeleteKeysTask>>) {
        let mut new_task_map = HashMap::new();
        let mut new_tasks = Vec::new();

        // Copy old tasks
        for (dbname, nodes) in self.task_map.iter() {
            let new_nodes = match local_db_map.get_map().get(dbname) {
                Some(nodes) => nodes,
                None => continue,
            };
            for (address, task) in nodes.iter() {
                if new_nodes.get(address).is_none() {
                    continue;
                }
                let db = new_task_map
                    .entry(dbname.clone())
                    .or_insert_with(HashMap::new);
                db.insert(address.clone(), task.clone());
            }
        }

        // Add new tasks
        for (dbname, nodes) in left_slots_after_change.into_iter() {
            for (address, slots) in nodes.into_iter() {
                let db = new_task_map
                    .entry(dbname.clone())
                    .or_insert_with(HashMap::new);
                let task = Arc::new(DeleteKeysTask::new(
                    address.clone(),
                    slots,
                    client_factory.clone(),
                    config.get_delete_rate(),
                ));
                db.insert(address, task.clone());
                new_tasks.push(task);
            }
        }

        (
            Self {
                task_map: new_task_map,
            },
            new_tasks,
        )
    }
}

type ScanDelResult = Result<(SlotRangeArray, u64), RedisClientError>;
type ScanDelFuture = Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send>>;
type MigrationResult = Result<(), MigrationError>;

pub struct DeleteKeysTask {
    address: String,
    slot_ranges: SlotRangeArray,
    _handle: FutureAutoStopHandle, // once this task get dropped, the future will stop.
    fut: AtomicOption<Pin<Box<dyn Future<Output = MigrationResult> + Send>>>,
}

impl DeleteKeysTask {
    fn new<F: RedisClientFactory>(
        address: String,
        slot_ranges: Vec<SlotRange>,
        client_factory: Arc<F>,
        delete_rate: u64,
    ) -> Self {
        let slot_ranges = slot_ranges
            .into_iter()
            .map(|range| (range.start, range.end))
            .collect();
        let slot_ranges = SlotRangeArray {
            ranges: slot_ranges,
        };
        let (fut, handle) = Self::gen_future(
            address.clone(),
            slot_ranges.clone(),
            client_factory,
            delete_rate,
        );
        Self {
            address,
            slot_ranges,
            _handle: handle,
            fut: AtomicOption::new(Box::new(fut)),
        }
    }

    pub fn start(&self) -> Option<Pin<Box<dyn Future<Output = MigrationResult> + Send>>> {
        self.fut.take(Ordering::SeqCst).map(|t| *t)
    }

    fn gen_future<F: RedisClientFactory>(
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        delete_rate: u64,
    ) -> (ScanDelFuture, FutureAutoStopHandle) {
        let data = (slot_ranges, 0);
        const SCAN_DEFAULT_SIZE: u64 = 10;
        let interval = Duration::from_nanos(1_000_000_000 / (delete_rate / SCAN_DEFAULT_SIZE));
        info!("delete keys with interval {:?}", interval);
        let send = keep_connecting_and_sending(
            data,
            client_factory,
            address,
            interval,
            Self::scan_and_delete_keys,
        );
        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(|opt| match opt {
            Some(_) => Ok(()),
            None => Err(MigrationError::Canceled),
        });
        (Box::pin(send), handle)
    }

    async fn scan_and_delete_keys_impl<C: RedisClient>(
        data: (SlotRangeArray, u64),
        client: &mut C,
    ) -> Result<(SlotRangeArray, u64), RedisClientError> {
        let (slot_ranges, index) = data;
        let scan_cmd = vec!["SCAN".to_string(), index.to_string()];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();

        let resp = client.execute_single(byte_cmd).await?;
        let ScanResponse { next_index, keys } =
            ScanResponse::parse_scan(resp).ok_or_else(|| RedisClientError::InvalidReply)?;

        let keys: Vec<Vec<u8>> = keys
            .into_iter()
            .filter(|k| !slot_ranges.is_key_inside(k.as_slice()))
            .collect();

        if keys.is_empty() {
            return Ok((slot_ranges, next_index));
        }

        let mut del_cmd = vec!["DEL".to_string().into_bytes()];
        del_cmd.extend_from_slice(keys.as_slice());
        let resp = client.execute_single(del_cmd).await?;

        match resp {
            Resp::Error(err) => {
                error!("failed to delete keys: {:?}", err);
                Err(RedisClientError::InvalidReply)
            }
            _ => Ok((slot_ranges, next_index)),
        }
    }

    fn scan_and_delete_keys<C: RedisClient>(
        data: (SlotRangeArray, u64),
        client: &mut C,
    ) -> Pin<Box<dyn Future<Output = ScanDelResult> + Send + '_>> {
        Box::pin(Self::scan_and_delete_keys_impl(data, client))
    }

    pub fn get_address(&self) -> String {
        self.address.clone()
    }
}
