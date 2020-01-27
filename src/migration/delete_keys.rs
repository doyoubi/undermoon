use super::task::{MigrationError, ScanResponse, SlotRangeArray};
use crate::common::cluster::SlotRange;
use crate::common::config::AtomicMigrationConfig;
use crate::common::db::HostDBMap;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::keep_connecting_and_sending;
use crate::protocol::{RedisClient, RedisClientError, RedisClientFactory, Resp};
use atomic_option::AtomicOption;
use futures01::{future, Future};
use itertools::Itertools;
use std::collections::HashMap;
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

pub struct DeleteKeysTask {
    address: String,
    slot_ranges: SlotRangeArray,
    _handle: FutureAutoStopHandle, // once this task get dropped, the future will stop.
    fut: AtomicOption<Box<dyn Future<Item = (), Error = MigrationError> + Send>>,
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

    pub fn start(&self) -> Option<Box<dyn Future<Item = (), Error = MigrationError> + Send>> {
        self.fut.take(Ordering::SeqCst).map(|t| *t)
    }

    fn gen_future<F: RedisClientFactory>(
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        delete_rate: u64,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
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
        (Box::new(send.map_err(|_| MigrationError::Canceled)), handle)
    }

    fn scan_and_delete_keys<C: RedisClient>(
        data: (SlotRangeArray, u64),
        client: C,
    ) -> Box<dyn Future<Item = ((SlotRangeArray, u64), C), Error = RedisClientError> + Send> {
        let (slot_ranges, index) = data;
        let scan_cmd = vec!["SCAN".to_string(), index.to_string()];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();
        let exec_fut = client
            .execute(byte_cmd)
            .and_then(move |(client, resp)| {
                future::result(
                    ScanResponse::parse_scan(resp).ok_or_else(|| RedisClientError::InvalidReply),
                )
                .and_then(move |scan| {
                    let ScanResponse { next_index, keys } = scan;
                    let keys: Vec<Vec<u8>> = keys
                        .into_iter()
                        .filter(|k| !slot_ranges.is_key_inside(k.as_slice()))
                        .collect();

                    let fut: Box<
                        dyn Future<Item = (SlotRangeArray, u64, C), Error = RedisClientError>
                            + Send,
                    > = if keys.is_empty() {
                        Box::new(future::ok((slot_ranges, next_index, client)))
                    } else {
                        let mut del_cmd = vec!["DEL".to_string().into_bytes()];
                        del_cmd.extend_from_slice(keys.as_slice());
                        Box::new(
                            client
                                .execute(del_cmd)
                                .and_then(|(client, resp)| {
                                    let r = match resp {
                                        Resp::Error(err) => {
                                            error!("failed to delete keys: {:?}", err);
                                            Err(RedisClientError::InvalidReply)
                                        }
                                        _ => Ok(client),
                                    };
                                    future::result(r)
                                })
                                .map(move |client| (slot_ranges, next_index, client)),
                        )
                    };
                    fut
                })
            })
            .and_then(|(slot_ranges, next_index, client)| {
                if next_index == 0 {
                    future::err(RedisClientError::Done)
                } else {
                    future::ok(((slot_ranges, next_index), client))
                }
            });
        Box::new(exec_fut)
    }

    pub fn get_address(&self) -> String {
        self.address.clone()
    }
}
