use super::task::{ScanResponse, SlotRangeArray};
use crate::common::cluster::{ClusterName, RangeList, SlotRange};
use crate::common::config::AtomicMigrationConfig;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::proto::ProxyClusterMap;
use crate::common::resp_execution::keep_connecting_and_sending;
use crate::migration::task::MigrationError;
use crate::protocol::{
    Array, BulkStr, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec,
};
use atomic_option::AtomicOption;
use futures::{Future, FutureExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct DeleteKeysTaskMap {
    task_map: HashMap<ClusterName, HashMap<String, Arc<DeleteKeysTask>>>,
}

impl DeleteKeysTaskMap {
    pub fn empty() -> Self {
        Self {
            task_map: HashMap::new(),
        }
    }

    pub fn contains_running_tasks(&self) -> bool {
        for cluster_tasks in self.task_map.values() {
            for task in cluster_tasks.values() {
                if !task.is_finished() {
                    return true;
                }
            }
        }
        false
    }

    pub fn info(&self) -> RespVec {
        let tasks: Vec<RespVec> = self
            .task_map
            .iter()
            .map(|(cluster, nodes)| {
                let lines = nodes
                    .iter()
                    .map(|(address, task)| {
                        format!(
                            "{}-{}-({})-({})",
                            cluster,
                            address,
                            task.slot_ranges.info(),
                            task.is_finished()
                        )
                    })
                    .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
                    .collect::<Vec<_>>();
                Resp::Arr(Array::Arr(lines))
            })
            .collect();
        Resp::Arr(Array::Arr(tasks))
    }

    pub fn update_from_old_task_map<F: RedisClientFactory>(
        &self,
        local_cluster_map: &ProxyClusterMap,
        left_slots_after_change: HashMap<ClusterName, HashMap<String, Vec<SlotRange>>>,
        config: Arc<AtomicMigrationConfig>,
        client_factory: Arc<F>,
    ) -> (Self, Vec<Arc<DeleteKeysTask>>) {
        let mut new_task_map = HashMap::new();
        let mut new_tasks = Vec::new();

        // Copy old tasks
        for (cluster_name, nodes) in self.task_map.iter() {
            let new_nodes = match local_cluster_map.get_map().get(cluster_name) {
                Some(nodes) => nodes,
                None => continue,
            };
            for (address, task) in nodes.iter() {
                // Clear finished task at the same time.
                if new_nodes.get(address).is_none() || task.is_finished() {
                    continue;
                }
                let cluster = new_task_map
                    .entry(cluster_name.clone())
                    .or_insert_with(HashMap::new);
                cluster.insert(address.clone(), task.clone());
            }
        }

        // Add new tasks
        for (cluster_name, nodes) in left_slots_after_change.into_iter() {
            for (address, slots) in nodes.into_iter() {
                let cluster = new_task_map
                    .entry(cluster_name.clone())
                    .or_insert_with(HashMap::new);
                let task = Arc::new(DeleteKeysTask::new(
                    address.clone(),
                    slots,
                    client_factory.clone(),
                    config.clone(),
                ));
                cluster.insert(address, task.clone());
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
    finished: Arc<AtomicBool>,
}

impl DeleteKeysTask {
    fn new<F: RedisClientFactory>(
        address: String,
        slot_ranges: Vec<SlotRange>,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
    ) -> Self {
        let slot_ranges = slot_ranges
            .into_iter()
            .map(|range| range.to_range_list())
            .collect();
        let range_list = RangeList::merge(slot_ranges);
        let slot_ranges = SlotRangeArray::new(range_list);
        let finished = Arc::new(AtomicBool::new(false));
        let (fut, handle) = Self::gen_future(
            address.clone(),
            slot_ranges.clone(),
            client_factory,
            config,
            finished.clone(),
        );
        Self {
            address,
            slot_ranges,
            _handle: handle,
            fut: AtomicOption::new(Box::new(fut)),
            finished,
        }
    }

    pub fn start(&self) -> Option<Pin<Box<dyn Future<Output = MigrationResult> + Send>>> {
        self.fut.take(Ordering::SeqCst).map(|t| *t)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    fn gen_future<F: RedisClientFactory>(
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
        finished: Arc<AtomicBool>,
    ) -> (ScanDelFuture, FutureAutoStopHandle) {
        let data = (slot_ranges, 0);
        let scan_count = config.get_delete_count();
        let interval = Duration::from_micros(config.get_delete_interval());
        info!("delete keys with interval {:?}", interval);
        let send = keep_connecting_and_sending(
            data,
            client_factory,
            address,
            interval,
            move |data, client| Self::scan_and_delete_keys(data, client, scan_count),
        );
        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(move |opt| {
            finished.store(true, Ordering::SeqCst);
            match opt {
                Some(_) => {
                    info!("Deleting key task finished successfully");
                    Ok(())
                }
                None => {
                    warn!("Deleting key task is canceled");
                    Err(MigrationError::Canceled)
                }
            }
        });
        (Box::pin(send), handle)
    }

    async fn scan_and_delete_keys_impl<C: RedisClient>(
        data: (SlotRangeArray, u64),
        client: &mut C,
        scan_count: u64,
    ) -> Result<(SlotRangeArray, u64), RedisClientError> {
        let (slot_ranges, index) = data;
        let scan_cmd = vec![
            "SCAN".to_string(),
            index.to_string(),
            "COUNT".to_string(),
            scan_count.to_string(),
        ];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();

        let resp = client.execute_single(byte_cmd).await?;
        let ScanResponse { next_index, keys } =
            ScanResponse::parse_scan(resp).ok_or_else(|| RedisClientError::InvalidReply)?;

        let keys: Vec<Vec<u8>> = keys
            .into_iter()
            .filter(|k| !slot_ranges.is_key_inside(k.as_slice()))
            .collect();

        if keys.is_empty() {
            if next_index == 0 {
                return Err(RedisClientError::Done);
            }
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
            _ if next_index == 0 => Err(RedisClientError::Done),
            _ => Ok((slot_ranges, next_index)),
        }
    }

    fn scan_and_delete_keys<C: RedisClient>(
        data: (SlotRangeArray, u64),
        client: &mut C,
        scan_count: u64,
    ) -> Pin<Box<dyn Future<Output = ScanDelResult> + Send + '_>> {
        Box::pin(Self::scan_and_delete_keys_impl(data, client, scan_count))
    }

    pub fn get_address(&self) -> String {
        self.address.clone()
    }

    pub fn get_slot_ranges(&self) -> SlotRangeArray {
        self.slot_ranges.clone()
    }
}
