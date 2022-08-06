use super::stats::MigrationStats;
use super::task::{ScanResponse, SlotRangeArray};
use crate::common::cluster::SlotRange;
use crate::common::config::AtomicMigrationConfig;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::keep_connecting_and_sending_cmd_with_cached_client;
use crate::common::response;
use crate::common::slot_lock::SlotMutex;
use crate::common::try_chunks::TryChunksStreamExt;
use crate::common::utils::{generate_lock_slot, pretty_print_bytes};
use crate::common::yield_now::YieldNow;
use crate::migration::task::MigrationError;
use crate::protocol::{
    BinSafeStr, BulkStr, OptionalMulti, Pool, RedisClient, RedisClientError, RedisClientFactory,
    Resp, RespVec,
};
use crate::proxy::backend::CmdTask;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{future, Future, FutureExt, StreamExt};
use parking_lot::Mutex;
use std::cmp::min;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const PTTL_NO_EXPIRE: &[u8] = b"-1";
pub const PTTL_KEY_NOT_FOUND: &[u8] = b"-2";
pub const RESTORE_NO_EXPIRE: &[u8] = b"0";
const BUSYKEY_ERROR: &[u8] = b"BUSYKEY";

pub fn pttl_to_restore_expire_time(pttl: Vec<u8>) -> Vec<u8> {
    let mut expire_time = pttl;
    if pttl_need_to_be_no_expire(&expire_time) {
        // Reuse this vector
        expire_time.clear();
        expire_time.extend_from_slice(RESTORE_NO_EXPIRE)
    }
    expire_time
}

fn pttl_need_to_be_no_expire(buf: &[u8]) -> bool {
    if buf == PTTL_NO_EXPIRE {
        return true;
    }

    let n = match btoi::btoi::<i64>(buf) {
        Ok(n) => n,
        Err(_) => return true, // invalid expire number
    };
    // -1 no expire
    // -2 key not found
    n < 0
}

#[derive(Clone)]
struct DataEntry {
    key: Vec<u8>,
    pttl: Vec<u8>,
    raw_data: Vec<u8>,
}

type MgrFut = Pin<Box<dyn Future<Output = Result<(), MigrationError>> + Send>>;

pub struct ScanMigrationTask<T: CmdTask, F: RedisClientFactory> {
    handle: Arc<Mutex<Option<FutureAutoStopHandle>>>, // once this task get dropped, the future will stop.
    fut: Arc<Mutex<Option<MgrFut>>>,
    sync_tasks_sender: UnboundedSender<T>,
    src_address: String,
    dst_address: String,
    client_factory: Arc<F>,
    slot_mutex: Arc<SlotMutex>,
    src_client_pool: Pool<F::Client>,
    dst_client_pool: Pool<F::Client>,
    stats: Arc<MigrationStats>,
    stats_conn_last_update_time: AtomicU64,
}

impl<T: CmdTask, F: RedisClientFactory> ScanMigrationTask<T, F> {
    pub fn new(
        src_address: String,
        dst_address: String,
        slot_range: SlotRange,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
        stats: Arc<MigrationStats>,
    ) -> Self {
        let ranges = slot_range.to_range_list();
        let slot_ranges = SlotRangeArray::new(ranges);
        let (sender, receiver) = unbounded();
        let slot_mutex = Arc::new(SlotMutex::default());
        let (fut, fut_handle) = Self::gen_future(
            src_address.clone(),
            dst_address.clone(),
            slot_ranges,
            client_factory.clone(),
            sender.clone(),
            receiver,
            config,
            slot_mutex.clone(),
            stats.clone(),
        );

        const POOL_SIZE: usize = 1024;

        Self {
            handle: Arc::new(Mutex::new(Some(fut_handle))),
            fut: Arc::new(Mutex::new(Some(fut))),
            sync_tasks_sender: sender,
            src_address,
            dst_address,
            client_factory,
            slot_mutex,
            src_client_pool: Pool::new(POOL_SIZE),
            dst_client_pool: Pool::new(POOL_SIZE),
            stats,
            stats_conn_last_update_time: AtomicU64::new(0),
        }
    }

    pub async fn handle_sync_task(&self, task: T) {
        let (key, lock_slot) = match task.get_key() {
            Some(key) => (key.to_vec(), generate_lock_slot(key)),
            None => {
                task.set_resp_result(Ok(Resp::Error(b"missing key".to_vec())));
                return;
            }
        };

        let guard = match self.slot_mutex.lock(lock_slot) {
            Some(guard) => guard,
            None => {
                self.stats
                    .migrating_active_sync_lock_failed
                    .fetch_add(1, Ordering::Relaxed);

                // The slot is locked. We put it into a slow path.
                if let Err(err) = self.sync_tasks_sender.unbounded_send(task) {
                    let task = err.into_inner();
                    task.set_resp_result(Ok(Resp::Simple(
                        response::MIGRATING_FINISHED.to_string().into_bytes(),
                    )));
                }
                return;
            }
        };
        self.stats
            .migrating_active_sync_lock_success
            .fetch_add(1, Ordering::Relaxed);

        let mut src_client = {
            match self.src_client_pool.get_raw() {
                Ok(Some(c)) => c,
                _ => match self
                    .client_factory
                    .create_client(self.src_address.clone())
                    .await
                {
                    Ok(c) => c,
                    Err(err) => {
                        task.set_resp_result(Ok(Resp::Error(
                            format!("failed to create src redis client: {:?}", err).into_bytes(),
                        )));
                        return;
                    }
                },
            }
        };

        let mut dst_client = {
            match self.dst_client_pool.get_raw() {
                Ok(Some(c)) => c,
                _ => match self
                    .client_factory
                    .create_client(self.dst_address.clone())
                    .await
                {
                    Ok(c) => c,
                    Err(err) => {
                        task.set_resp_result(Ok(Resp::Error(
                            format!("failed to create dst redis client: {:?}", err).into_bytes(),
                        )));
                        return;
                    }
                },
            }
        };

        let entries = match Self::produce_entries(vec![key.clone()], &mut src_client).await {
            Ok(entries) => entries,
            Err(err) => {
                task.set_resp_result(Ok(Resp::Error(
                    format!("failed to produce entries from src: {:?}", err).into_bytes(),
                )));
                return;
            }
        };

        if !entries.is_empty() {
            let transferred_keys: Vec<_> = entries.iter().map(|entry| entry.key.clone()).collect();
            dst_client = Self::forward_entries(
                self.dst_address.clone(),
                Some(dst_client),
                self.client_factory.clone(),
                entries,
            )
            .await;

            if let Err(err) = Self::delete_keys(&mut src_client, transferred_keys).await {
                task.set_resp_result(Ok(Resp::Error(
                    format!("failed to forward entries from dst: {:?}", err).into_bytes(),
                )));
                return;
            }
        }

        if self
            .src_client_pool
            .get_reclaim_sender()
            .send(src_client)
            .is_err()
        {
            warn!("src_client pool is full");
        }
        if self
            .dst_client_pool
            .get_reclaim_sender()
            .send(dst_client)
            .is_err()
        {
            warn!("dst_client pool is full");
        }

        task.set_resp_result(Ok(Resp::Simple(
            response::OK_REPLY.to_string().into_bytes(),
        )));
        drop(guard);

        // `pool.len()` may be expensive. Just update it every second.
        let last = self.stats_conn_last_update_time.load(Ordering::Relaxed);
        let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(t) => t.as_secs(),
            Err(err) => {
                error!(
                    "failed to get system time for updating for migration stats: {}",
                    err
                );
                return;
            }
        };
        const UPDATE_INTERVAL_SECS: u64 = 1;
        if now > last + UPDATE_INTERVAL_SECS {
            self.stats
                .migrating_src_redis_conn
                .store(self.src_client_pool.len(), Ordering::Relaxed);
            self.stats
                .migrating_dst_redis_conn
                .store(self.dst_client_pool.len(), Ordering::Relaxed);
            self.stats_conn_last_update_time
                .store(now, Ordering::Relaxed);
        }
    }

    pub fn start(&self) -> Option<MgrFut> {
        self.fut.lock().take()
    }

    pub fn stop(&self) -> bool {
        self.handle.lock().take().is_some()
    }

    fn handle_forward(opt_multi_resp: OptionalMulti<RespVec>) -> Result<(), RedisClientError> {
        let resps = match opt_multi_resp {
            OptionalMulti::Single(r) => {
                error!("unexpected single reply: {:?}", r);
                return Err(RedisClientError::InvalidReply);
            }
            OptionalMulti::Multi(v) => v,
        };
        for resp in resps.into_iter() {
            if let Resp::Error(err_msg) = resp {
                if err_msg.get(..BUSYKEY_ERROR.len()) != Some(BUSYKEY_ERROR) {
                    error!("RESTORE error: {:?}", pretty_print_bytes(&err_msg));
                    return Err(RedisClientError::InvalidReply);
                }
            }
        }
        Err(RedisClientError::Done)
    }

    #[allow(clippy::too_many_arguments)]
    fn gen_future(
        src_address: String,
        dst_address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        sync_tasks_sender: UnboundedSender<T>,
        sync_tasks_receiver: UnboundedReceiver<T>,
        config: Arc<AtomicMigrationConfig>,
        slot_mutex: Arc<SlotMutex>,
        stats: Arc<MigrationStats>,
    ) -> (MgrFut, FutureAutoStopHandle) {
        let interval = min(
            Duration::from_micros(config.get_scan_interval()),
            Duration::from_millis(10),
        );
        let scan_count = config.get_scan_count();
        info!(
            "scan and migrate keys with interval: {:?} count: {}",
            interval, scan_count
        );

        // When scan_and_migrate_keys fails, it will retry from the last scanning index.
        // So we won't lose data here.
        let send = Self::keep_migrating(
            src_address,
            dst_address,
            slot_ranges,
            client_factory,
            sync_tasks_sender,
            sync_tasks_receiver,
            config,
            slot_mutex,
            stats,
        );

        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(|opt| opt.map_or(Err(MigrationError::Canceled), |r| r));
        (Box::pin(send), handle)
    }

    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_arguments)]
    async fn keep_migrating(
        src_address: String,
        dst_address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        sync_tasks_sender: UnboundedSender<T>,
        mut sync_tasks_receiver: UnboundedReceiver<T>,
        config: Arc<AtomicMigrationConfig>,
        slot_mutex: Arc<SlotMutex>,
        stats: Arc<MigrationStats>,
    ) -> Result<(), MigrationError> {
        const SLEEP_BATCH_TIMES: u64 = 10;

        let interval = min(
            Duration::from_micros(config.get_scan_interval() * SLEEP_BATCH_TIMES),
            Duration::from_millis(10),
        );
        let scan_count = config.get_scan_count();
        info!(
            "scan and migrate keys with batched interval: {:?} count: {}",
            interval, scan_count
        );

        let chunk_size = match NonZeroUsize::new(scan_count as usize) {
            None => {
                error!("zero scan count");
                sync_tasks_sender.close_channel();
                while let Some(cmd_task) = sync_tasks_receiver.next().await {
                    cmd_task.set_resp_result(Ok(Resp::Simple(
                        response::MIGRATING_FINISHED.to_string().into_bytes(),
                    )));
                }
                return Err(MigrationError::InvalidConfig);
            }
            Some(chunk_size) => chunk_size,
        };
        let mut sync_tasks_receiver = sync_tasks_receiver.try_chunks(chunk_size);

        let mut scan_index = 0;
        let mut cached_dst_client = None;
        let mut sleep_count = 0;
        loop {
            let mut src_client = match client_factory.create_client(src_address.clone()).await {
                Ok(client) => client,
                Err(err) => {
                    error!("failed to create redis client: {:?}", err);
                    tokio::time::sleep(interval).await;
                    continue;
                }
            };
            loop {
                let sync_tasks = if sleep_count >= SLEEP_BATCH_TIMES {
                    sleep_count = 0;
                    if interval == Duration::from_secs(0) {
                        // Need yield so that we won't get stuck in the unit tests.
                        match future::select(sync_tasks_receiver.next(), YieldNow::default()).await
                        {
                            future::Either::Left((Some(cmd_tasks), _)) => Some(cmd_tasks),
                            _ => None,
                        }
                    } else {
                        match tokio::time::timeout(interval, sync_tasks_receiver.next()).await {
                            Ok(Some(cmd_tasks)) => Some(cmd_tasks),
                            _ => None,
                        }
                    }
                } else {
                    sleep_count += 1;
                    match future::select(sync_tasks_receiver.next(), future::ready(())).await {
                        future::Either::Left((Some(cmd_tasks), _)) => Some(cmd_tasks),
                        _ => None,
                    }
                };

                let res = match sync_tasks {
                    Some(cmd_tasks) => {
                        let res = Self::handle_blocking_requests(
                            &slot_ranges,
                            cached_dst_client.take(),
                            &mut src_client,
                            dst_address.clone(),
                            client_factory.clone(),
                            cmd_tasks,
                        )
                        .await;
                        match res {
                            Err(err) => {
                                error!("failed to handle blocking requests {:?}", err);
                                break;
                            }
                            Ok(dst_client) => {
                                cached_dst_client = dst_client;
                            }
                        }
                        continue;
                    }
                    None => {
                        Self::scan_and_migrate_keys(
                            &slot_ranges,
                            scan_index,
                            cached_dst_client.take(),
                            &mut src_client,
                            dst_address.clone(),
                            client_factory.clone(),
                            scan_count,
                            &slot_mutex,
                            &stats,
                        )
                        .await
                    }
                };

                match res {
                    Err(err) => {
                        error!("failed to scan and migrate {:?}", err);
                        break;
                    }
                    Ok((new_scan_index, scan_finished, dst_client)) => {
                        if scan_finished {
                            sync_tasks_sender.close_channel();
                            while let Some(cmd_tasks) = sync_tasks_receiver.next().await {
                                for cmd_task in cmd_tasks.into_iter() {
                                    cmd_task.set_resp_result(Ok(Resp::Simple(
                                        response::MIGRATING_FINISHED.to_string().into_bytes(),
                                    )));
                                }
                            }
                            return Ok(());
                        }
                        scan_index = new_scan_index;
                        cached_dst_client = dst_client;
                    }
                }
            }
            tokio::time::sleep(interval).await;
        }
    }

    // Returns (next_index, scan_finished, cached_client) on success
    #[allow(clippy::too_many_arguments)]
    async fn scan_and_migrate_keys(
        slot_ranges: &SlotRangeArray,
        index: u64,
        mut dst_client: Option<F::Client>,
        src_client: &mut F::Client,
        dst_address: String,
        client_factory: Arc<F>,
        scan_count: u64,
        slot_mutex: &SlotMutex,
        stats: &MigrationStats,
    ) -> Result<(u64, bool, Option<F::Client>), RedisClientError> {
        let ScanResponse { next_index, keys } =
            Self::scan_keys(src_client, index, scan_count).await?;

        let keys: Vec<_> = keys
            .into_iter()
            .filter(|key| slot_ranges.is_key_inside(key.as_slice()))
            .collect();

        let mut locks = vec![];
        let mut locked_keys = vec![];
        let mut locked_slots = HashSet::<usize>::default();
        for key in keys.iter() {
            let lock_slot = generate_lock_slot(key.as_slice());
            if let Some(lock) = slot_mutex.lock(lock_slot) {
                locks.push(lock);
                locked_keys.push(key.clone());
                locked_slots.insert(lock_slot);
            } else if locked_slots.contains(&lock_slot) {
                // Locked in this function call. Can also add this key.
                locked_keys.push(key.clone());
            }
        }
        let need_retry = locked_keys.len() < keys.len();
        stats
            .migrating_scan_lock_success
            .fetch_add(locked_keys.len(), Ordering::Relaxed);
        stats
            .migrating_scan_lock_failed
            .fetch_add(keys.len() - locked_keys.len(), Ordering::Relaxed);

        let entries = Self::produce_entries(locked_keys, src_client).await?;
        if !entries.is_empty() {
            let transferred_keys: Vec<_> = entries.iter().map(|entry| entry.key.clone()).collect();
            let dst_client_cache =
                Self::forward_entries(dst_address, dst_client, client_factory, entries).await;
            dst_client = Some(dst_client_cache);

            Self::delete_keys(src_client, transferred_keys).await?;
        }
        drop(locks);

        if need_retry {
            // Some keys are missed in this round.
            // Retry the last index again.
            Ok((index, false, dst_client))
        } else {
            Ok((next_index, next_index == 0, dst_client))
        }
    }

    async fn handle_blocking_requests(
        slot_ranges: &SlotRangeArray,
        dst_client: Option<F::Client>,
        src_client: &mut F::Client,
        dst_address: String,
        client_factory: Arc<F>,
        cmd_tasks: Vec<T>,
    ) -> Result<Option<F::Client>, RedisClientError> {
        let keys = cmd_tasks
            .iter()
            .filter_map(|t| t.get_key().map(|b| b.to_vec()))
            .filter(|key| slot_ranges.is_key_inside(key.as_slice()))
            .collect();

        let res = match Self::produce_entries(keys, src_client).await {
            Ok(entries) => {
                if entries.is_empty() {
                    Ok(dst_client)
                } else {
                    let transferred_keys: Vec<_> =
                        entries.iter().map(|entry| entry.key.clone()).collect();
                    let dst_client =
                        Self::forward_entries(dst_address, dst_client, client_factory, entries)
                            .await;

                    Self::delete_keys(src_client, transferred_keys)
                        .await
                        .map(move |()| Some(dst_client))
                }
            }
            Err(err) => Err(err),
        };

        let resp = if res.is_ok() {
            Resp::Simple(response::OK_REPLY.to_string().into_bytes())
        } else {
            Resp::Error(b"failed to delete keys".to_vec())
        };

        for cmd_task in cmd_tasks.into_iter() {
            cmd_task.set_resp_result(Ok(resp.clone()));
        }

        res
    }

    async fn scan_keys<C: RedisClient>(
        src_client: &mut C,
        index: u64,
        scan_count: u64,
    ) -> Result<ScanResponse, RedisClientError> {
        let scan_cmd = vec![
            "SCAN".to_string(),
            index.to_string(),
            "COUNT".to_string(),
            scan_count.to_string(),
        ];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();

        let resp = src_client.execute_single(byte_cmd).await?;
        ScanResponse::parse_scan(&resp).ok_or_else(|| {
            error!("Invalid scan reply: {:?}", resp);
            RedisClientError::InvalidReply
        })
    }

    async fn produce_entries<C: RedisClient>(
        keys: Vec<BinSafeStr>,
        client: &mut C,
    ) -> Result<Vec<DataEntry>, RedisClientError> {
        let key_num = keys.len();

        let mut commands = vec![];
        for key in &keys {
            let pttl_cmd = vec!["PTTL".to_string().into_bytes(), key.clone()];
            let dump_cmd = vec!["DUMP".to_string().into_bytes(), key.clone()];

            commands.push(pttl_cmd);
            commands.push(dump_cmd);
        }

        let resps = client.execute_multi(commands).await?;
        if resps.len() != 2 * key_num {
            error!(
                "mismatch batch result number, expected {}, found {}",
                2 * key_num,
                resps.len()
            );
            return Err(RedisClientError::InvalidReply);
        }

        let mut resp_iter = resps.into_iter();
        let mut entries = vec![];
        for key in keys.into_iter() {
            let resp = resp_iter.next().ok_or_else(|| {
                error!("invalid state, can't get resp");
                RedisClientError::InvalidState
            })?;

            let pttl_opt = match resp {
                // -2 for key not eixsts
                Resp::Integer(pttl) if pttl == PTTL_KEY_NOT_FOUND => None,
                Resp::Integer(pttl) => Some(pttl),
                others => {
                    error!("failed to get PTTL: {:?}", others);
                    return Err(RedisClientError::InvalidReply);
                }
            };

            let resp = resp_iter.next().ok_or_else(|| {
                error!("invalid state, can't get resp");
                RedisClientError::InvalidState
            })?;

            match (resp, pttl_opt) {
                // This is the most possible case.
                (Resp::Bulk(BulkStr::Str(raw_data)), Some(pttl)) => {
                    entries.push(DataEntry {
                        key,
                        pttl,
                        raw_data,
                    });
                }
                (Resp::Bulk(BulkStr::Nil), _) | (_, None) => (),
                (others, _pttl_opt) => {
                    error!("failed to dump data: {:?}", others);
                    return Err(RedisClientError::InvalidReply);
                }
            };
        }

        Ok(entries)
    }

    async fn forward_entries(
        dst_address: String,
        cached_dst_client: Option<F::Client>,
        client_factory: Arc<F>,
        entries: Vec<DataEntry>,
    ) -> F::Client {
        let mut commands = Vec::with_capacity(entries.len());
        for entry in entries.into_iter() {
            let DataEntry {
                key,
                pttl,
                raw_data,
            } = entry.clone();

            let expire_time = pttl_to_restore_expire_time(pttl);

            let restore_cmd = vec![
                "RESTORE".to_string().into_bytes(),
                key,
                expire_time,
                raw_data,
            ];

            commands.push(restore_cmd);
        }

        let retry_interval = Duration::from_millis(1);
        keep_connecting_and_sending_cmd_with_cached_client(
            cached_dst_client,
            client_factory,
            dst_address,
            OptionalMulti::Multi(commands),
            retry_interval,
            Self::handle_forward,
        )
        .await
    }

    async fn delete_keys<C: RedisClient>(
        client: &mut C,
        keys: Vec<BinSafeStr>,
    ) -> Result<(), RedisClientError> {
        let mut del_cmd = vec!["DEL".to_string().into_bytes()];
        del_cmd.extend_from_slice(keys.as_slice());
        let resp = client.execute_single(del_cmd).await?;

        match resp {
            Resp::Error(err) => {
                error!("failed to delete keys: {:?}", err);
                Err(RedisClientError::InvalidReply)
            }
            _ => Ok(()),
        }
    }
}

impl<T: CmdTask, F: RedisClientFactory> Drop for ScanMigrationTask<T, F> {
    fn drop(&mut self) {
        self.stop();
    }
}
