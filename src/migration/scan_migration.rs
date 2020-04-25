use super::task::{ScanResponse, SlotRangeArray};
use crate::common::cluster::SlotRange;
use crate::common::config::AtomicMigrationConfig;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::keep_connecting_and_sending_cmd_with_cached_client;
use crate::common::response;
use crate::common::try_chunks::TryChunksStreamExt;
use crate::common::utils::pretty_print_bytes;
use crate::migration::task::MigrationError;
use crate::protocol::{
    BinSafeStr, BulkStr, OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, Resp,
    RespVec,
};
use crate::proxy::backend::CmdTask;
use atomic_option::AtomicOption;
use btoi;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{future, Future, FutureExt, StreamExt};
use futures_timer::Delay;
use std::cmp::min;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

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

pub struct ScanMigrationTask<T: CmdTask> {
    handle: AtomicOption<FutureAutoStopHandle>, // once this task get dropped, the future will stop.
    fut: AtomicOption<MgrFut>,
    sync_tasks_sender: UnboundedSender<T>,
}

impl<T: CmdTask> ScanMigrationTask<T> {
    pub fn new<F: RedisClientFactory>(
        src_address: String,
        dst_address: String,
        slot_range: SlotRange,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
    ) -> Self {
        let ranges = slot_range.to_range_list();
        let slot_ranges = SlotRangeArray::new(ranges);
        let (sender, receiver) = unbounded();
        let (fut, fut_handle) = Self::gen_future(
            src_address,
            dst_address,
            slot_ranges,
            client_factory,
            receiver,
            config,
        );

        Self {
            handle: AtomicOption::new(Box::new(fut_handle)),
            fut: AtomicOption::new(Box::new(fut)),
            sync_tasks_sender: sender,
        }
    }

    pub fn handle_sync_task(&self, task: T) {
        if let Err(err) = self.sync_tasks_sender.unbounded_send(task) {
            let task = err.into_inner();
            task.set_resp_result(Ok(Resp::Simple(
                response::MIGRATING_FINISHED.to_string().into_bytes(),
            )));
        }
    }

    pub fn start(&self) -> Option<MgrFut> {
        self.fut.take(Ordering::SeqCst).map(|t| *t)
    }

    pub fn stop(&self) -> bool {
        self.handle.take(Ordering::SeqCst).is_some()
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

    fn gen_future<F: RedisClientFactory>(
        src_address: String,
        dst_address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        sync_tasks_receiver: UnboundedReceiver<T>,
        config: Arc<AtomicMigrationConfig>,
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
            sync_tasks_receiver,
            config,
        );

        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(|opt| {
            opt.map_or(Err(MigrationError::Canceled), |r| {
                r.map_err(MigrationError::RedisClient)
            })
        });
        (Box::pin(send), handle)
    }

    async fn keep_migrating<F: RedisClientFactory>(
        src_address: String,
        dst_address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        sync_tasks_receiver: UnboundedReceiver<T>,
        config: Arc<AtomicMigrationConfig>,
    ) -> Result<(), RedisClientError> {
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
                return Err(RedisClientError::InitError);
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
                    Delay::new(interval).await;
                    continue;
                }
            };
            loop {
                let cmd_tasks = if sleep_count >= SLEEP_BATCH_TIMES {
                    sleep_count = 0;
                    match future::select(sync_tasks_receiver.next(), Delay::new(interval)).await {
                        future::Either::Left((Some(cmd_tasks), _)) => Some(cmd_tasks),
                        _ => None,
                    }
                } else {
                    sleep_count += 1;
                    match future::select(sync_tasks_receiver.next(), future::ready(())).await {
                        future::Either::Left((Some(cmd_tasks), _)) => Some(cmd_tasks),
                        _ => None,
                    }
                };

                let res = Self::scan_and_migrate_keys(
                    &slot_ranges,
                    scan_index,
                    cached_dst_client.take(),
                    &mut src_client,
                    dst_address.clone(),
                    client_factory.clone(),
                    scan_count,
                    cmd_tasks,
                )
                .await;
                match res {
                    Err(err) => {
                        error!("failed to scan and migrate {:?}", err);
                        break;
                    }
                    Ok((new_scan_index, dst_client)) => {
                        if new_scan_index == 0 {
                            return Ok(());
                        }
                        scan_index = new_scan_index;
                        cached_dst_client = dst_client;
                    }
                }
            }
            Delay::new(interval).await;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn scan_and_migrate_keys<F: RedisClientFactory>(
        slot_ranges: &SlotRangeArray,
        index: u64,
        dst_client: Option<F::Client>,
        src_client: &mut F::Client,
        dst_address: String,
        client_factory: Arc<F>,
        scan_count: u64,
        sync_tasks: Option<Vec<T>>,
    ) -> Result<(u64, Option<F::Client>), RedisClientError> {
        let (next_index, keys) = match &sync_tasks {
            Some(tasks) => {
                let keys = tasks
                    .iter()
                    .filter_map(|t| t.get_key().map(|b| b.to_vec()))
                    .collect();
                (index, keys)
            }
            None => {
                let ScanResponse { next_index, keys } =
                    Self::scan_keys(src_client, index, scan_count).await?;
                (next_index, keys)
            }
        };

        let entries = Self::produce_entries(slot_ranges, keys, src_client).await?;
        let res = if entries.is_empty() {
            Ok(dst_client)
        } else {
            let transferred_keys: Vec<_> = entries.iter().map(|entry| entry.key.clone()).collect();
            let dst_client =
                Self::forward_entries(dst_address, dst_client, client_factory, entries).await;

            Self::delete_keys(src_client, transferred_keys)
                .await
                .map(move |()| Some(dst_client))
        };

        if let Some(cmd_tasks) = sync_tasks {
            let resp = if res.is_ok() {
                Resp::Simple(response::OK_REPLY.to_string().into_bytes())
            } else {
                Resp::Error(b"failed to delete keys".to_vec())
            };
            for cmd_task in cmd_tasks.into_iter() {
                cmd_task.set_resp_result(Ok(resp.clone()));
            }
        }

        res.map(move |dst_client| (next_index, dst_client))
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
        slot_ranges: &SlotRangeArray,
        keys: Vec<BinSafeStr>,
        client: &mut C,
    ) -> Result<Vec<DataEntry>, RedisClientError> {
        let keys: Vec<_> = keys
            .into_iter()
            .filter(|key| slot_ranges.is_key_inside(key.as_slice()))
            .collect();
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

    async fn forward_entries<F: RedisClientFactory>(
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

impl<T: CmdTask> Drop for ScanMigrationTask<T> {
    fn drop(&mut self) {
        self.stop();
    }
}
