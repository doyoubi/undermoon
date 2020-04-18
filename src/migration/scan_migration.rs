use super::task::{ScanResponse, SlotRangeArray};
use crate::common::cluster::SlotRange;
use crate::common::config::AtomicMigrationConfig;
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::{
    keep_connecting_and_sending, keep_connecting_and_sending_cmd_with_cached_client,
};
use crate::common::utils::pretty_print_bytes;
use crate::migration::task::MigrationError;
use crate::protocol::{
    BulkStr, OptionalMulti, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec,
};
use atomic_option::AtomicOption;
use btoi;
use futures::channel::{mpsc, oneshot};
use futures::{select, stream, Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use futures_batch::ChunksTimeoutStreamExt;
use futures_timer::Delay;
use std::cmp::min;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const PTTL_NO_EXPIRE: &[u8] = b"-1";
pub const PTTL_KEY_NOT_FOUND: &[u8] = b"-2";
pub const RESTORE_NO_EXPIRE: &[u8] = b"0";
const BUSYKEY_ERROR: &[u8] = b"BUSYKEY";
const DATA_QUEUE_SIZE: usize = 4096;

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

pub struct ScanMigrationTask {
    handle: AtomicOption<(FutureAutoStopHandle, FutureAutoStopHandle)>, // once this task get dropped, the future will stop.
    fut: AtomicOption<(MgrFut, MgrFut)>,
}

impl ScanMigrationTask {
    pub fn new<F: RedisClientFactory>(
        src_address: String,
        dst_address: String,
        slot_range: SlotRange,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
    ) -> Self {
        let ranges = slot_range.to_range_list();
        let slot_ranges = SlotRangeArray::new(ranges);
        let (data_sender, data_receiver) = mpsc::channel(DATA_QUEUE_SIZE);
        let (stop_sender, stop_receiver) = oneshot::channel();
        let counter = Arc::new(AtomicI64::new(0));
        let (producer_fut, producer_handle) = Self::gen_producer(
            data_sender.clone(),
            stop_sender,
            counter.clone(),
            src_address,
            slot_ranges,
            client_factory.clone(),
            config.clone(),
        );

        let (consumer_fut, consumer_handle) = Self::gen_consumer(
            data_sender,
            stop_receiver,
            counter,
            data_receiver,
            dst_address,
            client_factory,
            config,
        );
        Self {
            handle: AtomicOption::new(Box::new((producer_handle, consumer_handle))),
            fut: AtomicOption::new(Box::new((producer_fut, consumer_fut))),
        }
    }

    pub fn start(&self) -> Option<(MgrFut, MgrFut)> {
        self.fut.take(Ordering::SeqCst).map(|t| *t)
    }

    pub fn stop(&self) -> bool {
        self.handle.take(Ordering::SeqCst).is_some()
    }

    fn gen_consumer<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        stop_receiver: oneshot::Receiver<()>,
        counter: Arc<AtomicI64>,
        receiver: mpsc::Receiver<DataEntry>,
        address: String,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
    ) -> (MgrFut, FutureAutoStopHandle) {
        let scan_count = config.get_scan_count();
        let send = Self::forward_entries(
            sender,
            receiver,
            stop_receiver,
            counter,
            address,
            client_factory,
            scan_count,
        );
        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(|opt| {
            opt.map_or(Err(MigrationError::Canceled), |r| {
                r.map_err(MigrationError::RedisClient)
            })
        });

        (Box::pin(send), handle)
    }

    async fn forward_entries<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        receiver: mpsc::Receiver<DataEntry>,
        stop_receiver: oneshot::Receiver<()>,
        counter: Arc<AtomicI64>,
        address: String,
        client_factory: Arc<F>,
        scan_count: u64,
    ) -> Result<(), RedisClientError> {
        let counter_clone = counter.clone();

        let wait_timeout = Duration::from_millis(10);

        let forward = async move {
            let _sender = sender;
            let mut client_opt = None;
            let mut receiver = receiver.chunks_timeout(scan_count as usize, wait_timeout);

            while let Some(entries) = receiver.next().await {
                let key_num = entries.len();
                let mut commands = Vec::with_capacity(scan_count as usize);
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

                let interval = Duration::from_millis(1);
                let client = keep_connecting_and_sending_cmd_with_cached_client(
                    client_opt.take(),
                    client_factory.clone(),
                    address.clone(),
                    OptionalMulti::Multi(commands),
                    interval,
                    Self::handle_forward,
                )
                .await;

                client_opt = Some(client);
                counter_clone.fetch_sub(key_num as i64, Ordering::SeqCst);
            }
        };

        let stop = async move {
            if let Err(err) = stop_receiver.await {
                debug!("failed to receive stop signal: {:?}", err);
            }
            loop {
                if 0 == counter.load(Ordering::SeqCst) {
                    break;
                }
                let interval = Duration::from_millis(100);
                Delay::new(interval).await;
            }
        };

        select! {
            _ = forward.fuse() => Err(RedisClientError::Canceled),
            _ = stop.fuse() => Ok(()),
        }
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

    fn gen_producer<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        stop_sender: oneshot::Sender<()>,
        counter: Arc<AtomicI64>,
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        config: Arc<AtomicMigrationConfig>,
    ) -> (MgrFut, FutureAutoStopHandle) {
        let data = (slot_ranges, 0, sender, counter);
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
        let send = keep_connecting_and_sending(
            data,
            client_factory,
            address,
            interval,
            move |data, client| Self::scan_and_migrate_keys(data, client, scan_count),
        );
        let (send, handle) = new_auto_drop_future(send);
        let send = send.map(move |opt| {
            if let Err(err) = stop_sender.send(()) {
                debug!("failed to send stop signal: {:?}", err);
            }
            match opt {
                Some(_) => Ok(()),
                None => Err(MigrationError::Canceled),
            }
        });
        (Box::pin(send), handle)
    }

    async fn scan_and_migrate_keys_impl<C: RedisClient>(
        data: (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
        client: &mut C,
        scan_count: u64,
    ) -> Result<(SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>), RedisClientError>
    {
        let (slot_ranges, index, mut sender, counter) = data;
        let scan_cmd = vec![
            "SCAN".to_string(),
            index.to_string(),
            "COUNT".to_string(),
            scan_count.to_string(),
        ];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();

        let resp = client.execute_single(byte_cmd).await?;
        let ScanResponse { next_index, keys } =
            ScanResponse::parse_scan(&resp).ok_or_else(|| {
                error!("Invalid scan reply: {:?}", resp);
                RedisClientError::InvalidReply
            })?;

        let (slot_ranges, entries) = Self::produce_entries(slot_ranges, keys, client).await?;

        let entries_num = entries.len() as i64;
        sender
            .send_all(&mut stream::iter(entries.into_iter().map(Ok)))
            .map_err(|_err| RedisClientError::Canceled)
            .await?;

        counter.fetch_add(entries_num, Ordering::SeqCst);

        if next_index == 0 {
            Err(RedisClientError::Done)
        } else {
            Ok((slot_ranges, next_index, sender, counter))
        }
    }

    // TODO: fix this
    #[allow(clippy::type_complexity)]
    fn scan_and_migrate_keys<C: RedisClient>(
        data: (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
        client: &mut C,
        scan_count: u64,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
                        RedisClientError,
                    >,
                > + Send
                + '_,
        >,
    > {
        Box::pin(Self::scan_and_migrate_keys_impl(data, client, scan_count))
    }

    async fn produce_entries<C: RedisClient>(
        slot_ranges: SlotRangeArray,
        keys: Vec<Vec<u8>>,
        client: &mut C,
    ) -> Result<(SlotRangeArray, Vec<DataEntry>), RedisClientError> {
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

        Ok((slot_ranges, entries))
    }
}

impl Drop for ScanMigrationTask {
    fn drop(&mut self) {
        self.stop();
    }
}
