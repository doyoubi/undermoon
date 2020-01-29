use super::task::{ScanResponse, SlotRangeArray};
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::{
    keep_connecting_and_sending, keep_connecting_and_sending_cmd_with_cached_client,
};
use crate::common::utils::pretty_print_bytes;
use crate::migration::task::MigrationError;
use crate::protocol::{BulkStr, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec};
use atomic_option::AtomicOption;
use futures::channel::{mpsc, oneshot};
use futures::{select, stream, Future, FutureExt, SinkExt, StreamExt, TryFutureExt};
use futures_timer::Delay;
use std::pin::Pin;
use std::str;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const PEXPIRE_NO_EXPIRE: &str = "-1";
const RESTORE_NO_EXPIRE: &str = "0";
const BUSYKEY_ERROR: &str = "BUSYKEY";
const DATA_QUEUE_SIZE: usize = 4096;

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
        slot_range: (usize, usize),
        client_factory: Arc<F>,
        scan_rate: u64,
    ) -> Self {
        let slot_ranges = SlotRangeArray {
            ranges: vec![slot_range],
        };
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
            scan_rate,
        );
        let (consumer_fut, consumer_handle) = Self::gen_consumer(
            data_sender,
            stop_receiver,
            counter,
            data_receiver,
            dst_address,
            client_factory,
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
    ) -> (MgrFut, FutureAutoStopHandle) {
        let send = Self::forward_entries(
            sender,
            receiver,
            stop_receiver,
            counter,
            address,
            client_factory,
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
    ) -> Result<(), RedisClientError> {
        let counter_clone = counter.clone();

        let forward = async move {
            let _sender = sender;
            let mut client_opt = None;
            let mut receiver = receiver;

            while let Some(entry) = receiver.next().await {
                let DataEntry {
                    key,
                    pttl,
                    raw_data,
                } = entry.clone();

                let expire_time = if pttl == PEXPIRE_NO_EXPIRE.as_bytes() {
                    RESTORE_NO_EXPIRE.as_bytes().into()
                } else {
                    pttl
                };

                let restore_cmd = vec![
                    "RESTORE".to_string().into_bytes(),
                    key,
                    expire_time,
                    raw_data,
                ];
                let interval = Duration::from_nanos(0);

                let client = keep_connecting_and_sending_cmd_with_cached_client(
                    client_opt.take(),
                    client_factory.clone(),
                    address.clone(),
                    restore_cmd,
                    interval,
                    Self::handle_forward,
                )
                .await;

                client_opt = Some(client);
                counter_clone.fetch_sub(1, Ordering::SeqCst);
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

    fn handle_forward(resp: RespVec) -> Result<(), RedisClientError> {
        match resp {
            Resp::Error(err_msg) => {
                if &err_msg[..BUSYKEY_ERROR.as_bytes().len()] == BUSYKEY_ERROR.as_bytes() {
                    Err(RedisClientError::Done)
                } else {
                    error!("RESTORE error: {:?}", pretty_print_bytes(&err_msg));
                    Err(RedisClientError::InvalidReply)
                }
            }
            _ => Err(RedisClientError::Done),
        }
    }

    fn gen_producer<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        stop_sender: oneshot::Sender<()>,
        counter: Arc<AtomicI64>,
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        scan_rate: u64,
    ) -> (MgrFut, FutureAutoStopHandle) {
        let data = (slot_ranges, 0, sender, counter);
        const SCAN_DEFAULT_SIZE: u64 = 10;
        let interval = Duration::from_nanos(1_000_000_000 / (scan_rate / SCAN_DEFAULT_SIZE));
        info!("scan and migrate keys with interval {:?}", interval);

        // When scan_and_migrate_keys fails, it will retry from the last scanning index.
        // So we won't lose data here.
        let send = keep_connecting_and_sending(
            data,
            client_factory,
            address,
            interval,
            Self::scan_and_migrate_keys,
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
    ) -> Result<(SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>), RedisClientError>
    {
        let (slot_ranges, index, mut sender, counter) = data;
        let scan_cmd = vec!["SCAN".to_string(), index.to_string()];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();

        let resp = client.execute(byte_cmd).await?;
        let ScanResponse { next_index, keys } =
            ScanResponse::parse_scan(resp).ok_or_else(|| RedisClientError::InvalidReply)?;

        let (slot_ranges, entries) = Self::produce_entries(slot_ranges, keys, client).await?;

        let entries_num = entries.len() as i64;
        sender
            .send_all(&mut stream::iter(entries.into_iter().map(Ok)))
            .map_err(|_err| RedisClientError::Canceled)
            .await?;

        counter.fetch_add(entries_num, Ordering::SeqCst);
        Ok((slot_ranges, next_index, sender, counter))
    }

    // TODO: fix this
    #[allow(clippy::type_complexity)]
    fn scan_and_migrate_keys<C: RedisClient>(
        data: (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
        client: &mut C,
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
        Box::pin(Self::scan_and_migrate_keys_impl(data, client))
    }

    async fn produce_entries<C: RedisClient>(
        slot_ranges: SlotRangeArray,
        keys: Vec<Vec<u8>>,
        client: &mut C,
    ) -> Result<(SlotRangeArray, Vec<DataEntry>), RedisClientError> {
        let mut entries = vec![];
        for key in keys {
            if !slot_ranges.is_key_inside(key.as_slice()) {
                continue;
            }

            let pttl_cmd = vec!["PTTL".to_string().into_bytes(), key.clone()];
            let resp = client.execute(pttl_cmd).await?;
            let pttl_opt = match resp {
                // -2 for key not eixsts
                Resp::Integer(pttl) if pttl == b"-2" => None,
                Resp::Integer(pttl) => Some(pttl),
                others => {
                    error!("failed to get PTTL: {:?}", others);
                    return Err(RedisClientError::InvalidReply);
                }
            };

            let dump_cmd = vec!["DUMP".to_string().into_bytes(), key.clone()];
            let resp = client.execute(dump_cmd).await?;
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
