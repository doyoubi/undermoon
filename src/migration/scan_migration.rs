use super::task::{MigrationError, ScanResponse, SlotRangeArray};
use crate::common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use crate::common::resp_execution::{
    keep_connecting_and_sending, keep_connecting_and_sending_cmd_with_cached_client,
};
use crate::common::utils::pretty_print_bytes;
use crate::protocol::{BulkStr, RedisClient, RedisClientError, RedisClientFactory, Resp, RespVec};
use atomic_option::AtomicOption;
use futures01::sync::{mpsc, oneshot};
use futures01::Sink;
use futures01::{future, Future};
use futures01::{stream, Stream};
use futures_timer::Delay;
use std::iter;
use std::str;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use futures::TryFutureExt;

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

type MgrFut = Box<dyn Future<Item = (), Error = MigrationError> + Send>;

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
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        let send = Self::forward_entries(
            sender,
            receiver,
            stop_receiver,
            counter,
            address,
            client_factory,
        );
        let (send, handle) = new_auto_drop_future(send);
        (Box::new(send.map_err(|_| MigrationError::Canceled)), handle)
    }

    fn forward_entries<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        receiver: mpsc::Receiver<DataEntry>,
        stop_receiver: oneshot::Receiver<()>,
        counter: Arc<AtomicI64>,
        address: String,
        client_factory: Arc<F>,
    ) -> impl Future<Item = (), Error = RedisClientError> {
        let counter_clone = counter.clone();
        let forward = receiver
            .map_err(|()| RedisClientError::Canceled)
            .fold((sender, None), move |(sender, client_opt), entry| {
                let counter = counter_clone.clone();
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
                let client_factory2 = client_factory.clone();
                let sender2 = sender.clone();
                let restore_cmd = vec![
                    "RESTORE".to_string().into_bytes(),
                    key,
                    expire_time,
                    raw_data,
                ];
                let interval = Duration::from_nanos(0);

                // TODO: batch multiple entries to speed up migration.
                keep_connecting_and_sending_cmd_with_cached_client(
                    client_opt,
                    client_factory2,
                    address.clone(),
                    restore_cmd,
                    interval,
                    Self::handle_forward,
                ).compat()
                .map(move |client_opt| {
                    counter.fetch_sub(1, Ordering::SeqCst);
                    (sender, client_opt)
                })
                .or_else(move |_err| {
                    // If it fails, retry this entry.
                    sender2
                        .send(entry)
                        .map(|sender| (sender, None))
                        .map_err(|_err| RedisClientError::Canceled)
                })
            })
            .map(|_| ());

        let stop = stop_receiver.then(move |_| {
            let s = stream::iter_ok(iter::repeat(()));
            s.for_each(move |()| {
                let interval = Duration::from_millis(100);
                let fut: Box<dyn Future<Item = (), Error = ()> + Send> =
                    if counter.load(Ordering::SeqCst) == 0 {
                        Box::new(future::err(()))
                    } else {
                        Box::new(Delay::new(interval).map_err(|_| ()))
                    };
                fut
            })
            .then(|_| future::ok::<(), ()>(()))
        });

        forward
            .select(stop.map_err(|_| RedisClientError::Canceled))
            .map(|_| ())
            .map_err(|(err, _)| err)
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
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
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
        let send = send
            .then(move |_| stop_sender.send(()))
            .map_err(|_| MigrationError::Canceled);
        (Box::new(send), handle)
    }

    // TODO: fix this
    #[allow(clippy::type_complexity)]
    fn scan_and_migrate_keys<C: RedisClient>(
        data: (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
        client: C,
    ) -> Box<
        dyn Future<
                Item = (
                    (SlotRangeArray, u64, mpsc::Sender<DataEntry>, Arc<AtomicI64>),
                    C,
                ),
                Error = RedisClientError,
            > + Send,
    > {
        let (slot_ranges, index, sender, counter) = data;
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

                    Self::produce_entries(slot_ranges, keys, client).and_then(
                        move |(slot_ranges, client, entries)| {
                            let entries_num = entries.len() as i64;
                            sender
                                .send_all(stream::iter_ok(entries))
                                .map(move |(sender, _entries)| {
                                    counter.fetch_add(entries_num, Ordering::SeqCst);
                                    (slot_ranges, next_index, client, sender, counter)
                                })
                                .map_err(|_err| RedisClientError::Canceled)
                        },
                    )
                })
            })
            .and_then(|(slot_ranges, next_index, client, sender, counter)| {
                if next_index == 0 {
                    future::err(RedisClientError::Done)
                } else {
                    future::ok(((slot_ranges, next_index, sender, counter), client))
                }
            });
        Box::new(exec_fut)
    }

    fn produce_entries<C: RedisClient>(
        slot_ranges: SlotRangeArray,
        keys: Vec<Vec<u8>>,
        client: C,
    ) -> impl Future<Item = (SlotRangeArray, C, Vec<DataEntry>), Error = RedisClientError> {
        let keys_iter: Vec<Vec<u8>> = keys
            .into_iter()
            .filter(|k| !slot_ranges.is_key_inside(k.as_slice()))
            .collect();

        let entries = vec![];
        let s = stream::iter_ok(keys_iter);
        s.fold((client, entries), move |(client, mut entries), key| {
            let pttl_cmd = vec!["PTTL".to_string().into_bytes(), key.clone()];
            client
                .execute(pttl_cmd)
                .and_then(|(client, resp)| {
                    let r = match resp {
                        // -2 for key not eixsts
                        Resp::Integer(pttl) if pttl == b"-2" => Ok((client, None)),
                        Resp::Integer(pttl) => Ok((client, Some(pttl))),
                        others => {
                            error!("failed to get PTTL: {:?}", others);
                            Err(RedisClientError::InvalidReply)
                        }
                    };
                    future::result(r)
                })
                .and_then(move |(client, pttl_opt)| {
                    let dump_cmd = vec!["DUMP".to_string().into_bytes(), key.clone()];
                    client.execute(dump_cmd).and_then(move |(client, resp)| {
                        let r = match (resp, pttl_opt) {
                            // This is the most possible case.
                            (Resp::Bulk(BulkStr::Str(raw_data)), Some(pttl)) => Ok((
                                client,
                                Some(DataEntry {
                                    key,
                                    pttl,
                                    raw_data,
                                }),
                            )),
                            (Resp::Bulk(BulkStr::Nil), _) | (_, None) => Ok((client, None)),
                            (others, _pttl_opt) => {
                                error!("failed to dump data: {:?}", others);
                                Err(RedisClientError::InvalidReply)
                            }
                        };
                        future::result(r)
                    })
                })
                .map(move |(client, entry_opt)| {
                    if let Some(entry) = entry_opt {
                        entries.push(entry);
                    }
                    (client, entries)
                })
        })
        .map(|(client, entries)| (slot_ranges, client, entries))
    }
}
