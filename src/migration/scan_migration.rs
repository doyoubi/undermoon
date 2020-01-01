use super::task::{MigrationError, ScanResponse, SlotRangeArray};
use atomic_option::AtomicOption;
use common::cluster::SlotRange;
use common::config::AtomicMigrationConfig;
use common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use common::resp_execution::{
    keep_connecting_and_sending, keep_connecting_and_sending_cmd_with_cached_client,
};
use futures::sync::{mpsc, oneshot};
use futures::Sink;
use futures::{future, Future};
use futures::{stream, Stream};
use protocol::{
    Array, BinSafeStr, BulkStr, RedisClient, RedisClientError, RedisClientFactory, Resp,
};
use std::collections::HashMap;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

const DATA_QUEUE_SIZE: usize = 4096;

#[derive(Clone)]
struct DataEntry {
    key: Vec<u8>,
    pttl: Vec<u8>,
    raw_data: Vec<u8>,
}

type MgrFut = Box<dyn Future<Item = (), Error = MigrationError> + Send>;

pub struct ScanMigrationTask {
    src_address: String,
    dst_address: String,
    slot_ranges: SlotRangeArray,
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
        let (producer_fut, producer_handle) = Self::gen_producer(
            data_sender.clone(),
            stop_sender,
            src_address.clone(),
            slot_ranges.clone(),
            client_factory.clone(),
            scan_rate,
        );
        let (consumer_fut, consumer_handle) = Self::gen_consumer(
            data_sender,
            stop_receiver,
            data_receiver,
            dst_address.clone(),
            client_factory,
        );
        Self {
            src_address,
            dst_address,
            slot_ranges,
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
        receiver: mpsc::Receiver<DataEntry>,
        address: String,
        client_factory: Arc<F>,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        let interval = Duration::from_nanos(0);
        let send = Self::forward_entries(sender, receiver, stop_receiver, address, client_factory);
        let (send, handle) = new_auto_drop_future(send);
        (Box::new(send.map_err(|_| MigrationError::Canceled)), handle)
    }

    fn forward_entries<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        receiver: mpsc::Receiver<DataEntry>,
        stop_receiver: oneshot::Receiver<()>,
        address: String,
        client_factory: Arc<F>,
    ) -> impl Future<Item = (), Error = RedisClientError> {
        let forward = receiver
            .map_err(|()| RedisClientError::Canceled)
            .fold((sender, None), move |(sender, client_opt), entry| {
                let DataEntry {
                    key,
                    pttl,
                    raw_data,
                } = entry.clone();
                let client_factory2 = client_factory.clone();
                let sender = sender.clone();
                let sender2 = sender.clone();
                let restore_cmd = vec!["RESTORE".to_string().into_bytes(), key, pttl, raw_data];
                let interval = Duration::from_nanos(0);
                keep_connecting_and_sending_cmd_with_cached_client(
                    client_opt,
                    client_factory2,
                    address.clone(),
                    restore_cmd,
                    interval,
                    Self::handle_forward,
                )
                .map(move |client_opt| (sender, client_opt))
                .or_else(move |_err| {
                    // If it fails, retry this entry.
                    sender2
                        .send(entry)
                        .map(|sender| (sender, None))
                        .map_err(|_err| RedisClientError::Canceled)
                })
            })
            .map(|_| ());
        forward
            .select(stop_receiver.map_err(|_| RedisClientError::Canceled))
            .map(|_| ())
            .map_err(|(err, _)| err)
    }

    fn handle_forward(resp: Resp) -> Result<(), RedisClientError> {
        match resp {
            Resp::Error(_) => Err(RedisClientError::InvalidReply),
            _ => Err(RedisClientError::Done),
        }
    }

    fn gen_producer<F: RedisClientFactory>(
        sender: mpsc::Sender<DataEntry>,
        stop_sender: oneshot::Sender<()>,
        address: String,
        slot_ranges: SlotRangeArray,
        client_factory: Arc<F>,
        scan_rate: u64,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        let data = (slot_ranges, 0, sender);
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

    fn scan_and_migrate_keys<C: RedisClient>(
        data: (SlotRangeArray, u64, mpsc::Sender<DataEntry>),
        client: C,
    ) -> Box<
        dyn Future<
                Item = ((SlotRangeArray, u64, mpsc::Sender<DataEntry>), C),
                Error = RedisClientError,
            > + Send,
    > {
        let (slot_ranges, index, sender) = data;
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

                    let slot_ranges_clone = slot_ranges.clone();
                    Self::produce_entries(slot_ranges, keys, client).and_then(
                        move |(slot_ranges, client, entries)| {
                            sender
                                .send_all(stream::iter_ok(entries))
                                .map(move |(sender, _entries)| {
                                    (slot_ranges, next_index, client, sender)
                                })
                                .map_err(|_err| RedisClientError::Canceled)
                        },
                    )
                })
            })
            .and_then(|(slot_ranges, next_index, client, sender)| {
                if next_index == 0 {
                    future::err(RedisClientError::Done)
                } else {
                    future::ok(((slot_ranges, next_index, sender), client))
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
                        Resp::Integer(pttl) => Ok((client, pttl)),
                        others => {
                            error!("failed to get PTTL: {:?}", others);
                            Err(RedisClientError::InvalidReply)
                        }
                    };
                    future::result(r)
                })
                .and_then(move |(client, pttl)| {
                    let dump_cmd = vec!["DUMP".to_string().into_bytes(), key.clone()];
                    client.execute(dump_cmd).and_then(move |(client, resp)| {
                        let r = match resp {
                            Resp::Bulk(BulkStr::Str(raw_data)) => Ok((
                                client,
                                DataEntry {
                                    key,
                                    pttl,
                                    raw_data,
                                },
                            )),
                            others => {
                                error!("failed to dump data: {:?}", others);
                                Err(RedisClientError::InvalidReply)
                            }
                        };
                        future::result(r)
                    })
                })
                .map(move |(client, entry)| {
                    entries.push(entry);
                    (client, entries)
                })
        })
        .map(|(client, entries)| (slot_ranges, client, entries))
    }
}
