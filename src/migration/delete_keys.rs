use super::task::{ImportingPostTask, MigratingPostTask, MigrationError};
use common::cluster::SlotRange;
use common::future_group::{new_auto_drop_future, FutureAutoStopHandle};
use common::resp_execution::keep_connecting_and_sending;
use common::utils::{get_resp_bytes, ThreadSafe};
use futures::{future, Future};
use protocol::{
    Array, BinSafeStr, BulkStr, RedisClient, RedisClientError, RedisClientFactory, Resp,
};
use std::str;
use std::sync::Arc;
use std::time::Duration;

pub struct DeleteKeysTask<F: RedisClientFactory> {
    address: String,
    slot_range: SlotRange,
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> DeleteKeysTask<F> {
    fn new(address: String, slot_range: SlotRange, client_factory: Arc<F>) -> Self {
        Self {
            address,
            slot_range,
            client_factory,
        }
    }

    fn start_deleting(
        &self,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        let data = (self.slot_range.start, self.slot_range.end, 0);
        let interval = Duration::from_nanos(100);
        let send = keep_connecting_and_sending(
            data,
            self.client_factory.clone(),
            self.address.clone(),
            interval,
            Self::scan_and_delete_keys,
        );
        let (send, handle) = new_auto_drop_future(send);
        (Box::new(send.map_err(|_| MigrationError::Canceled)), handle)
    }

    fn scan_and_delete_keys(
        data: (usize, usize, u64),
        client: F::Client,
    ) -> Box<dyn Future<Item = ((usize, usize, u64), F::Client), Error = RedisClientError> + Send>
    {
        let (start, end, index) = data;
        let scan_cmd = vec!["SCAN".to_string(), index.to_string()];
        let byte_cmd = scan_cmd.into_iter().map(|s| s.into_bytes()).collect();
        let exec_fut = client
            .execute(byte_cmd)
            .and_then(move |(client, resp)| {
                future::result(parse_scan(resp).ok_or_else(|| RedisClientError::InvalidReply))
                    .and_then(move |scan| {
                        let ScanResponse { next_index, keys } = scan;
                        let mut del_cmd = vec!["DEL".to_string().into_bytes()];
                        del_cmd.extend_from_slice(keys.as_slice());
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
                            .map(move |client| (next_index, client))
                    })
            })
            .map(move |(next_index, client)| ((start, end, next_index), client));
        Box::new(exec_fut)
    }
}

impl<F: RedisClientFactory> ThreadSafe for DeleteKeysTask<F> {}

impl<F: RedisClientFactory> MigratingPostTask for DeleteKeysTask<F> {
    fn start(
        &self,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        self.start_deleting()
    }
}

impl<F: RedisClientFactory> ImportingPostTask for DeleteKeysTask<F> {
    fn start(
        &self,
    ) -> (
        Box<dyn Future<Item = (), Error = MigrationError> + Send>,
        FutureAutoStopHandle,
    ) {
        self.start_deleting()
    }
}

struct ScanResponse {
    next_index: u64,
    keys: Vec<BinSafeStr>,
}

fn parse_scan(resp: Resp) -> Option<ScanResponse> {
    match resp {
        Resp::Arr(Array::Arr(ref resps)) => {
            let index_data = resps.get(0).and_then(|resp| match resp {
                Resp::Bulk(BulkStr::Str(ref s)) => Some(s.clone()),
                Resp::Simple(ref s) => Some(s.clone()),
                _ => None,
            })?;
            let next_index = str::from_utf8(index_data.as_slice()).ok()?.parse().ok()?;
            let keys = get_resp_bytes(resps.get(1)?)?;
            Some(ScanResponse { next_index, keys })
        }
        _ => None,
    }
}
