use crate::protocol::{PooledRedisClientFactory, RedisClient, RedisClientFactory, Resp};
use futures::future;
use std::cmp::max;
use std::time::Duration;

pub struct EpochFetchResult {
    pub largest_epoch: u64,
    pub failed_addresses: Vec<String>,
}

pub async fn fetch_largest_epoch(proxy_addresses: Vec<String>) -> EpochFetchResult {
    let timeout = Duration::from_secs(1);
    let client_factory = PooledRedisClientFactory::new(1, timeout);

    let futs: Vec<_> = proxy_addresses
        .into_iter()
        .map(|address| fetch_proxy_epoch(address, &client_factory))
        .collect();
    let results = future::join_all(futs).await;

    let mut failed_addresses = vec![];
    let mut largest_epoch = 0;
    for res in results.into_iter() {
        match res {
            Ok(epoch) => {
                largest_epoch = max(largest_epoch, epoch);
            }
            Err(address) => {
                failed_addresses.push(address);
            }
        }
    }
    EpochFetchResult {
        largest_epoch,
        failed_addresses,
    }
}

async fn fetch_proxy_epoch(
    address: String,
    client_factory: &PooledRedisClientFactory,
) -> Result<u64, String> {
    let mut client = client_factory
        .create_client(address.clone())
        .await
        .map_err(|err| {
            error!(
                "Failed to create client for broker recovery: {} {}",
                address, err
            );
            address.clone()
        })?;

    let cmd = vec![b"UMCTL".to_vec(), b"GETEPOCH".to_vec()];
    let resp = client.execute_single(cmd).await.map_err(|err| {
        error!("Failed to send UMCTL GETEPOCH: {} {}", address, err);
        address.clone()
    })?;

    match resp {
        Resp::Integer(int_bytes) => match btoi::btoi::<u64>(&int_bytes) {
            Ok(epoch) => Ok(epoch),
            Err(_) => {
                error!(
                    "Invalid UMCTL GETEPOCH int reply: {} {:?}",
                    address, int_bytes
                );
                Err(address.clone())
            }
        },
        other => {
            error!("Invalid UMCTL GETEPOCH reply: {} {:?}", address, other);
            Err(address.clone())
        }
    }
}
