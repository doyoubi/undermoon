use crate::protocol::{PooledRedisClientFactory, RedisClient, RedisClientFactory, Resp};
use futures::future;
use std::cmp::max;
use std::time::Duration;

pub struct EpochFetchResult {
    pub max_epoch: u64,
    pub failed_addresses: Vec<String>,
}

pub async fn fetch_max_epoch(proxy_addresses: Vec<String>) -> EpochFetchResult {
    let timeout = Duration::from_secs(1);
    let client_factory = PooledRedisClientFactory::new(1, timeout);

    let futs: Vec<_> = proxy_addresses
        .into_iter()
        .map(|address| fetch_proxy_epoch(address, &client_factory))
        .collect();
    let results = future::join_all(futs).await;

    let mut failed_addresses = vec![];
    let mut max_epoch = 0;
    for res in results.into_iter() {
        match res {
            Ok(epoch) => {
                max_epoch = max(max_epoch, epoch);
            }
            Err(address) => {
                failed_addresses.push(address);
            }
        }
    }
    EpochFetchResult {
        max_epoch,
        failed_addresses,
    }
}

const MAX_RETRY_TIMES: usize = 30;
const RETRY_INTERVAL: u64 = 1;

pub async fn wait_for_proxy_epoch(proxy_addresses: Vec<String>, epoch: u64) -> Result<(), String> {
    let timeout = Duration::from_secs(1);
    let client_factory = PooledRedisClientFactory::new(1, timeout);

    let mut i = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(RETRY_INTERVAL)).await;
        let min_epoch = match fetch_min_epoch(proxy_addresses.clone(), &client_factory).await {
            Ok(min_epoch) => min_epoch,
            Err(failed_address) => {
                if i >= MAX_RETRY_TIMES {
                    return Err(failed_address);
                }
                i += 1;
                continue;
            }
        };
        if min_epoch >= epoch {
            return Ok(());
        }
        info!("waiting for proxy epoch");
    }
}

async fn fetch_min_epoch(
    proxy_addresses: Vec<String>,
    client_factory: &PooledRedisClientFactory,
) -> Result<u64, String> {
    let futs: Vec<_> = proxy_addresses
        .into_iter()
        .map(|address| fetch_proxy_epoch(address, &client_factory))
        .collect();
    let results = future::join_all(futs).await;

    let mut epoch_list = vec![];
    for res in results.into_iter() {
        epoch_list.push(res?);
    }
    Ok(epoch_list.into_iter().min().unwrap_or(0))
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
