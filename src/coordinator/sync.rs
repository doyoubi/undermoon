use std::collections::HashMap;
use futures::{Future, future};
use ::common::db::HostDBMap;
use ::common::cluster::{Host, SlotRange};
use protocol::{RedisClient, SimpleRedisClient, Resp};
use super::broker::{MetaDataBroker, MetaDataBrokerError};
use super::core::{HostMetaSender, HostMetaRetriever, CoordinateError};

pub struct HostMetaRespSender<C: RedisClient> {
    client: C
}

impl<C: RedisClient> HostMetaRespSender<C> {
    pub fn new(client: C) -> Self { Self{ client }}
}

impl<C: RedisClient> HostMetaSender for HostMetaRespSender<C> {
    fn send_meta(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        Box::new(
            send_meta(&self.client, host, "SETDB".to_string(), "NOFLAG".to_string())
        )
    }
}

pub struct PeerMetaRespSender<C: RedisClient> {
    client: C
}

impl<C: RedisClient> PeerMetaRespSender<C> {
    pub fn new(client: C) -> Self { Self{ client }}
}

impl<C: RedisClient> HostMetaSender for PeerMetaRespSender<C> {
    fn send_meta(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        Box::new(
            send_meta(&self.client, host, "SETPEER".to_string(), "NOFLAG".to_string())
        )
    }
}

pub struct LocalMetaRetriever<B: MetaDataBroker> {
    broker: B
}

impl<B: MetaDataBroker> LocalMetaRetriever<B> {
    pub fn new(broker: B) -> Self { Self{ broker }}
}

impl<B: MetaDataBroker> HostMetaRetriever for LocalMetaRetriever<B> {
    fn get_host_meta(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = CoordinateError> + Send> {
        Box::new(self.broker.get_host(address).map_err(CoordinateError::MetaData))
    }
}

pub struct PeerMetaRetriever<B: MetaDataBroker> {
    broker: B
}

impl<B: MetaDataBroker> PeerMetaRetriever<B> {
    pub fn new(broker: B) -> Self { Self{ broker }}
}

impl<B: MetaDataBroker> HostMetaRetriever for PeerMetaRetriever<B> {
    fn get_host_meta(&self, address: String) -> Box<dyn Future<Item = Option<Host>, Error = CoordinateError> + Send> {
        Box::new(self.broker.get_peer(address).map_err(CoordinateError::MetaData))
    }
}

// sub_command should be SETDB or SETPEER
fn send_meta<C: RedisClient>(client: &C, host: Host, sub_command: String, flag: String) -> impl Future<Item = (), Error = CoordinateError> + Send {
    let address = host.get_address().clone();
    let epoch = host.get_epoch();
    let mut db_map: HashMap<String, HashMap<String, Vec<SlotRange>>> = HashMap::new();
    for node in host.get_nodes() {
        let dbs = db_map.entry(node.get_cluster_name().clone()).or_insert(HashMap::new());
        dbs.insert(node.get_address().clone(), node.get_slots().clone());
    }
    let args = HostDBMap::new(epoch, db_map).db_map_to_args();
    let mut cmd = vec![
        "UMCTL".to_string(), "SETDB".to_string(), epoch.to_string(), "NOFLAG".to_string(),
    ];
    cmd.extend(args.into_iter());
    debug!("sending meta {:?}", cmd);
    client.execute(address, cmd.into_iter().map(|s| s.into_bytes()).collect())
        .map_err(|e| {
            println!("Failed to send meta data of host {:?}", e);
            CoordinateError::Redis(e)
        })
        .and_then(|resp| {
            error!("failed to send meta, invalid reply {:?}", resp);
            match resp {
                Resp::Error(err_str) => {
                    future::err(CoordinateError::InvalidReply)
                },
                _ => future::ok(()),
            }
        })
}
