use std::collections::HashMap;
use futures::{Future, future};
use ::common::db::HostDBMap;
use ::common::cluster::{Host, SlotRange};
use protocol::{RedisClient, SimpleRedisClient, Resp};
use super::core::{HostMetaSender, CoordinateError};

pub struct HostMetaRespSender<C: RedisClient> {
    client: C
}

impl<C: RedisClient> HostMetaRespSender<C> {
    fn new(client: C) -> Self { Self{ client }}
}

impl<C: RedisClient> HostMetaSender for HostMetaRespSender<C> {
    fn send_meta(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let address = host.get_address().clone();
        let epoch = host.get_epoch();
        let mut db_map: HashMap<String, HashMap<String, Vec<SlotRange>>> = HashMap::new();
        for node in host.get_nodes() {
            let dbs = db_map.entry(node.get_cluster_name().clone()).or_insert(HashMap::new());
            dbs.insert(node.get_address().clone(), node.get_slots().clone());
        }
        let args = HostDBMap::new(epoch, db_map).db_map_to_args();
        let mut cmd = vec![
            "NMCTL".to_string(), "SETDB".to_string(), epoch.to_string(), "NOFLAG".to_string(),
        ];
        cmd.extend(args.into_iter());
        let f = self.client.execute(address, cmd.into_iter().map(|s| s.into_bytes()).collect())
            .map_err(|e| {
                println!("Failed to send meta data of host {:?}", e);
                CoordinateError::Redis(e)
            })
            .and_then(|resp| {
                match resp {
                    Resp::Error(err_str) => {
                        future::err(CoordinateError::InvalidReply)
                    },
                    _ => future::ok(()),
                }
            });
        Box::new(f)
    }
}
