use super::broker::MetaDataBroker;
use super::core::{CoordinateError, HostMeta, HostMetaRetriever, HostMetaSender};
use common::cluster::{Host, Role, SlotRange};
use common::db::{DBMapFlags, HostDBMap, ProxyDBMeta};
use common::utils::OLD_EPOCH_REPLY;
use futures::{future, Future};
use protocol::{RedisClient, RedisClientFactory, Resp};
use replication::replicator::{encode_repl_meta, MasterMeta, ReplicaMeta, ReplicatorMeta};
use std::collections::HashMap;
use std::sync::Arc;

pub struct HostMetaRespSender<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> HostMetaRespSender<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }
}

impl<F: RedisClientFactory> HostMetaSender for HostMetaRespSender<F> {
    fn send_meta(
        &self,
        host: HostMeta,
    ) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let HostMeta { local, peer } = host;
        let address = local.get_address().clone();

        let client_fut = self
            .client_factory
            .create_client(address)
            .map_err(CoordinateError::Redis);

        let local_host_with_only_masters = filter_host_masters(local.clone());
        Box::new(
            client_fut
                .and_then(move |client| {
                    // SETREPL should be sent before SETDB to avoid sending to replica while handling slots.
                    send_meta(
                        client,
                        "SETREPL".to_string(),
                        generate_repl_meta_cmd_args(local, DBMapFlags { force: false }),
                    )
                })
                .and_then(|client| {
                    send_meta(
                        client,
                        "SETDB".to_string(),
                        generate_host_meta_cmd_args(
                            DBMapFlags { force: false },
                            local_host_with_only_masters,
                            peer,
                        ),
                    )
                })
                .map(|_| ()),
        )
    }
}

fn filter_host_masters(host: Host) -> Host {
    let address = host.get_address().clone();
    let epoch = host.get_epoch();
    let masters = host
        .into_nodes()
        .into_iter()
        .filter(|node| node.get_role() == Role::Master)
        .collect();

    Host::new(address, epoch, masters)
}

pub struct BrokerMetaRetriever<B: MetaDataBroker> {
    broker: Arc<B>,
}

impl<B: MetaDataBroker> BrokerMetaRetriever<B> {
    pub fn new(broker: Arc<B>) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> HostMetaRetriever for BrokerMetaRetriever<B> {
    fn get_host_meta(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<HostMeta>, Error = CoordinateError> + Send> {
        // We should get local first but send the peer first for consistency for migration,
        // so that we have local.epoch <= peer.epoch,
        // which means if proxy received a SETDB clearing the migrated slot range,
        // there should have been a SETPEER
        // with higher or the same epoch including the migrated out slot range.
        let get_local = self
            .broker
            .get_host(address.clone())
            .map_err(CoordinateError::MetaData);
        let get_peer = self
            .broker
            .get_peer(address)
            .map_err(CoordinateError::MetaData);
        Box::new(get_local.and_then(|l| {
            get_peer.map(move |p| {
                if let (Some(local), Some(peer)) = (l, p) {
                    // If requesting two read operation from different stateless broker proxy,
                    // we might still have a stale SETDB.
                    // Check this explicitly.
                    if local.get_epoch() == peer.get_epoch() {
                        return Some(HostMeta { local, peer });
                    }
                    debug!("local.epoch > peer.epoch. Drop it.")
                }
                None
            })
        }))
    }
}

fn generate_host_meta_cmd_args(
    flags: DBMapFlags,
    local_proxy: Host,
    peer_proxy: Host,
) -> Vec<String> {
    let epoch = local_proxy.get_epoch();
    let local = generate_proxy_db_map(local_proxy);
    let peer = generate_proxy_db_map(peer_proxy);
    let proxy_db_meta = ProxyDBMeta::new(epoch, flags.clone(), local, peer);
    proxy_db_meta.to_args()
}

fn generate_proxy_db_map(proxy: Host) -> HostDBMap {
    let mut db_map: HashMap<String, HashMap<String, Vec<SlotRange>>> = HashMap::new();
    for node in proxy.into_nodes() {
        let dbs = db_map
            .entry(node.get_cluster_name().clone())
            .or_insert_with(HashMap::new);
        dbs.insert(node.get_address().clone(), node.into_slots().clone());
    }
    HostDBMap::new(db_map)
}

// sub_command should be SETDB or SETPEER
fn send_meta<C: RedisClient>(
    client: C,
    sub_command: String,
    args: Vec<String>,
) -> impl Future<Item = C, Error = CoordinateError> + Send + 'static {
    debug!("sending meta {} {:?}", sub_command, args);
    let mut cmd = vec!["UMCTL".to_string(), sub_command.clone()];
    cmd.extend(args);
    client
        .execute(cmd.into_iter().map(String::into_bytes).collect())
        .map_err(|e| {
            error!("failed to send meta data of host {:?}", e);
            CoordinateError::Redis(e)
        })
        .and_then(move |(client, resp)| match resp {
            Resp::Error(err_str) => {
                if err_str == OLD_EPOCH_REPLY.as_bytes() {
                    future::ok(client)
                } else {
                    error!("failed to send meta, invalid reply {:?}", err_str);
                    future::err(CoordinateError::InvalidReply)
                }
            }
            reply => {
                debug!("Successfully set meta {} {:?}", sub_command, reply);
                future::ok(client)
            }
        })
}

fn generate_repl_meta_cmd_args(host: Host, flags: DBMapFlags) -> Vec<String> {
    let epoch = host.get_epoch();

    let mut masters = Vec::new();
    let mut replicas = Vec::new();

    for node in host.into_nodes().into_iter() {
        let role = node.get_role();
        let meta = node.get_repl_meta();
        let db_name = node.get_cluster_name().clone();
        match role {
            Role::Master => {
                // For importing nodes, the role is controlled by the migration progress.
                if node.get_slots().iter().any(|sr| sr.tag.is_importing()) {
                    continue;
                }

                let master_node_address = node.get_address().clone();
                let replicas = meta.get_peers().clone();
                let master_meta = MasterMeta {
                    db_name,
                    master_node_address,
                    replicas,
                };
                masters.push(master_meta);
            }
            Role::Replica => {
                let replica_node_address = node.get_address().clone();
                let masters = meta.get_peers().clone();
                let replica_meta = ReplicaMeta {
                    db_name,
                    replica_node_address,
                    masters,
                };
                replicas.push(replica_meta);
            }
        }
    }

    let repl_meta = ReplicatorMeta {
        epoch,
        flags,
        masters,
        replicas,
    };

    encode_repl_meta(repl_meta)
}
