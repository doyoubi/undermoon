use super::broker::MetaDataBroker;
use super::core::{CoordinateError, HostMetaRetriever, HostMetaSender};
use common::cluster::{Host, Role, SlotRange};
use common::db::{DBMapFlags, HostDBMap};
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
    fn send_meta(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let address = host.get_address().clone();
        let client_fut = self
            .client_factory
            .create_client(address)
            .map_err(CoordinateError::Redis);
        let host_with_only_masters = filter_host_masters(host.clone());
        Box::new(
            client_fut
                .and_then(|client| {
                    send_meta(
                        client,
                        "SETDB".to_string(),
                        generate_host_meta_cmd_args(
                            host_with_only_masters,
                            DBMapFlags { force: false },
                        ),
                    )
                    .and_then(move |client| {
                        send_meta(
                            client,
                            "SETREPL".to_string(),
                            generate_repl_meta_cmd_args(host, DBMapFlags { force: false }),
                        )
                    })
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

pub struct PeerMetaRespSender<F: RedisClientFactory> {
    client_factory: Arc<F>,
}

impl<F: RedisClientFactory> PeerMetaRespSender<F> {
    pub fn new(client_factory: Arc<F>) -> Self {
        Self { client_factory }
    }
}

impl<F: RedisClientFactory> HostMetaSender for PeerMetaRespSender<F> {
    fn send_meta(&self, host: Host) -> Box<dyn Future<Item = (), Error = CoordinateError> + Send> {
        let address = host.get_address().clone();
        let client_fut = self
            .client_factory
            .create_client(address)
            .map_err(CoordinateError::Redis);
        // TODO: Remove MIGRATING and IMPORTING tags for peers.
        Box::new(client_fut.and_then(|client| {
            send_meta(
                client,
                "SETPEER".to_string(),
                generate_host_meta_cmd_args(host, DBMapFlags { force: false }),
            )
            .map(|_| ())
        }))
    }
}

pub struct LocalMetaRetriever<B: MetaDataBroker> {
    broker: B,
}

impl<B: MetaDataBroker> LocalMetaRetriever<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> HostMetaRetriever for LocalMetaRetriever<B> {
    fn get_host_meta(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<Host>, Error = CoordinateError> + Send> {
        Box::new(
            self.broker
                .get_host(address)
                .map_err(CoordinateError::MetaData),
        )
    }
}

pub struct PeerMetaRetriever<B: MetaDataBroker> {
    broker: B,
}

impl<B: MetaDataBroker> PeerMetaRetriever<B> {
    pub fn new(broker: B) -> Self {
        Self { broker }
    }
}

impl<B: MetaDataBroker> HostMetaRetriever for PeerMetaRetriever<B> {
    fn get_host_meta(
        &self,
        address: String,
    ) -> Box<dyn Future<Item = Option<Host>, Error = CoordinateError> + Send> {
        Box::new(
            self.broker
                .get_peer(address)
                .map_err(CoordinateError::MetaData),
        )
    }
}

fn generate_host_meta_cmd_args(host: Host, flags: DBMapFlags) -> Vec<String> {
    let epoch = host.get_epoch();
    let mut db_map: HashMap<String, HashMap<String, Vec<SlotRange>>> = HashMap::new();
    for node in host.into_nodes() {
        let dbs = db_map
            .entry(node.get_cluster_name().clone())
            .or_insert_with(HashMap::new);
        dbs.insert(node.get_address().clone(), node.into_slots().clone());
    }
    let args = HostDBMap::new(epoch, flags.clone(), db_map).db_map_to_args();
    let mut cmd = vec![epoch.to_string(), flags.to_arg()];
    cmd.extend(args.into_iter());
    cmd
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
