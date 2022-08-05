use super::backend::{BackendError, CmdTask, IntoTask, SenderBackendError};
use super::sender::{CmdTaskSender, CmdTaskSenderFactory};
use super::service::ClusterNodesVersion;
use super::slot::SlotMap;
use crate::common::cluster::{ClusterName, RangeList, SlotRange, SlotRangeTag, EMPTY_CLUSTER_NAME};
use crate::common::config::ClusterConfig;
use crate::common::proto::ProxyClusterMeta;
use crate::common::utils::gen_moved;
use crate::migration::task::MigrationState;
use crate::protocol::{Array, BulkStr, Resp, RespVec};
use crc64::crc64;
use either::Either;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::iter::Iterator;

pub const DEFAULT_CLUSTER: &str = "admin";
const CLUSTER_NODES_CPORT: usize = 5299;

#[derive(Debug)]
pub enum ClusterMetaError {
    OldEpoch,
    TryAgain,
    NotMyMeta,
}

impl fmt::Display for ClusterMetaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ClusterMetaError {
    fn cause(&self) -> Option<&dyn Error> {
        None
    }
}

pub struct ClusterBackendMap<S: CmdTaskSender, P: CmdTaskSender>
where
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
{
    cluster_name: ClusterName,
    local_cluster: LocalCluster<S>,
    remote_cluster: RemoteCluster<P>,
}

impl<S: CmdTaskSender, P: CmdTaskSender> Default for ClusterBackendMap<S, P>
where
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
{
    fn default() -> Self {
        Self {
            cluster_name: EMPTY_CLUSTER_NAME.clone(),
            local_cluster: LocalCluster::default(),
            remote_cluster: RemoteCluster::default(),
        }
    }
}

impl<S: CmdTaskSender, P: CmdTaskSender> ClusterBackendMap<S, P>
where
    <S as CmdTaskSender>::Task: IntoTask<<P as CmdTaskSender>::Task>,
{
    pub fn from_cluster_map<
        F: CmdTaskSenderFactory<Sender = S>,
        PF: CmdTaskSenderFactory<Sender = P>,
    >(
        cluster_meta: &ProxyClusterMeta,
        sender_factory: &F,
        peer_sender_factory: &PF,
        active_redirection: bool,
    ) -> Self {
        let epoch = cluster_meta.get_epoch();
        let cluster_name = cluster_meta.get_cluster_name().clone();
        let config = cluster_meta.get_config().clone();

        let slot_ranges = cluster_meta.get_local().clone();
        let local_cluster = LocalCluster::from_slot_map(
            sender_factory,
            cluster_name.clone(),
            epoch,
            slot_ranges,
            config,
        );

        let peer_slot_ranges = cluster_meta.get_peer().clone();
        let remote_cluster = RemoteCluster::from_slot_map(
            peer_sender_factory,
            cluster_name.clone(),
            epoch,
            peer_slot_ranges,
            active_redirection,
        );
        Self {
            cluster_name,
            local_cluster,
            remote_cluster,
        }
    }

    pub fn info(&self) -> RespVec {
        let local = self.local_cluster.info();
        let peer = self.remote_cluster.info();

        Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(self.cluster_name.to_string().into_bytes())),
            Resp::Bulk(BulkStr::Str(b"local".to_vec())),
            local,
            Resp::Bulk(BulkStr::Str(b"peer".to_vec())),
            peer,
        ]))
    }

    pub fn send(
        &self,
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), ClusterSendError<<S as CmdTaskSender>::Task>> {
        let cmd_task = match self.local_cluster.send(cmd_task) {
            Err(ClusterSendError::SlotNotFound(cmd_task)) => cmd_task,
            others => return others,
        };

        self.remote_cluster.send_remote(cmd_task)
    }

    pub fn send_remote_directly(
        &self,
        cmd_task: <P as CmdTaskSender>::Task,
        slot: usize,
        address: String,
    ) -> Result<(), ClusterSendError<<P as CmdTaskSender>::Task>> {
        self.remote_cluster
            .send_remote_directly(cmd_task, slot, address.as_str())
    }

    pub fn get_cluster(&self) -> ClusterName {
        self.cluster_name.clone()
    }

    pub fn gen_cluster_nodes(
        &self,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
        cluster_nodes_version: ClusterNodesVersion,
    ) -> String {
        let local = self.local_cluster.gen_local_cluster_nodes(
            service_address,
            migration_states,
            cluster_nodes_version,
        );
        let remote = self
            .remote_cluster
            .gen_remote_cluster_nodes(migration_states, cluster_nodes_version);

        format!("{}{}", local, remote)
    }

    pub fn gen_cluster_slots(
        &self,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<RespVec, String> {
        let mut local = self
            .local_cluster
            .gen_local_cluster_slots(service_address, migration_states)?;
        let mut remote = self
            .remote_cluster
            .gen_remote_cluster_slots(migration_states)?;
        local.append(&mut remote);
        Ok(Resp::Arr(Array::Arr(local)))
    }

    pub fn is_ready(&self) -> bool {
        self.local_cluster.is_ready()
    }

    pub fn get_config(&self) -> &ClusterConfig {
        &self.local_cluster.config
    }

    pub fn get_cluster_any_node(&self) -> Option<String> {
        self.local_cluster.get_any_node()
    }
}

struct SenderMap<S: CmdTaskSender> {
    nodes: HashMap<String, S>,
    slot_map: SlotMap,
}

impl<S: CmdTaskSender> Default for SenderMap<S> {
    fn default() -> Self {
        Self {
            nodes: HashMap::default(),
            slot_map: SlotMap::from_ranges(HashMap::default()),
        }
    }
}

impl<S: CmdTaskSender> SenderMap<S> {
    fn from_slot_map<F: CmdTaskSenderFactory<Sender = S>>(
        sender_factory: &F,
        slot_map: &HashMap<String, Vec<SlotRange>>,
    ) -> Self {
        let mut nodes = HashMap::new();
        for addr in slot_map.keys() {
            nodes.insert(addr.to_string(), sender_factory.create(addr.to_string()));
        }
        Self {
            nodes,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
        }
    }
}

pub struct LocalCluster<S: CmdTaskSender> {
    cluster_name: ClusterName,
    epoch: u64,
    local_backend: SenderMap<S>,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
    config: ClusterConfig,
}

impl<S: CmdTaskSender> Default for LocalCluster<S> {
    fn default() -> Self {
        Self {
            cluster_name: EMPTY_CLUSTER_NAME.clone(),
            epoch: 0,
            local_backend: SenderMap::default(),
            slot_ranges: HashMap::default(),
            config: ClusterConfig::default(),
        }
    }
}

impl<S: CmdTaskSender> LocalCluster<S> {
    pub fn from_slot_map<F: CmdTaskSenderFactory<Sender = S>>(
        sender_factory: &F,
        name: ClusterName,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
        config: ClusterConfig,
    ) -> Self {
        let local_backend = SenderMap::from_slot_map(sender_factory, &slot_map);
        LocalCluster {
            cluster_name: name,
            epoch,
            local_backend,
            slot_ranges: slot_map,
            config,
        }
    }

    pub fn info(&self) -> RespVec {
        let lines = vec![
            format!("name: {}", self.cluster_name),
            format!("epoch: {}", self.epoch),
            "nodes:".to_string(),
        ];
        let mut arr: Vec<_> = lines
            .into_iter()
            .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
            .collect();
        arr.extend(format_slot_ranges(&self.slot_ranges));
        Resp::Arr(Array::Arr(arr))
    }

    pub fn send(
        &self,
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), ClusterSendError<<S as CmdTaskSender>::Task>> {
        if self.cluster_name.is_empty() {
            return Err(ClusterSendError::ClusterNotFound { task: cmd_task });
        }

        let slot = match cmd_task.get_slot() {
            Some(slot) => slot,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };

        match self.local_backend.slot_map.get(slot) {
            Some(addr) => match self.local_backend.nodes.get(addr) {
                Some(sender) => sender
                    .send(cmd_task)
                    .map_err(ClusterSendError::from_sender_backend_error),
                None => {
                    warn!("failed to get node");
                    Err(ClusterSendError::SlotNotFound(cmd_task))
                }
            },
            None => Err(ClusterSendError::SlotNotFound(cmd_task)),
        }
    }

    pub fn gen_local_cluster_nodes(
        &self,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
        cluster_nodes_version: ClusterNodesVersion,
    ) -> String {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_nodes_helper(
            &self.cluster_name,
            self.epoch,
            &slot_ranges,
            migration_states,
            true,
            cluster_nodes_version,
        )
    }

    pub fn gen_local_cluster_slots(
        &self,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_slots_helper(&self.cluster_name, &slot_ranges, migration_states)
    }

    pub fn is_ready(&self) -> bool {
        is_ready(&self.slot_ranges)
    }

    pub fn get_any_node(&self) -> Option<String> {
        self.local_backend.nodes.keys().next().cloned()
    }
}

fn is_ready(slot_ranges: &HashMap<String, Vec<SlotRange>>) -> bool {
    for slot_ranges in slot_ranges.values() {
        for slot_range in slot_ranges.iter() {
            match &slot_range.tag {
                SlotRangeTag::Migrating(_) => continue,
                _ => return true,
            }
        }
    }
    false
}

pub struct RemoteCluster<P: CmdTaskSender> {
    cluster_name: ClusterName,
    epoch: u64,
    slot_map: SlotMap,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
    remote_backend: Option<SenderMap<P>>,
}

impl<P: CmdTaskSender> Default for RemoteCluster<P> {
    fn default() -> Self {
        Self {
            cluster_name: EMPTY_CLUSTER_NAME.clone(),
            epoch: 0,
            slot_map: SlotMap::from_ranges(HashMap::default()),
            slot_ranges: HashMap::default(),
            remote_backend: None,
        }
    }
}

impl<P: CmdTaskSender> RemoteCluster<P> {
    pub fn from_slot_map<F: CmdTaskSenderFactory<Sender = P>>(
        sender_factory: &F,
        name: ClusterName,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
        active_redirection: bool,
    ) -> Self {
        let remote_backend = if active_redirection {
            Some(SenderMap::from_slot_map(sender_factory, &slot_map))
        } else {
            None
        };
        Self {
            cluster_name: name,
            epoch,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
            slot_ranges: slot_map,
            remote_backend,
        }
    }

    pub fn info(&self) -> RespVec {
        let lines = vec!["peers:".to_string()];
        let mut arr: Vec<_> = lines
            .into_iter()
            .map(|s| Resp::Bulk(BulkStr::Str(s.into_bytes())))
            .collect();
        arr.extend(format_slot_ranges(&self.slot_ranges));
        Resp::Arr(Array::Arr(arr))
    }

    pub fn send_remote<T: CmdTask>(&self, cmd_task: T) -> Result<(), ClusterSendError<T>> {
        if self.cluster_name.is_empty() {
            return Err(ClusterSendError::ClusterNotFound { task: cmd_task });
        }

        let slot = match cmd_task.get_slot() {
            Some(slot) => slot,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };

        match self.slot_map.get(slot) {
            Some(addr) => {
                if self.remote_backend.is_some() {
                    Err(ClusterSendError::ActiveRedirection {
                        task: cmd_task,
                        slot,
                        address: addr.to_string(),
                    })
                } else {
                    let resp = Resp::Error(gen_moved(slot, addr.to_string()).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Ok(())
                }
            }
            None => {
                let resp = Resp::Error(format!("slot not covered {}", slot).into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                Err(ClusterSendError::SlotNotCovered)
            }
        }
    }

    pub fn send_remote_directly(
        &self,
        cmd_task: <P as CmdTaskSender>::Task,
        slot: usize,
        address: &str,
    ) -> Result<(), ClusterSendError<<P as CmdTaskSender>::Task>> {
        if let Some(remote_backend) = self.remote_backend.as_ref() {
            match remote_backend.nodes.get(address) {
                Some(sender) => sender
                    .send(cmd_task)
                    .map_err(ClusterSendError::from_sender_backend_error),
                None => {
                    warn!("failed to get node");
                    Err(ClusterSendError::SlotNotFound(cmd_task))
                }
            }
        } else {
            let resp = Resp::Error(gen_moved(slot, address.to_string()).into_bytes());
            cmd_task.set_resp_result(Ok(resp));
            Ok(())
        }
    }

    pub fn gen_remote_cluster_nodes(
        &self,
        migration_states: &HashMap<RangeList, MigrationState>,
        cluster_nodes_version: ClusterNodesVersion,
    ) -> String {
        gen_cluster_nodes_helper(
            &self.cluster_name,
            self.epoch,
            &self.slot_ranges,
            migration_states,
            false,
            cluster_nodes_version,
        )
    }

    pub fn gen_remote_cluster_slots(
        &self,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        gen_cluster_slots_helper(&self.cluster_name, &self.slot_ranges, migration_states)
    }
}

fn format_slot_ranges(slot_ranges: &HashMap<String, Vec<SlotRange>>) -> Vec<RespVec> {
    let mut arr = vec![];
    for (node, slot_ranges) in slot_ranges.iter() {
        let slot_ranges = slot_ranges
            .iter()
            .map(|slot_range| {
                let mut lines = vec![];
                if let Some(meta) = slot_range.tag.get_migration_meta() {
                    let meta_lines = vec![
                        meta.epoch.to_string().into_bytes(),
                        format!("src_proxy: {}", meta.src_proxy_address).into_bytes(),
                        format!("src_node: {}", meta.src_node_address).into_bytes(),
                        format!("dst_proxy: {}", meta.dst_proxy_address).into_bytes(),
                        format!("dst_node: {}", meta.dst_node_address).into_bytes(),
                    ];
                    lines.extend(meta_lines.into_iter().map(|s| Resp::Bulk(BulkStr::Str(s))));
                }
                lines.push(Resp::Bulk(BulkStr::Str(
                    slot_range
                        .get_range_list()
                        .to_strings()
                        .join(" ")
                        .into_bytes(),
                )));
                Resp::Arr(Array::Arr(lines))
            })
            .collect::<Vec<RespVec>>();
        let slot_ranges = Resp::Arr(Array::Arr(slot_ranges));
        arr.push(Resp::Arr(Array::Arr(vec![
            Resp::Bulk(BulkStr::Str(node.to_string().into_bytes())),
            slot_ranges,
        ])));
    }
    arr
}

pub enum ClusterSendError<T: CmdTask> {
    MissingKey,
    ClusterNotFound {
        task: T,
    },
    SlotNotFound(T),
    SlotNotCovered,
    Backend(BackendError),
    MigrationError,
    ActiveRedirection {
        task: T,
        slot: usize,
        address: String,
    },
    Retry(T),
}

impl<T: CmdTask> ClusterSendError<T> {
    fn from_sender_backend_error(err: SenderBackendError<T>) -> Self {
        match BackendError::from_sender_backend_error(err) {
            Either::Left(backend_err) => Self::Backend(backend_err),
            Either::Right(retry_err) => Self::Retry(retry_err.into_inner()),
        }
    }
}

impl<T: CmdTask> fmt::Display for ClusterSendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: CmdTask> fmt::Debug for ClusterSendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::MissingKey => "ClusterSendError::MissingKey".to_string(),
            Self::ClusterNotFound { task: _ } => "ClusterSendError::ClusterNotFound".to_string(),
            Self::SlotNotFound(_) => "ClusterSendError::SlotNotFound".to_string(),
            Self::SlotNotCovered => "ClusterSendError::SlotNotCovered".to_string(),
            Self::Backend(err) => format!("ClusterSendError::Backend({})", err),
            Self::MigrationError => "ClusterSendError::MigrationError".to_string(),
            Self::ActiveRedirection { slot, address, .. } => {
                format!("ClusterSendError::Moved({} {})", slot, address)
            }
            Self::Retry(_) => "ClusterSendError::Retry".to_string(),
        };
        write!(f, "{}", s)
    }
}

impl<T: CmdTask> Error for ClusterSendError<T> {
    fn cause(&self) -> Option<&dyn Error> {
        match self {
            Self::MissingKey => None,
            Self::ClusterNotFound { .. } => None,
            Self::SlotNotFound(_) => None,
            Self::Backend(err) => Some(err),
            Self::SlotNotCovered => None,
            Self::MigrationError => None,
            Self::ActiveRedirection { .. } => None,
            Self::Retry(_) => None,
        }
    }
}

impl<T: CmdTask> ClusterSendError<T> {
    pub fn map_task<P, F>(self, f: F) -> ClusterSendError<P>
    where
        P: CmdTask,
        F: Fn(T) -> P,
    {
        match self {
            Self::MissingKey => ClusterSendError::MissingKey,
            Self::ClusterNotFound { task } => ClusterSendError::ClusterNotFound { task: f(task) },
            Self::SlotNotFound(task) => ClusterSendError::SlotNotFound(f(task)),
            Self::Backend(err) => ClusterSendError::Backend(err),
            Self::SlotNotCovered => ClusterSendError::SlotNotCovered,
            Self::MigrationError => ClusterSendError::MigrationError,
            Self::ActiveRedirection {
                task,
                slot,
                address,
            } => ClusterSendError::ActiveRedirection {
                task: f(task),
                slot,
                address,
            },
            Self::Retry(task) => ClusterSendError::Retry(f(task)),
        }
    }
}

fn gen_cluster_nodes_helper(
    name: &ClusterName,
    epoch: u64,
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
    migration_states: &HashMap<RangeList, MigrationState>,
    local: bool,
    cluster_nodes_version: ClusterNodesVersion,
) -> String {
    let mut cluster_nodes = String::from("");
    for (addr, ranges) in slot_ranges {
        let id = gen_node_id(name, addr.as_str());
        let address = match cluster_nodes_version {
            ClusterNodesVersion::V1 => addr.clone(),
            ClusterNodesVersion::V2 => format!("{}@{}", addr, CLUSTER_NODES_CPORT),
        };

        let mut slot_range_str = String::new();
        let slot_range = ranges
            .iter()
            .flat_map(|slot_range| {
                if should_ignore_slots(slot_range, migration_states) {
                    return None;
                }
                let ranges: Vec<String> = slot_range
                    .get_range_list()
                    .get_ranges()
                    .iter()
                    .map(|range| {
                        if range.start() == range.end() {
                            range.start().to_string()
                        } else {
                            format!("{}-{}", range.start(), range.end())
                        }
                    })
                    .collect();
                Some(ranges)
            })
            .flatten()
            .collect::<Vec<String>>()
            .join(" ");
        if !slot_range.is_empty() {
            slot_range_str.push(' ');
            slot_range_str.push_str(&slot_range);
        }

        let flags = if local { "myself,master" } else { "master" };

        let line = format!(
            "{id} {address} {flags} {master} {ping_sent} {pong_recv} {epoch} {link_state}{slot_range}\n",
            id=id, address=address, flags=flags, master="-", ping_sent=0, pong_recv=0, epoch=epoch,
            link_state="connected", slot_range=slot_range_str,
        );
        cluster_nodes.push_str(&line);
    }
    cluster_nodes
}

#[inline]
fn gen_node_id(cluster_name: &ClusterName, addr: &str) -> String {
    // 24 bytes cluster name + 16 bytes address hash
    // format!("{:x}", u64::MAX).len() == 16
    let mut name_seg = format!("{:_<24}", cluster_name.to_string());
    name_seg.truncate(24);

    let mut addr_hash_seg = format!("{:_<16x}", crc64(0, addr.as_bytes()));
    addr_hash_seg.truncate(16);
    format!("{}{}", name_seg, addr_hash_seg)
}

fn should_ignore_slots(
    range: &SlotRange,
    migration_states: &HashMap<RangeList, MigrationState>,
) -> bool {
    // In the new migration protocol, after switching at the very beginning,
    // the importing nodes will take care of all the migrating slots.
    // From the point of view of other nodes, since they can't
    // find any migration_states, migrating nodes always does not
    // own the migrating slots while the importing nodes always own
    // the migrating slots.
    match &range.tag {
        SlotRangeTag::Migrating(_) => {
            migration_states.get(range.get_range_list()).cloned() != Some(MigrationState::PreCheck)
        }
        SlotRangeTag::Importing(_) => {
            migration_states.get(range.get_range_list()).cloned() == Some(MigrationState::PreCheck)
        }
        _ => false,
    }
}

fn gen_cluster_slots_helper(
    name: &ClusterName,
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
    migration_states: &HashMap<RangeList, MigrationState>,
) -> Result<Vec<RespVec>, String> {
    let mut slot_range_element = Vec::new();
    for (addr, ranges) in slot_ranges {
        let mut segs = addr.split(':');
        let host = segs
            .next()
            .ok_or_else(|| format!("invalid address {}", addr))?;
        let port = segs
            .next()
            .ok_or_else(|| format!("invalid address {}", addr))?;

        for slot_range in ranges {
            if should_ignore_slots(slot_range, migration_states) {
                continue;
            }

            let node_id = gen_node_id(name, addr.as_str());
            let ip_port_array = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(host.as_bytes().to_vec())),
                Resp::Integer(port.as_bytes().to_vec()),
                Resp::Bulk(BulkStr::Str(node_id.into_bytes())),
            ]));

            for range in slot_range.get_range_list().get_ranges().iter() {
                let mut arr = vec![
                    Resp::Integer(range.start().to_string().into_bytes()),
                    Resp::Integer(range.end().to_string().into_bytes()),
                ];
                arr.push(ip_port_array.clone());
                slot_range_element.push(Resp::Arr(Array::Arr(arr)))
            }
        }
    }
    Ok(slot_range_element)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::cluster::{MigrationMeta, RangeList};
    use crate::protocol::{Array, BulkStr};
    use std::convert::TryFrom;
    use std::iter::repeat;

    #[test]
    fn test_gen_node_id() {
        assert_eq!(format!("{:x}", u64::MAX).len(), 16);

        let short_cluster_name: String = (0..10).map(|_| 'x').collect();
        let short = ClusterName::try_from(short_cluster_name.as_str()).unwrap();
        let node_id = gen_node_id(&short, "127.0.0.1:6379");
        assert_eq!(node_id.len(), 40);

        let long_cluster_name: String = (0..30).map(|_| 'x').collect();
        let long = ClusterName::try_from(long_cluster_name.as_str()).unwrap();
        let node_id = gen_node_id(&long, "127.0.0.1:6379");
        assert_eq!(node_id.len(), 40);
    }

    fn gen_testing_slot_ranges(address: &str) -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(
            address.to_string(),
            vec![
                SlotRange {
                    range_list: RangeList::try_from("1 0-100").unwrap(),
                    tag: SlotRangeTag::None,
                },
                SlotRange {
                    range_list: RangeList::try_from("1 300-300").unwrap(),
                    tag: SlotRangeTag::None,
                },
            ],
        );
        slot_ranges
    }

    fn gen_testing_migration_slot_ranges(migrating: bool) -> HashMap<String, Vec<SlotRange>> {
        let mut slot_ranges = HashMap::new();
        let meta = MigrationMeta {
            epoch: 200,
            src_proxy_address: "127.0.0.1:7000".to_string(),
            src_node_address: "127.0.0.1:6379".to_string(),
            dst_proxy_address: "127.0.0.1:7001".to_string(),
            dst_node_address: "127.0.0.1:6380".to_string(),
        };
        let tag = if migrating {
            SlotRangeTag::Migrating(meta)
        } else {
            SlotRangeTag::Importing(meta)
        };
        slot_ranges.insert(
            "127.0.0.1:5299".to_string(),
            vec![SlotRange {
                range_list: RangeList::try_from("1 0-1000").unwrap(),
                tag,
            }],
        );
        slot_ranges
    }

    #[test]
    fn test_gen_cluster_nodes() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_slot_ranges("127.0.0.1:5299");
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            true,
            ClusterNodesVersion::V2,
        );
        assert_eq!(output, "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 myself,master - 0 0 233 connected 0-100 300\n");
    }

    #[test]
    fn test_gen_cluster_nodes_with_long_address() {
        // Should always be able to find the migration state.
        // This will never be empty. But should also work.
        let m = HashMap::new();
        let long_address: String = repeat('x').take(50).collect();
        let slot_ranges = gen_testing_slot_ranges(&long_address);
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            false,
            ClusterNodesVersion::V2,
        );
        assert_eq!(output, format!("testcluster_____________a744988af9aa86ed {}@5299 master - 0 0 233 connected 0-100 300\n", long_address));
    }

    #[test]
    fn test_gen_importing_cluster_nodes() {
        // From the point of view of other nodes.
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            false,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_importing_cluster_nodes_without_pre_check_done() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range_list(), MigrationState::PreCheck);
            }
        }
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            true,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 myself,master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_importing_cluster_nodes_with_pre_check_done() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range_list(), MigrationState::PreBlocking);
            }
        }
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            true,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 myself,master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes() {
        // From the point of view of other nodes.
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            false,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes_without_pre_check_done() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range_list(), MigrationState::PreCheck);
            }
        }
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            true,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 myself,master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes_with_pre_check_done() {
        let mut m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        for slot_ranges in slot_ranges.values() {
            for slot_range in slot_ranges {
                m.insert(slot_range.to_range_list(), MigrationState::PreBlocking);
            }
        }
        let output = gen_cluster_nodes_helper(
            &ClusterName::try_from("testcluster").unwrap(),
            233,
            &slot_ranges,
            &m,
            true,
            ClusterNodesVersion::V2,
        );
        assert_eq!(
            output,
            "testcluster_____________9f8fca2805923328 127.0.0.1:5299@5299 myself,master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_cluster_slots() {
        let cluster_name = ClusterName::try_from("test_cluster_name").unwrap();
        let m = HashMap::new();
        let slot_ranges = gen_testing_slot_ranges("127.0.0.1:5299");
        let output = gen_cluster_slots_helper(&cluster_name, &slot_ranges, &m).unwrap();
        let slot_range1 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(0.to_string().into_bytes()),
            Resp::Integer(100.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
                Resp::Bulk(BulkStr::Str(
                    b"test_cluster_name_______9f8fca2805923328".to_vec(),
                )),
            ])),
        ]));
        let slot_range2 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
                Resp::Bulk(BulkStr::Str(
                    b"test_cluster_name_______9f8fca2805923328".to_vec(),
                )),
            ])),
        ]));
        if output != vec![slot_range2.clone(), slot_range1.clone()] {
            assert_eq!(output, vec![slot_range1, slot_range2]);
        }
    }

    #[test]
    fn test_gen_importing_cluster_slots() {
        let cluster_name = ClusterName::try_from("test_cluster_name").unwrap();
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output = gen_cluster_slots_helper(&cluster_name, &slot_ranges, &m).unwrap();
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_gen_migrating_cluster_slots() {
        let cluster_name = ClusterName::try_from("test_cluster_name").unwrap();
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output = gen_cluster_slots_helper(&cluster_name, &slot_ranges, &m).unwrap();
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_ready_check() {
        assert!(is_ready(&gen_testing_slot_ranges("127.0.0.1:5299")));
        assert!(is_ready(&gen_testing_migration_slot_ranges(false)));
        assert!(!is_ready(&gen_testing_migration_slot_ranges(true)));
    }

    #[test]
    fn test_default_cluster_length() {
        ClusterName::try_from(DEFAULT_CLUSTER).unwrap();
    }
}
