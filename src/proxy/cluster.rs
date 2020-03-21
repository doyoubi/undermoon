use super::backend::CmdTask;
use super::backend::{BackendError, CmdTaskSender, CmdTaskSenderFactory};
use super::slot::SlotMap;
use crate::common::cluster::{ClusterName, RangeList, SlotRange, SlotRangeTag};
use crate::common::config::ClusterConfig;
use crate::common::proto::ProxyClusterMeta;
use crate::common::utils::{gen_moved, get_slot};
use crate::migration::task::MigrationState;
use crate::protocol::{Array, BulkStr, Resp, RespVec};
use crc64::crc64;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::iter::Iterator;

pub const DEFAULT_CLUSTER: &str = "admin";

#[derive(Debug)]
pub enum ClusterMetaError {
    OldEpoch,
    TryAgain,
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

pub trait ClusterTag {
    fn get_cluster_name(&self) -> ClusterName;
    fn set_cluster_name(&mut self, cluster_name: ClusterName);
}

pub struct ClusterBackendMap<S: CmdTaskSender>
where
    <S as CmdTaskSender>::Task: ClusterTag,
{
    local_clusters: HashMap<ClusterName, LocalCluster<S>>,
    remote_clusters: HashMap<ClusterName, RemoteCluster>,
}

impl<S: CmdTaskSender> Default for ClusterBackendMap<S>
where
    <S as CmdTaskSender>::Task: ClusterTag,
{
    fn default() -> Self {
        Self {
            local_clusters: HashMap::new(),
            remote_clusters: HashMap::new(),
        }
    }
}

impl<S: CmdTaskSender> ClusterBackendMap<S>
where
    <S as CmdTaskSender>::Task: ClusterTag,
{
    pub fn from_cluster_map<F: CmdTaskSenderFactory<Sender = S>>(
        cluster_meta: &ProxyClusterMeta,
        sender_factory: &F,
    ) -> Self {
        let epoch = cluster_meta.get_epoch();

        let mut local_clusters = HashMap::new();
        for (cluster_name, slot_ranges) in cluster_meta.get_local().get_map().iter() {
            let config = cluster_meta.get_configs().get(cluster_name);
            let local_cluster = LocalCluster::from_slot_map(
                sender_factory,
                cluster_name.clone(),
                epoch,
                slot_ranges.clone(),
                config,
            );
            local_clusters.insert(cluster_name.clone(), local_cluster);
        }

        let mut remote_clusters = HashMap::new();
        for (cluster_name, slot_ranges) in cluster_meta.get_peer().get_map().iter() {
            let remote_cluster =
                RemoteCluster::from_slot_map(cluster_name.clone(), epoch, slot_ranges.clone());
            remote_clusters.insert(cluster_name.clone(), remote_cluster);
        }
        Self {
            local_clusters,
            remote_clusters,
        }
    }

    pub fn info(&self) -> String {
        let local = self
            .local_clusters
            .iter()
            .map(|(_, local_cluster)| local_cluster.info())
            .collect::<Vec<String>>()
            .join("\r\n");
        let peer = self
            .remote_clusters
            .iter()
            .map(|(_, remote_cluster)| remote_cluster.info())
            .collect::<Vec<String>>()
            .join("\r\n");
        format!("{}\r\n{}", local, peer)
    }

    pub fn send(
        &self,
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), ClusterSendError<<S as CmdTaskSender>::Task>> {
        let (cmd_task, cluster_exists) = match self.local_clusters.get(&cmd_task.get_cluster_name())
        {
            Some(local_cluster) => match local_cluster.send(cmd_task) {
                Err(ClusterSendError::SlotNotFound(cmd_task)) => (cmd_task, true),
                others => return others,
            },
            None => (cmd_task, false),
        };

        match self.remote_clusters.get(&cmd_task.get_cluster_name()) {
            Some(remote_cluster) => remote_cluster.send_remote(cmd_task),
            None => {
                if cluster_exists {
                    let resp = Resp::Error(
                        format!("slot not found: {}", cmd_task.get_cluster_name()).into_bytes(),
                    );
                    cmd_task.set_resp_result(Ok(resp));
                    Err(ClusterSendError::SlotNotCovered)
                } else {
                    let cluster_name = cmd_task.get_cluster_name().to_string();
                    debug!("cluster not found: {}", cluster_name);
                    let resp =
                        Resp::Error(format!("cluster not found: {}", cluster_name).into_bytes());
                    cmd_task.set_resp_result(Ok(resp));
                    Err(ClusterSendError::ClusterNotFound(cluster_name))
                }
            }
        }
    }

    pub fn get_clusters(&self) -> Vec<ClusterName> {
        self.local_clusters.keys().cloned().collect()
    }

    pub fn gen_cluster_nodes(
        &self,
        cluster_name: ClusterName,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> String {
        let local =
            self.local_clusters
                .get(&cluster_name)
                .map_or("".to_string(), |local_cluster| {
                    local_cluster.gen_local_cluster_nodes(service_address, migration_states)
                });
        let remote = self
            .remote_clusters
            .get(&cluster_name)
            .map_or("".to_string(), |remote_cluster| {
                remote_cluster.gen_remote_cluster_nodes(migration_states)
            });
        format!("{}{}", local, remote)
    }

    pub fn gen_cluster_slots(
        &self,
        cluster_name: ClusterName,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<RespVec, String> {
        let mut local =
            self.local_clusters
                .get(&cluster_name)
                .map_or(Ok(vec![]), |local_cluster| {
                    local_cluster.gen_local_cluster_slots(service_address, migration_states)
                })?;
        let mut remote = self
            .remote_clusters
            .get(&cluster_name)
            .map_or(Ok(vec![]), |remote_cluster| {
                remote_cluster.gen_remote_cluster_slots(migration_states)
            })?;
        local.append(&mut remote);
        Ok(Resp::Arr(Array::Arr(local)))
    }

    pub fn auto_select_cluster(&self) -> Option<ClusterName> {
        {
            let local = &self.local_clusters;
            match local.len() {
                0 => (),
                1 => return local.keys().next().cloned(),
                _ => return None,
            }
        }
        {
            let remote = &self.remote_clusters;
            if remote.len() == 1 {
                return remote.keys().next().cloned();
            }
        }
        None
    }

    pub fn get_config(&self, cluster_name: &ClusterName) -> Option<&ClusterConfig> {
        self.local_clusters
            .get(cluster_name)
            .map(|local_cluster| &local_cluster.config)
    }
}

// We combine the nodes and slot_map to let them fit into
// the same lock with a smaller critical section
// compared to the one we need if splitting them.
struct LocalBackend<S: CmdTaskSender> {
    nodes: HashMap<String, S>,
    slot_map: SlotMap,
}

pub struct LocalCluster<S: CmdTaskSender> {
    name: ClusterName,
    epoch: u64,
    local_backend: LocalBackend<S>,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
    config: ClusterConfig,
}

impl<S: CmdTaskSender> LocalCluster<S> {
    pub fn from_slot_map<F: CmdTaskSenderFactory<Sender = S>>(
        sender_factory: &F,
        name: ClusterName,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
        config: ClusterConfig,
    ) -> Self {
        let mut nodes = HashMap::new();
        for addr in slot_map.keys() {
            nodes.insert(addr.to_string(), sender_factory.create(addr.to_string()));
        }
        let local_backend = LocalBackend {
            nodes,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
        };
        LocalCluster {
            name,
            epoch,
            local_backend,
            slot_ranges: slot_map,
            config,
        }
    }

    pub fn info(&self) -> String {
        let mut lines = vec![
            format!("name: {}", self.name),
            format!("epoch: {}", self.epoch),
            "nodes:".to_string(),
        ];
        for (node, slot_ranges) in self.slot_ranges.iter() {
            let slot_ranges = slot_ranges
                .iter()
                .map(|slot_range| {
                    format!(
                        "{:?} {}",
                        slot_range.tag.get_migration_meta(),
                        slot_range.get_range_list().to_strings().join(" "),
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");
            lines.push(format!("{}: {}", node, slot_ranges));
        }
        lines.join("\r\n")
    }

    pub fn send(
        &self,
        cmd_task: <S as CmdTaskSender>::Task,
    ) -> Result<(), ClusterSendError<<S as CmdTaskSender>::Task>> {
        let key = match cmd_task.get_key() {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };

        match self.local_backend.slot_map.get_by_key(key) {
            Some(addr) => match self.local_backend.nodes.get(addr) {
                Some(sender) => sender.send(cmd_task).map_err(ClusterSendError::Backend),
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
    ) -> String {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_nodes_helper(&self.name, self.epoch, &slot_ranges, migration_states)
    }

    pub fn gen_local_cluster_slots(
        &self,
        service_address: String,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        let slots: Vec<SlotRange> = self
            .slot_ranges
            .values()
            .cloned()
            .flatten()
            .collect::<Vec<SlotRange>>();
        let mut slot_ranges = HashMap::new();
        slot_ranges.insert(service_address, slots);
        gen_cluster_slots_helper(&slot_ranges, migration_states)
    }
}

pub struct RemoteCluster {
    name: ClusterName,
    epoch: u64,
    slot_map: SlotMap,
    slot_ranges: HashMap<String, Vec<SlotRange>>,
}

impl RemoteCluster {
    pub fn from_slot_map(
        name: ClusterName,
        epoch: u64,
        slot_map: HashMap<String, Vec<SlotRange>>,
    ) -> RemoteCluster {
        RemoteCluster {
            name,
            epoch,
            slot_map: SlotMap::from_ranges(slot_map.clone()),
            slot_ranges: slot_map,
        }
    }

    pub fn info(&self) -> String {
        let mut lines = vec!["peers:".to_string()];
        for (node, slot_ranges) in self.slot_ranges.iter() {
            let slot_ranges = slot_ranges
                .iter()
                .map(|slot_range| {
                    format!(
                        "{:?} {}",
                        slot_range.tag.get_migration_meta(),
                        slot_range.get_range_list().to_strings().join(" "),
                    )
                })
                .collect::<Vec<String>>()
                .join(", ");
            lines.push(format!("{}: {}", node, slot_ranges));
        }
        lines.join("\r\n")
    }

    pub fn send_remote<T: CmdTask>(&self, cmd_task: T) -> Result<(), ClusterSendError<T>> {
        let key = match cmd_task.get_key() {
            Some(key) => key,
            None => {
                let resp = Resp::Error("missing key".to_string().into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                return Err(ClusterSendError::MissingKey);
            }
        };
        match self.slot_map.get_by_key(key) {
            Some(addr) => {
                let resp = Resp::Error(gen_moved(get_slot(key), addr.to_string()).into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                Ok(())
            }
            None => {
                let resp = Resp::Error(format!("slot not covered {:?}", key).into_bytes());
                cmd_task.set_resp_result(Ok(resp));
                Err(ClusterSendError::SlotNotCovered)
            }
        }
    }

    pub fn gen_remote_cluster_nodes(
        &self,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> String {
        gen_cluster_nodes_helper(&self.name, self.epoch, &self.slot_ranges, migration_states)
    }

    pub fn gen_remote_cluster_slots(
        &self,
        migration_states: &HashMap<RangeList, MigrationState>,
    ) -> Result<Vec<RespVec>, String> {
        gen_cluster_slots_helper(&self.slot_ranges, migration_states)
    }
}

pub enum ClusterSendError<T: CmdTask> {
    MissingKey,
    ClusterNotFound(String),
    SlotNotFound(T),
    SlotNotCovered,
    Backend(BackendError),
    MigrationError,
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
            Self::ClusterNotFound(cluster_name) => {
                format!("ClusterSendError::ClusterNotFound({})", cluster_name)
            }
            Self::SlotNotFound(_) => "ClusterSendError::SlotNotFound".to_string(),
            Self::SlotNotCovered => "ClusterSendError::SlotNotCovered".to_string(),
            Self::Backend(err) => format!("ClusterSendError::Backend({})", err),
            Self::MigrationError => "ClusterSendError::MigrationError".to_string(),
        };
        write!(f, "{}", s)
    }
}

impl<T: CmdTask> Error for ClusterSendError<T> {
    fn description(&self) -> &str {
        match self {
            ClusterSendError::MissingKey => "missing key",
            ClusterSendError::ClusterNotFound(_) => "cluster not found",
            ClusterSendError::SlotNotFound(_) => "slot not found",
            ClusterSendError::Backend(_) => "backend error",
            ClusterSendError::SlotNotCovered => "slot not covered",
            ClusterSendError::MigrationError => "migration queue error",
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match self {
            ClusterSendError::MissingKey => None,
            ClusterSendError::ClusterNotFound(_) => None,
            ClusterSendError::SlotNotFound(_) => None,
            ClusterSendError::Backend(err) => Some(err),
            ClusterSendError::SlotNotCovered => None,
            ClusterSendError::MigrationError => None,
        }
    }
}

fn gen_cluster_nodes_helper(
    name: &ClusterName,
    epoch: u64,
    slot_ranges: &HashMap<String, Vec<SlotRange>>,
    migration_states: &HashMap<RangeList, MigrationState>,
) -> String {
    let mut cluster_nodes = String::from("");
    let mut name_seg = format!("{:_<20}", name.to_string());
    name_seg.truncate(20);
    for (addr, ranges) in slot_ranges {
        let mut addr_hash_seg = format!("{:_<20x}", crc64(0, addr.as_bytes()));
        addr_hash_seg.truncate(20);
        let id = format!("{}{}", name_seg, addr_hash_seg);

        let mut slot_range_str = String::new();
        let slot_range = ranges
            .iter()
            .map(|slot_range| {
                if should_ignore_slots(&slot_range, &migration_states) {
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
            .filter_map(|s| s)
            .flatten()
            .collect::<Vec<String>>()
            .join(" ");
        if !slot_range.is_empty() {
            slot_range_str.push(' ');
            slot_range_str.push_str(&slot_range);
        }

        let line = format!(
            "{id} {addr} {flags} {master} {ping_sent} {pong_recv} {epoch} {link_state}{slot_range}\n",
            id=id, addr=addr, flags="master", master="-", ping_sent=0, pong_recv=0, epoch=epoch,
            link_state="connected", slot_range=slot_range_str,
        );
        cluster_nodes.push_str(&line);
    }
    cluster_nodes
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

            let ip_port_array = Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str(host.as_bytes().to_vec())),
                Resp::Integer(port.as_bytes().to_vec()),
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
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(output, "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-100 300\n");
    }

    #[test]
    fn test_gen_cluster_nodes_with_long_address() {
        // Should always be able to find the migration state.
        // This will never be empty. But should also work.
        let m = HashMap::new();
        let long_address: String = repeat('x').take(50).collect();
        let slot_ranges = gen_testing_slot_ranges(&long_address);
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(output, format!("testdb______________a744988af9aa86ed____ {} master - 0 0 233 connected 0-100 300\n", long_address));
    }

    #[test]
    fn test_gen_importing_cluster_nodes() {
        // From the point of view of other nodes.
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
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
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
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
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
        );
    }

    #[test]
    fn test_gen_migrating_cluster_nodes() {
        // From the point of view of other nodes.
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
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
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected 0-1000\n"
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
        let output =
            gen_cluster_nodes_helper(&ClusterName::from("testdb").unwrap(), 233, &slot_ranges, &m);
        assert_eq!(
            output,
            "testdb______________9f8fca2805923328____ 127.0.0.1:5299 master - 0 0 233 connected\n"
        );
    }

    #[test]
    fn test_gen_cluster_slots() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_slot_ranges("127.0.0.1:5299");
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
        let slot_range1 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(0.to_string().into_bytes()),
            Resp::Integer(100.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
            ])),
        ]));
        let slot_range2 = Resp::Arr(Array::Arr(vec![
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Integer(300.to_string().into_bytes()),
            Resp::Arr(Array::Arr(vec![
                Resp::Bulk(BulkStr::Str("127.0.0.1".to_string().into_bytes())),
                Resp::Integer(5299.to_string().into_bytes()),
            ])),
        ]));
        if output != vec![slot_range2.clone(), slot_range1.clone()] {
            assert_eq!(output, vec![slot_range1, slot_range2]);
        }
    }

    #[test]
    fn test_gen_importing_cluster_slots() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(false);
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_gen_migrating_cluster_slots() {
        let m = HashMap::new();
        let slot_ranges = gen_testing_migration_slot_ranges(true);
        let output = gen_cluster_slots_helper(&slot_ranges, &m).expect("test_gen_cluster_slots");
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_default_cluster_length() {
        ClusterName::from(DEFAULT_CLUSTER).unwrap();
    }
}
