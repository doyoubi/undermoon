use super::store::{
    ClusterStore, MetaStore, MetaStoreError, MigrationMetaStore, MigrationSlotRangeStore,
    MigrationSlots, CHUNK_NODE_NUM,
};
use super::utils::InvalidStateError;
use crate::common::cluster::ClusterName;
use crate::common::cluster::{MigrationTaskMeta, Range, RangeList, SlotRange, SlotRangeTag};
use crate::common::utils::SLOT_NUM;
use std::cmp::min;
use std::convert::TryFrom;

pub struct MetaStoreMigrate<'a> {
    store: &'a mut MetaStore,
}

impl<'a> MetaStoreMigrate<'a> {
    pub fn new(store: &'a mut MetaStore) -> Self {
        Self { store }
    }

    pub fn migrate_slots(&mut self, cluster_name: String) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.store.bump_global_epoch();

        let cluster = match self.store.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => cluster,
        };

        let empty_exists = cluster
            .chunks
            .iter()
            .any(|chunk| chunk.stable_slots.iter().any(|slots| slots.is_none()));
        if !empty_exists {
            return Err(MetaStoreError::SlotsAlreadyEven);
        }

        if let Err(err) = Self::check_running_tasks(cluster) {
            return Err(err);
        }

        let migration_slots = Self::remove_slots_from_src(cluster, new_epoch)
            .map_err(|InvalidStateError {}| MetaStoreError::InvalidState)?;
        Self::assign_dst_slots(cluster, migration_slots.clone())
            .map_err(|InvalidStateError {}| MetaStoreError::InvalidState)?;
        cluster.set_epoch(new_epoch);

        Self::print_migration_slot(cluster, &migration_slots);
        Ok(())
    }

    fn remove_slots_from_src(
        cluster: &mut ClusterStore,
        epoch: u64,
    ) -> Result<Vec<MigrationSlots>, InvalidStateError> {
        let dst_chunk_num = cluster
            .chunks
            .iter()
            .filter(|chunk| chunk.stable_slots[0].is_none() && chunk.stable_slots[1].is_none())
            .count();
        let dst_master_num = dst_chunk_num * 2;
        let master_num = cluster.chunks.len() * 2;
        let src_chunk_num = cluster.chunks.len() - dst_chunk_num;
        let src_master_num = src_chunk_num * 2;
        let average = SLOT_NUM / master_num;
        let remainder = SLOT_NUM - average * master_num;

        let mut curr_dst_master_index = 0;
        let mut migration_slots = vec![];
        let mut curr_dst_slots = vec![];
        let mut curr_slots_num = 0;

        for (src_chunk_index, src_chunk) in cluster.chunks.iter_mut().enumerate() {
            for (src_chunk_part, slot_range) in src_chunk.stable_slots.iter_mut().enumerate() {
                if let Some(slot_range) = slot_range {
                    while curr_dst_master_index != dst_master_num {
                        let src_master_index = src_chunk_index * 2 + src_chunk_part;
                        let src_r = (src_master_index < remainder) as usize; // true will be 1, false will be 0
                        let dst_master_index = src_master_num + curr_dst_master_index;
                        let dst_r = (dst_master_index < remainder) as usize; // true will be 1, false will be 0
                        let src_final_num = average + src_r;
                        let dst_final_num = average + dst_r;

                        if slot_range.get_range_list().get_slots_num() <= src_final_num {
                            break;
                        }

                        let need_num = dst_final_num - curr_slots_num;
                        let available_num =
                            slot_range.get_range_list().get_slots_num() - src_final_num;
                        let remove_num = min(need_num, available_num);
                        let num = slot_range
                            .get_range_list()
                            .get_ranges()
                            .last()
                            .map(|r| r.end() - r.start() + 1)
                            .ok_or_else(|| {
                                error!("FATAL remove_slots_from_src: slots > average + src_r >= 0");
                                InvalidStateError
                            })?;

                        if remove_num >= num {
                            let range = slot_range
                                .get_mut_range_list()
                                .get_mut_ranges()
                                .pop()
                                .ok_or_else(|| {
                                    error!("FATAL remove_slots_from_src: need_num >= num");
                                    InvalidStateError
                                })?;
                            curr_dst_slots.push(range);
                            curr_slots_num += num;
                        } else {
                            let range = slot_range
                                .get_mut_range_list()
                                .get_mut_ranges()
                                .last_mut()
                                .ok_or_else(|| {
                                    error!("FATAL remove_slots_from_src");
                                    InvalidStateError
                                })?;
                            let end = range.end();
                            let start = end - remove_num + 1;
                            *range.end_mut() -= remove_num;
                            curr_dst_slots.push(Range(start, end));
                            curr_slots_num += remove_num;
                        }

                        // reset current state
                        if curr_slots_num >= dst_final_num
                            || slot_range.get_range_list().get_slots_num() <= src_final_num
                        {
                            // assert curr_dst_slots.is_not_empty()
                            migration_slots.push(MigrationSlots {
                                meta: MigrationMetaStore {
                                    epoch,
                                    src_chunk_index,
                                    src_chunk_part,
                                    dst_chunk_index: src_chunk_num + (curr_dst_master_index / 2),
                                    dst_chunk_part: curr_dst_master_index % 2,
                                },
                                ranges: curr_dst_slots.drain(..).collect(),
                            });
                            if curr_slots_num >= dst_final_num {
                                curr_dst_master_index += 1;
                                curr_slots_num = 0;
                            }
                            if slot_range.get_range_list().get_slots_num() <= src_final_num {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(migration_slots)
    }

    fn print_migration_slot(cluster: &ClusterStore, mgr_slots: &[MigrationSlots]) {
        info!("cluster {} start migration", cluster.name);
        for slots in mgr_slots.iter() {
            let meta = &slots.meta;
            info!(
                "{} {} {} => {} {}: {:?}",
                meta.epoch,
                meta.src_chunk_index,
                meta.src_chunk_part,
                meta.dst_chunk_index,
                meta.dst_chunk_part,
                slots.ranges
            );
        }
        for chunk in cluster.chunks.iter() {
            for mgr_slots in chunk.migrating_slots.iter() {
                for slots in mgr_slots.iter() {
                    let slot_range = match slots.to_slot_range(&cluster.chunks) {
                        Ok(slot_range) => slot_range,
                        _ => continue,
                    };
                    let tag = &slot_range.tag;
                    if !tag.is_migrating() {
                        continue;
                    }
                    if let Some(meta) = tag.get_migration_meta() {
                        info!(
                            "{} {} {} => {} {}: {}",
                            meta.epoch,
                            meta.src_proxy_address,
                            meta.src_node_address,
                            meta.dst_proxy_address,
                            meta.dst_node_address,
                            slot_range.range_list
                        );
                    }
                }
            }
        }
    }

    fn check_slots_balance(cluster: &ClusterStore) {
        for chunk in cluster.chunks.iter() {
            if !chunk.migrating_slots[0].is_empty() || !chunk.migrating_slots[1].is_empty() {
                return;
            }
        }

        let mut slot_num = vec![];
        for chunk in cluster.chunks.iter() {
            let mut start_empty = false;
            for slots in chunk.stable_slots.iter() {
                match slots.as_ref() {
                    Some(slots) => {
                        slot_num.push(slots.range_list.get_slots_num());
                        if start_empty {
                            error!("Invalid metadata: Scattered slots distribution");
                        }
                    }
                    None => {
                        slot_num.push(0);
                        start_empty = true;
                    }
                }
            }
        }

        let slot_num_without_zeros: Vec<usize> =
            slot_num.iter().filter(|n| **n > 0).cloned().collect();

        let (max, min) = match (
            slot_num_without_zeros.iter().max(),
            slot_num_without_zeros.iter().min(),
        ) {
            (Some(max), Some(min)) => (max, min),
            _ => {
                error!(
                    "Invalid metadata: cluster without any slot {}",
                    cluster.name
                );
                return;
            }
        };

        if max - min > 1 {
            error!("Unbalanced slots: {:?}", slot_num);
        }
    }

    fn assign_dst_slots(
        cluster: &mut ClusterStore,
        migration_slots: Vec<MigrationSlots>,
    ) -> Result<(), InvalidStateError> {
        for migration_slot_range in migration_slots.into_iter() {
            let MigrationSlots { ranges, meta } = migration_slot_range;

            {
                let src_chunk = cluster
                    .chunks
                    .get_mut(meta.src_chunk_index)
                    .ok_or_else(|| {
                        error!("FATAL assign_dst_slots");
                        InvalidStateError
                    })?;
                let migrating_slots = src_chunk
                    .migrating_slots
                    .get_mut(meta.src_chunk_part)
                    .ok_or_else(|| {
                        error!("FATAL assign_dst_slots");
                        InvalidStateError
                    })?;
                let slot_range = MigrationSlotRangeStore {
                    range_list: RangeList::new(ranges.clone()),
                    is_migrating: true,
                    meta: meta.clone(),
                };
                migrating_slots.push(slot_range);
            }
            {
                let dst_chunk = cluster
                    .chunks
                    .get_mut(meta.dst_chunk_index)
                    .ok_or_else(|| {
                        error!("FATAL assign_dst_slots");
                        InvalidStateError
                    })?;
                let migrating_slots = dst_chunk
                    .migrating_slots
                    .get_mut(meta.dst_chunk_part)
                    .ok_or_else(|| {
                        error!("FATAL assign_dst_slots");
                        InvalidStateError
                    })?;
                let slot_range = MigrationSlotRangeStore {
                    range_list: RangeList::new(ranges.clone()),
                    is_migrating: false,
                    meta,
                };
                migrating_slots.push(slot_range);
            }
        }

        Self::compact_slots(cluster);
        Ok(())
    }

    fn compact_slots(cluster: &mut ClusterStore) {
        for chunk in cluster.chunks.iter_mut() {
            for slots in chunk.stable_slots.iter_mut() {
                if let Some(slots) = slots {
                    slots.get_mut_range_list().compact();
                }
            }
            for slots in chunk.migrating_slots.iter_mut() {
                for store in slots.iter_mut() {
                    store.range_list.compact();
                }
            }
        }
    }

    pub fn migrate_slots_to_scale_down(
        &mut self,
        cluster_name: String,
        new_node_num: usize,
    ) -> Result<(), MetaStoreError> {
        let cluster_name = ClusterName::try_from(cluster_name.as_str())
            .map_err(|_| MetaStoreError::InvalidClusterName)?;
        let new_epoch = self.store.bump_global_epoch();

        let cluster = match self.store.clusters.get_mut(&cluster_name) {
            None => return Err(MetaStoreError::ClusterNotFound),
            Some(cluster) => cluster,
        };

        let empty_exists = cluster
            .chunks
            .iter()
            .any(|chunk| chunk.stable_slots.iter().any(|slots| slots.is_none()));
        if empty_exists {
            return Err(MetaStoreError::FreeNodeFound);
        }

        if let Err(err) = Self::check_running_tasks(cluster) {
            return Err(err);
        }

        if new_node_num == 0
            || new_node_num % CHUNK_NODE_NUM != 0
            || new_node_num >= cluster.chunks.len() * CHUNK_NODE_NUM
        {
            return Err(MetaStoreError::InvalidNodeNum);
        }

        let new_chunk_num = new_node_num / 4;
        let migration_slots =
            Self::remove_slots_from_src_to_scale_down(cluster, new_epoch, new_chunk_num)
                .map_err(|InvalidStateError {}| MetaStoreError::InvalidState)?;
        Self::assign_dst_slots(cluster, migration_slots.clone())
            .map_err(|InvalidStateError {}| MetaStoreError::InvalidState)?;
        cluster.set_epoch(new_epoch);

        Self::print_migration_slot(cluster, &migration_slots);
        Ok(())
    }

    fn remove_slots_from_src_to_scale_down(
        cluster: &mut ClusterStore,
        epoch: u64,
        new_chunk_num: usize,
    ) -> Result<Vec<MigrationSlots>, InvalidStateError> {
        let dst_chunk_num = new_chunk_num;

        let dst_master_num = dst_chunk_num * 2;
        let average = SLOT_NUM / dst_master_num;
        let remainder = SLOT_NUM - average * dst_master_num;

        let mut curr_dst_master_index = 0;
        let mut migration_slots = vec![];
        let mut curr_dst_slots = vec![];
        let mut curr_slots_num = 0;

        let dst_existing_slots_num: Vec<usize> = cluster
            .chunks
            .iter()
            .take(dst_chunk_num)
            .flat_map(|chunk| chunk.stable_slots.iter())
            .map(|slot_range| match slot_range {
                Some(slot_range) => slot_range.get_range_list().get_slots_num(),
                None => 0,
            })
            .collect();
        info!("dst_existing_slots_num: {:?}", dst_existing_slots_num);

        for (src_chunk_index, src_chunk) in
            cluster.chunks.iter_mut().enumerate().skip(dst_chunk_num)
        {
            for (src_chunk_part, slot_range) in src_chunk.stable_slots.iter_mut().enumerate() {
                if let Some(slot_range) = slot_range {
                    while curr_dst_master_index != dst_master_num {
                        let dst_r = (curr_dst_master_index < remainder) as usize; // true will be 1, false will be 0
                        let dst_final_num = average + dst_r;

                        let dst_existing = *dst_existing_slots_num
                            .get(curr_dst_master_index)
                            .ok_or_else(|| {
                                error!("FATAL remove_slots_from_src_to_scale_down: get dst existing slots number");
                                InvalidStateError
                            })?;
                        let need_num = dst_final_num - curr_slots_num - dst_existing;
                        let available_num = slot_range.get_range_list().get_slots_num();

                        if available_num == 0 {
                            break;
                        }

                        let remove_num = min(need_num, available_num);
                        let num = slot_range
                            .get_range_list()
                            .get_ranges()
                            .get(0)
                            .map(|r| r.end() - r.start() + 1)
                            .ok_or_else(|| {
                                error!(
                                    "FATAL remove_slots_from_src_to_scale_down: available_num > 0"
                                );
                                InvalidStateError
                            })?;

                        if remove_num >= num {
                            let range = slot_range.get_mut_range_list().get_mut_ranges().remove(0);
                            curr_dst_slots.push(range);
                            curr_slots_num += num;
                        } else {
                            let range = slot_range
                                .get_mut_range_list()
                                .get_mut_ranges()
                                .get_mut(0)
                                .ok_or_else(|| {
                                    error!("FATAL remove_slots_from_src_to_scale_down: available_num > 0");
                                    InvalidStateError
                                })?;
                            // end - start + 1 == remove_num
                            // end == remove_num + start - 1
                            let start = range.start();
                            let end = remove_num + start - 1;
                            *range.start_mut() += remove_num;
                            curr_dst_slots.push(Range(start, end));
                            curr_slots_num += remove_num;
                        }

                        // reset current state
                        if curr_slots_num + dst_existing >= dst_final_num
                            || slot_range.get_range_list().get_slots_num() == 0
                        {
                            // assert curr_dst_slots.is_not_empty()
                            migration_slots.push(MigrationSlots {
                                meta: MigrationMetaStore {
                                    epoch,
                                    src_chunk_index,
                                    src_chunk_part,
                                    dst_chunk_index: curr_dst_master_index / 2,
                                    dst_chunk_part: curr_dst_master_index % 2,
                                },
                                ranges: curr_dst_slots.drain(..).collect(),
                            });
                            if curr_slots_num + dst_existing >= dst_final_num {
                                curr_dst_master_index += 1;
                                curr_slots_num = 0;
                            }
                            if slot_range.get_range_list().get_slots_num() == 0 {
                                break;
                            }
                        }
                    }
                }
                *slot_range = None;
            }
        }

        Ok(migration_slots)
    }

    pub fn commit_migration(&mut self, task: MigrationTaskMeta) -> Result<(), MetaStoreError> {
        // Will bump epoch later on success.
        let new_epoch = self.store.get_global_epoch() + 1;

        let cluster_name = task.cluster_name.clone();

        {
            let cluster = self
                .store
                .clusters
                .get_mut(&cluster_name)
                .ok_or(MetaStoreError::ClusterNotFound)?;
            let task_epoch = match &task.slot_range.tag {
                SlotRangeTag::None => return Err(MetaStoreError::InvalidMigrationTask),
                SlotRangeTag::Migrating(meta) => meta.epoch,
                SlotRangeTag::Importing(meta) => meta.epoch,
            };

            let (src_chunk_index, src_chunk_part) = cluster
                .chunks
                .iter()
                .enumerate()
                .flat_map(|(i, chunk)| {
                    chunk
                        .migrating_slots
                        .iter()
                        .enumerate()
                        .map(move |(j, slot_range_stores)| (i, j, slot_range_stores))
                })
                .flat_map(|(i, j, slot_range_stores)| {
                    slot_range_stores
                        .iter()
                        .map(move |slot_range_store| (i, j, slot_range_store))
                })
                .find(|(_, _, slot_range_store)| {
                    slot_range_store.range_list == task.slot_range.range_list
                        && slot_range_store.meta.epoch == task_epoch
                        && slot_range_store.is_migrating
                })
                .map(|(i, j, _)| (i, j))
                .ok_or(MetaStoreError::MigrationTaskNotFound)?;

            let (dst_chunk_index, dst_chunk_part) = cluster
                .chunks
                .iter()
                .enumerate()
                .flat_map(|(i, chunk)| {
                    chunk
                        .migrating_slots
                        .iter()
                        .enumerate()
                        .map(move |(j, slot_range_stores)| (i, j, slot_range_stores))
                })
                .flat_map(|(i, j, slot_range_stores)| {
                    slot_range_stores
                        .iter()
                        .map(move |slot_range_store| (i, j, slot_range_store))
                })
                .find(|(_, _, slot_range_store)| {
                    slot_range_store.range_list == task.slot_range.range_list
                        && slot_range_store.meta.epoch == task_epoch
                        && !slot_range_store.is_migrating
                })
                .map(|(i, j, _)| (i, j))
                .ok_or(MetaStoreError::MigrationTaskNotFound)?;

            let meta = MigrationMetaStore {
                epoch: task_epoch,
                src_chunk_index,
                src_chunk_part,
                dst_chunk_index,
                dst_chunk_part,
            };

            for chunk in cluster.chunks.iter_mut() {
                for migrating_slots in chunk.migrating_slots.iter_mut() {
                    migrating_slots.retain(|slot_range_store| {
                        !(slot_range_store.is_migrating
                            && slot_range_store.range_list == task.slot_range.range_list
                            && slot_range_store.meta == meta)
                    })
                }
            }

            for chunk in &mut cluster.chunks {
                let removed_slots = chunk.migrating_slots.iter_mut().enumerate().find_map(
                    |(j, migrating_slots)| {
                        migrating_slots
                            .iter()
                            .position(|slot_range_store| {
                                !slot_range_store.is_migrating
                                    && slot_range_store.meta == meta
                                    && slot_range_store.range_list == task.slot_range.range_list
                            })
                            .map(|index| (j, migrating_slots.remove(index).range_list))
                    },
                );
                if let Some((j, mut range_list)) = removed_slots {
                    match chunk.stable_slots.get_mut(j).ok_or_else(|| {
                        error!("FATAL commit_migration");
                        MetaStoreError::InvalidState
                    })? {
                        Some(stable_slots) => {
                            stable_slots
                                .get_mut_range_list()
                                .merge_another(&mut range_list);
                        }
                        stable_slots => {
                            let slot_range = SlotRange {
                                range_list,
                                tag: SlotRangeTag::None,
                            };
                            *stable_slots = Some(slot_range);
                        }
                    }
                    break;
                }
            }

            Self::compact_slots(cluster);
            cluster.set_epoch(new_epoch);

            Self::check_slots_balance(cluster);
        }

        self.store.bump_global_epoch();
        Ok(())
    }

    fn check_running_tasks(cluster: &mut ClusterStore) -> Result<(), MetaStoreError> {
        let running_migration = cluster
            .chunks
            .iter()
            .any(|chunk| chunk.migrating_slots.iter().any(|slots| !slots.is_empty()));
        if running_migration {
            return Err(MetaStoreError::MigrationRunning);
        }

        Ok(())
    }
}
