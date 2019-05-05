# Slots Migration

Goals:
- Simple
- Fast

Let's suppose we're migrating slots `0-1000` from node A to node B, proxy A to proxy B.

#### (1) Broker changes the metadata.
Broker add `MIGRATING` and `IMPORTING` flags to A and B, bumping the cluster epoch.

#### (2) Coordinator synchronize the metadata.
Coordinator sending the flags to node A and B by `UMCTL SETDB`.

#### (3) Node A starts checking the replication process
Proxy A checks whether the replication process on node A with node B has finished. Wait until the lag is close.

#### (4) Node B starts the replication with node A.
Proxy B send `SLAVEOF node_A_ip node_A_port` to node B to start the replication.

Now the commands for slots `0-1000` is still owned by node A. Proxy B will redirect them to proxy A.

#### (5) Proxy A transfers the slot range to proxy B temporarily.
Proxy A will
- Set the `Tmp Tranferred Started Tag` for `0-1000`. Now all the commands on `0-1000` will be redirected to a queue.
- These commands in the queue will wait for a short time to let the replication finish hopefully and start replying `MOVED` to proxy B.
- Keep sending `UMCTL TMPSWITCH proxy_version db_name 0-1000 epoch proxy_A_address node_A_address proxy_B_address node_B_address` to proxy B.
- After gets the `OK` reply from proxy B, proxy A set the `Tmp Tranferred Commited Tag`.
- Now proxy A will start to reply `Tmp Tranferred Commited Tag for node A to node B migration on 0-1000 of SomeDB` for `UMCTL SETDB`
- If proxy A waits for too long, it will directly change to the final state.

#### (6) Proxy B reacts to UMCTL TMPSWITCH
Proxy B sets the `Tmp Tranferred Started Tag` and starts accepting commands on `0-1000` and returns `OK` to proxy A.
Proxy B does not stop the replication now.
If proxy B waits for too long, it will directly change to the final state.

#### (7) Coordinator Reacts to the Done Flag from proxy A.
Coordinator finds out the migration is done from the reply of `UMCTL INFOMGR` and writes that to the broker.

#### (8) Broker sets the new metadata.
Broker cleans up `MIGRATING` and `IMPORTING` flags and bumps the epoch.

### (9) Coordinator synchronize the new metadata.
Coordinator gets the new metadata.

### (10) Proxy A clean up the migration.
Proxy A sets the new metadata and stop the migration process.

### (11) Proxy B cleanup the migration.
Proxy B sets the new metadata and stop the replication.

## Notices
- When broker replace nodes with importing flags, it should also change the corresponding nodes with migration flags.
- Every migration task should be associated with the epoch at which it starts. This epoch should also be stored inside the migrating and importing metadata transferred to proxies.
- Migrating meta should contains
  - epoch
  - cluster(db name)
  - src proxy address
  - src node address
  - dst proxy address
  - dst node address
  - slot ranges
- Migration metadata are bounded to nodes, not cluster.
