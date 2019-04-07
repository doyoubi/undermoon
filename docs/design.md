# Design

Rules:
- Simple
- Stateless coordinator
- Communicate by pushing meta data to server-side proxy
- Coordinator don't need to communicate with each others
- Rely on epoch for each host, cluster
- No replica currently

# Meta Data

- Cluster epoch { cluster => epoch }
- Cluster { ip:port => slots }
- Machine epoch { machine => epoch }
- Machine cluster { cluster => ip:port => slots }
- Coordinator
- Failure Count { ip => coordinator ips }

# Operations

- Creating a new cluster
- Removing a cluster
- Failure Detection
- Failure Recovery
- Adding and removing nodes in a cluster
- Migrating slots

# Slots Migration
Lets say we're migrating a range of slots 0-1000 from node A to node B.

- A owns the slots 0-1000.
- Coordinator pushes the message `Migrating 0-1000 to B` to A.
- Coordinator pushes the message `Importing 0-1000 from A` to B.
- Coordinator keeps doing the following operations in order:
    - pushing the message `Importing 0-1000 from A` to B.
    - pushing the message `Migrating 0-1000 to B` to A.
- When A is still migrating data, it will return the normal message `OK`.
- When A is done with migrating data, it will:
    - add 0-1000 to node B and start redirecting clients.
    - remove 0-1000 from itself.
    - tell the Coordinator by returning `Done with migrating 0-1000 from A`(which could have multiple this kind of migrating results)
    - return the response above if later some other Coordinator still sending `Migraitng 0-1000 to B` to A.
- Coordinator finally propagate the message `Node B owns 0-1000` to all other nodes.

Note that (1) can employ a blocking or non-blocking method for the coming commands during switching slot owner.

- Before A receive a `Migrating 0-1000 to B`, it should have set up the `MasterService`. If not, it will return `-MASTER NOT READY`.
- `Migrating 0-1000 from A` will let B create a `ReplicaService` during the data migration.

## Failure Recovery in Migration
Coordinator is totally stateless. There's only one coordinator start the process. But all of them will keep pushing meta data to proxy.

The only problem is how to deal the failure of node A and B.

If in any phase node A fails, the coordinators should create a new one with the same slots include the migrating flag.
If node B fails, the coordinators should create a new one and tell node A to change its destination of migration.

# Control Commands

- nmctl listdb
- nmctl cleardb
- ping
- cluster nodes
- cluster slots
### nmctl setdb

- nmctl setdb epoch flags [dbname1 ip:port slot_range] ...
- `epoch` is the epoch of host
- `flags` is reserved. Currently it may be NOFLAG or FORCE. In the future if we add more flags, separate them by ','.
- `slot_range` can be
    - 0-1000
    - migrating dst_ip:dst_port 0-1000

### nmctl setpeer

- nmctl setpeer epoch flags [dbname1 ip:port slot_range] ...
- `epoch` is the epoch of host
- `flags` is reserved. Currently it may be NOFLAG or FORCE.
- `slot_range` can be in the form of 0-1000

# Epoch

- Zero epoch is used to tag uninitialized state.

# Coordinator
The coordinators keep getting the meta data from brokers and push the meta data to the corresponding server-side proxies.

## Create Cluster
- get the meta data about clusters and machines
- find where to deploy new redis
- write that new meta data to brokers with transaction

# Replication
- May support different underlying storage systems including Redis.
- Abstract the different replication type by `MasterService` and `ReplicaService` trait.
- `MasterService` should keep sending `SLAVEOF NO ONE` to corresponding master.

## Interface
We need to separate the replication, including the extra port needed by replication.
#### UMCTL SETMASTER [dbname1 master_ip:master_port repl_port] ...
- `repl_port` is the replication port of this master used by other proxy as replica. We use another port to decouple the replication module from others.
It will create a `MasterService`.

#### UMCTL SETREPLICA [dbname1 replica_ip:replica_port proxy_ip:proxy_port master_ip:master_port] ...
It will create a `ReplicaService`.

For Redis, it should get the `repl_port` by using `UMCTL REPLINFO` and connect to it.

#### UMCTL REPLINFO master_ip:master_port
Returned response depends on different `MasterService`.

Basically it should contains:
- `repl_port` for this master.

#### Implementation Details
- Proxy should periodically send `SLAVEOF NO ONE` to master in case it was wrongly set.
- For Redis, `MasterService` and `ReplicaService` should check the replication process by using the data in `INFO` of redis.
