# Design

Rules:
- Simple
- Stateless coordinator
- Communicate by pushing meta data to server-side proxy
- Coordinator don't need to communicate with each other
- Rely on a global epoch
- No replica currently

# Meta Data

- Global epoch
- Coordinators
- Clusters { ip:port => slots }
- Machines { cluster => ip:port => slots }
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
- Coordinator keeps doing the following operations in order:
    - pushing the message `Add 0-1000` to B. 
    - pushing the message `Releasing 0-1000` to A.
- When A is still migrating data, it will reject the message `Releasing 0-1000`.
- When A is done with migrating data, it will:
    - add 0-1000 to node B and start redirecting clients.
    - remove 0-1000 from itself.
- Coordinator finally propagate the message `Node B owns 0-1000` to all other nodes.

Note that (1) can employ a blocking or non-blocking method for the comming commands during switching slot owner.

## Failure Recovery in Migration
Coordinator is totally stateless. There's only one coordinator start the process. But all of them will keep pushing meta data to proxy.

The only problem is how to deal the failure of node A and B.

If in any phase node A fails, the coordinators should create a new one with the same slots include the migrating flag.
If node B fails, the coordinators should create a new one and tell node A to change its destination of migration.

# Control Commands

- nmctl listdb
- nmctl cleardb
- ping
### nmctl setdb

- nmctl setdb epoch flags [dbname1 ip:port slot_range] ...
- `flags` is reserved. Currently it's just NOFLAG
- `slot_range` can be
    - 0-1000
    - migrating dst_ip:dst_port 0-1000

### nmctl setpeer

- nmctl setpeer epoch flags [dbname1 ip:port slot_range] ...
- `flags` is reserved. Currently it's just NOFLAG
- `slot_range` can be in the form of 0-1000

# Epoch

- Zero epoch is used to tag uninitialized state.
