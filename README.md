![undermoon logo](docs/undermoon-logo.png)

# Undermoon [![Build Status](https://travis-ci.com/doyoubi/undermoon.svg?branch=master)](https://travis-ci.com/doyoubi/undermoon)
`Undermoon` is a self-managed Redis clustering system based on **Redis Cluster Protocol** supporting:

- Horizontal scalability and high availability
- Cluster management through HTTP API
- Automatic failover for both master and replica
- Fast scaling
- String value compression

Any storage system implementing redis protocol could also somehow work with undermoon,
such as [KeyDB](https://github.com/JohnSully/KeyDB) and [pika](https://github.com/Qihoo360/pika).

#### Redis Cluster Client Protocol
[Redis Cluster](https://redis.io/topics/cluster-tutorial) is the official Redis distributed solution supporting sharding and failover.
Compared to using a single instance redis, clients connecting to `Redis Cluster` need to implement the `Redis Cluster Client Protocol`.
What it basically does is:
- Redirecting the requests if we are not sending commands to the right node.
- Caching the cluster routing table from one of the two commands, `CLUSTER NODES` and `CLUSTER SLOTS`.

To be compatible with the existing Redis client,
there're also some `Redis Cluster Proxies`
like [redis-cluster-proxy(official)](https://github.com/RedisLabs/redis-cluster-proxy),
[aster](https://github.com/wayslog/aster),
and [corvus](https://github.com/eleme/corvus)
to adapt the cluster protocol to the widely supported single instance protocol.

#### Why another "Redis Cluster Protocol" implementation?
This implementation not only support horizontal scalability and high availability,
but also enable you to build a self-managed distributed Redis supporting:
- Redis resource pool management
- Serving multiple clusters for different users
- Spreading the flood evenly to all the physical machines
- Scaling fast
- Much easier operation.

#### Why server-side proxy?
Redis and most redis proxies such as redis-cluster-proxy, corvus, aster, codis are deployed in separated machines
as the proxies normally need to spread the requests to different Redis instances.

Instead of routing requests, server-side proxy serves as a different role from these proxies
and is similar to the cluster module of Redis, which make it able to migrate the data and scale in a super fast way
by using some customized migration protocols.

#### Server-side Proxy
##### A small bite of server-side proxy and Redis Cluster Protocol

```bash
# Run a redis-server first
$ redis-server
```

```bash
# Build and run the server_proxy
> cargo build
> make server  # runs on port 5299 and will forward commands to 127.0.0.1:6379
```

```bash
> redis-cli -p 5299
# Initialize the proxy by `UMCTL` commands.
127.0.0.1:5299> UMCTL SETCLUSTER 1 NOFLAGS mydb 127.0.0.1:6379 1 0-8000 PEER mydb 127.0.0.1:7000 1 8001-16383

# Done! We can use it like a Redis Cluster!
# Unlike the official Redis Cluster, it only displays master nodes here
# instead of showing both masters and replicas.
127.0.0.1:5299> CLUSTER NODES
mydb________________9f8fca2805923328____ 127.0.0.1:5299 myself,master - 0 0 1 connected 0-8000
mydb________________d458dd9b55cc9ad9____ 127.0.0.1:7000 master - 0 0 1 connected 8001-16383

# As we initialize it using UMCTL SETCLUSTER,
# slots 8001-16383 belongs to another server proxy 127.0.0.1:7000
# so we get a redirection response.
#
# This is the key difference between normal Redis client and Redis Cluster client
# as we need to deal with the redirection.
127.0.0.1:5299> get a
(error) MOVED 15495 127.0.0.1:7000

# Key 'b' is what this proxy is responsible for so we process the request.
127.0.0.1:5299> set b 1
OK
```

### Architecture
![architecture](docs/architecture.svg)
##### Metadata Broker
Now it's a single point in-memory metadata storage storing all the metadata of the whole `undermoon` cluster,
including existing Redis instances, proxies, and exposed Redis clusters.
It will be replaced by [another implementation backed by Etcd]((https://github.com/doyoubi/overmoon)) in the future.

##### Coordinator
Coordinator will synchronize the metadata between broker and server proxy.
It also actively checks the liveness of server proxy and initiates a failover.

##### Storage Cluster
The storage cluster consists of server proxies and Redis instances.
It serves just like the official Redis Cluster to the applications.
A Redis Cluster Proxy could be added between it and applications
so that applications don't need to upgrade their Redis clients.
###### Chunk
Chunk is the smallest building block of every single exposed Redis Cluster.
Each chunk consists of 4 Redis instances and 2 server proxies evenly distributed in two different physical machines.
So the node number of each Redis cluster will be the multiples of 4 with half masters and half replicas.

The design of chunk makes it very easy to build a cluster with a good topology for **workload balancing**.

## Getting Started
Requirements:

- docker-compose
- redis-cli

#### Run the cluster in docker-compose
Before run any example, run this command to build the basic `undermoon` docker image:
```bash
$ make docker-build-image
$ make docker-mem-broker
```

#### Register Proxies
After everything is up, run the initialize script to register the storage resources:
```bash
$ ./examples/mem-broker/init.sh
```

We have 6 available proxies.
```bash
$ curl http://localhost:7799/api/v2/proxies/addresses
```

#### Create Cluster
Since every proxy has 2 corresponding Redis nodes, we have 12 nodes in total.
Note that the number of a cluster could only be the multiples of 4.
Let's create a cluster with 4 nodes.
```bash
$ curl -XPOST -H 'Content-Type: application/json' http://localhost:7799/api/v2/clusters/meta/mycluster -d '{"node_number": 4}'
```

Before connecting to the cluster, you need to add these hosts to you `/etc/hosts`:
```
# /etc/hosts
127.0.0.1 server_proxy1
127.0.0.1 server_proxy2
127.0.0.1 server_proxy3
127.0.0.1 server_proxy4
127.0.0.1 server_proxy5
127.0.0.1 server_proxy6
```

Let's checkout our cluster. It's created by some randomly chosen proxies.
We need to find them out first. Note that you need to install the `jq` command.

```bash
# List the proxies of the our "mycluster`:
$ curl -s http://localhost:7799/api/v2/clusters/meta/mycluster | jq '.cluster.nodes[].proxy_address' | uniq
"server_proxy5:6005"
"server_proxy6:6006"
```

Pickup one of the proxy address above (in my case it's `server_proxy5:6005`) for the cluster `mycluster` and connect to it.

```bash
# Add `-c` to enable cluster mode:
$ redis-cli -h server_proxy5 -p 6005 -c
# List the proxies:
server_proxy5:6005> cluster nodes
mycluster___________d71bc00fbdddf89_____ server_proxy5:6005 myself,master - 0 0 7 connected 0-8191
mycluster___________8de73f9146386295____ server_proxy6:6006 master - 0 0 7 connected 8192-16383
# Send out some requests:
server_proxy5:6005> get a
-> Redirected to slot [15495] located at server_proxy6:6006
(nil)
server_proxy6:6006> get b
-> Redirected to slot [3300] located at server_proxy5:6005
(nil)
```

#### Scale Up
It actually has 4 Redis nodes under the hood.
```bash
# List the nodes of the our "mycluster`:
$ curl -s http://localhost:7799/api/v2/clusters/meta/mycluster | jq '.cluster.nodes[].address'
"redis9:6379"
"redis10:6379"
"redis11:6379"
"redis12:6379"
```
Two of them are masters and the other two of them are replicas.

Let's scale up to 8 nodes:
```bash
# Add 4 nodes
$ curl -XPATCH -H 'Content-Type: application/json' http://localhost:7799/api/v2/clusters/nodes/mycluster -d '{"node_number": 4}'
# Start migrating the data
$ curl -XPOST http://localhost:7799/api/v2/clusters/migrations/mycluster
```

Now we have 4 server proxies:
```bash
$ redis-cli -h server_proxy5 -p 6005 -c
server_proxy5:6005> cluster nodes
mycluster___________d71bc00fbdddf89_____ server_proxy5:6005 myself,master - 0 0 12 connected 0-4095
mycluster___________8de73f9146386295____ server_proxy6:6006 master - 0 0 12 connected 8192-12287
mycluster___________be40fe317baf2cf7____ server_proxy2:6002 master - 0 0 12 connected 4096-8191
mycluster___________9434df4158f3c5a4____ server_proxy4:6004 master - 0 0 12 connected 12288-16383
```

and 8 nodes:
```bash
# List the nodes of the our "mycluster`:
$ curl -s http://localhost:7799/api/v2/clusters/meta/mycluster | jq '.cluster.nodes[].address'
"redis9:6379"
"redis10:6379"
"redis11:6379"
"redis12:6379"
"redis3:6379"
"redis4:6379"
"redis7:6379"
"redis8:6379"
```

#### Failover
If you shutdown any proxy, as long as the whole `undermoon` cluster has remaining proxies,
it will do the failover.
```bash
# List the proxies of the our "mycluster`:
$ curl -s http://localhost:7799/api/v2/clusters/meta/mycluster | jq '.cluster.nodes[].proxy_address' | uniq
"server_proxy5:6005"
"server_proxy6:6006"
"server_proxy2:6002"
"server_proxy4:6004"
```

Let's shutdown one of the proxies like `server_proxy5:6005` here.
```bash
$ docker ps | grep server_proxy5 | awk '{print $1}' | xargs docker kill
```

```bash
# server_proxy5 is replaced by server_proxy3
$ curl -s http://localhost:7799/api/v2/clusters/meta/mycluster | jq '.cluster.nodes[].proxy_address' | uniq
"server_proxy3:6003"
"server_proxy6:6006"
"server_proxy2:6002"
"server_proxy4:6004"
```

And we can remove the server_proxy3 from the `undermoon` cluster.
```bash
$ curl -XDELETE http://localhost:7799/api/v2/proxies/meta/server_proxy3:6003
```

## Documentation
## API
### Server-side Proxy Commands
Refer to [UMCTL command](./docs/meta_command.md).

### HTTP Broker API
Refer to [HTTP API documentation](./docs/broker_http_api.md).

## TODO
- Limit running commands, connections
- Statistics
