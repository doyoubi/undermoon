![undermoon logo](docs/undermoon-logo.png)

# Undermoon [![Build Status](https://travis-ci.com/doyoubi/undermoon.svg?branch=master)](https://travis-ci.com/doyoubi/undermoon)
Aims to provide a server-side Redis proxy implementing Redis Cluster Protocol supporting multiple tenants and easy scaling.

This proxy is not limit to Redis. Any storage system implementing redis protocol can work with undermoon.

## Quick Tour Examples
Requirements:

- docker-compose
- redis-cli

Before run any example, run this command to build the basic `undermoon` docker image:
```bash
$ make docker-build-image
```

### (1) Multi-process Redis
This example will run a redis proxy with multiple redis processes behind the proxy. You can use it as a "multi-threaded" redis.

```bash
$ make docker-multi-redis
```

Then you can connect to redis:

```bash
# Note that we need to add '-a' to select our database.
$ redis-cli -a mydb -h 127.0.0.1 -p 5299 SET key value
```

### (2) Redis Cluster
This example will run a sharding redis cluster with similar protocol as the [official Redis Cluster](https://redis.io/topics/cluster-tutorial).

```bash
$ make docker-multi-redis
```

You also need to add the following records to the `/etc/hosts` to support the redirection.

```
# /etc/hosts
127.0.0.1 server_proxy1
127.0.0.1 server_proxy2
127.0.0.1 server_proxy3
```

Now you have a Redis Cluster with 3 nodes!

- server_proxy1:6001
- server_proxy2:6002
- server_proxy3:6003

Connect to any node above and enable cluster mode by adding '-c':
```bash
$ redis-cli -h server_proxy1 -p 6001 -a mydb -c

Warning: Using a password with '-a' option on the command line interface may not be safe.
127.0.0.1:6001> set a 1
-> Redirected to slot [15495] located at server_proxy3:6003
OK
server_proxy3:6003> set b 2
-> Redirected to slot [3300] located at server_proxy1:6001
OK
server_proxy1:6001> set c 3
-> Redirected to slot [7365] located at server_proxy2:6002
OK
server_proxy2:6002> cluster nodes
mydb________________server_proxy2:6002__ server_proxy2:6002 master - 0 0 1 connected 5462-10922
mydb________________server_proxy1:6001__ server_proxy1:6001 master - 0 0 1 connected 0-5461
mydb________________server_proxy3:6003__ server_proxy3:6003 master - 0 0 1 connected 10923-16383
```

### (3) Failover
The server-side proxy itself does not support failure detection and failover.
But this can be easy done by using the two meta data management commands:

- UMCTL SETDB
- UMCTL SETPEER

See the api docs later for details.
Here we will use a simple Python script utilizing these two commands to do the failover for us.
This script locates in `examples/failover/checker.py`.
Now lets run it with the Redis Cluster in section (2):

```bash
$ make docker-failover
```

Connect to `server_proxy2`.

```bash
$ redis-cli -h server_proxy2 -p 6002 -a mydb
  Warning: Using a password with '-a' option on the command line interface may not be safe.
  server_proxy2:6002> cluster nodes
  mydb________________server_proxy2:6002__ server_proxy2:6002 master - 0 0 1 connected 5462-10922
  mydb________________server_proxy1:6001__ server_proxy1:6001 master - 0 0 1 connected 0-5461
  mydb________________server_proxy3:6003__ server_proxy3:6003 master - 0 0 1 connected 10923-16383
  server_proxy2:6002> get b
  (error) MOVED 3300 server_proxy1:6001
```

`server_proxy1` is responsible for key `b`. Now kill the `server_proxy1`:

```bash
$ docker ps | grep server_proxy1 | awk '{print $1}' | xargs docker kill
```

5 seconds later, slots from `server_proxy1` was transferred to `server_proxy2`!

```bash
$ redis-cli -h server_proxy2 -p 6002 -a mydb
Warning: Using a password with '-a' option on the command line interface may not be safe.
server_proxy2:6002> cluster nodes
mydb________________server_proxy2:6002__ server_proxy2:6002 master - 0 0 1 connected 5462-10922 0-5461
mydb________________server_proxy3:6003__ server_proxy3:6003 master - 0 0 1 connected 10923-16383
server_proxy2:6002> get b
(nil)
```

But what if the `checker.py` fails?
What if we have multiple `checker.py` running, but they have different views about the liveness of nodes?

Well, this checker script is only for those who just want a simple solution.
For serious distributed systems, we should use some other robust solution.
This is where the `coordinator` comes in.

# Architecture
![architecture](docs/architecture.svg)

# Broker
Since rust does not have a good etcd v3 client. We use a [proxy service](https://github.com/doyoubi/overmoon) written in Golang as a broker.

# Initialize Server-side Proxy
```
> ./server_proxy  # runs on port 5299 and forward commands to 127.0.0.1:6379
> redis-cli -p 5299
127.0.0.1:5299> umctl setdb 1 noflags mydb 127.0.0.1:6379 0-8000
127.0.0.1:5299> umctl setpeer 1 noflags mydb 127.0.0.1:7000 8001-16383
127.0.0.1:5299> auth mydb
OK
127.0.0.1:5299> cluster nodes
mydb________________127.0.0.1:5299______ 127.0.0.1:5299 master - 0 0 1 connected 0-8000
mydb________________127.0.0.1:7000______ 127.0.0.1:7000 master - 0 0 1 connected 8001-16383
127.0.0.1:5299> get a
(error) MOVED 15495 127.0.0.1:7000
127.0.0.1:5299> set b 1
OK
```

## TODO
- ~~Basic proxy implementation~~ (done)
- ~~Multiple backend connections~~ (done)
- ~~Slot map and cluster map~~ (done)
- ~~Implement AUTH command to select database~~ (done)
- ~~Implement meta data manipulation api~~ (done)
- Implement coordinator (in progress)
- Implement Redis Replication Protocol
- Support slot migration via replication
- ~~Optimize RESP parsing~~ (done)
- Implement CLUSTER SLOTS
- Implement commands to get proxy meta
- Track spawned futures
- Simple script to push configuration to proxy for demonstration
- Batch write operations with interval flushing
- ~~Add configuration~~ (done)
- Limit running commands, connections
- Slow log
- Statistics
- Support multi-key commands
