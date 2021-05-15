# Setting Up Backup for Memory Broker

## Setting Up Replica for Memory Broker
Build the binaries:
```
$ cargo build
```

Run the replica
```
$ RUST_LOG=warp=info,undermoon=info,mem_broker=info UNDERMOON_ADDRESS=127.0.0.1:8899 UNDERMOON_META_FILENAME=metadata2 target/debug/mem_broker
```

Run the master Memory
```
$ RUST_LOG=warp=info,undermoon=info,mem_broker=info UNDERMOON_REPLICA_ADDRESSES=127.0.0.1:8899 UNDERMOON_SYNC_META_INTERVAL=3 target/debug/mem_broker
```

```
# Put some data to the master:
$ ./examples/mem-broker/init.sh

# Verify that on master:
curl localhost:7799/api/v3/metadata
...

# Verify tat on replica after 3 seconds:
curl localhost:7799/api/v3/metadata
...
# Replica should have the same data as master.
```

Note that when master failed,
the whole system will **NOT** automatically fail back to replica.
You need to do it by calling the API of coordinator.
During this time, the server proxies will still be able to process the requests
but the whole system can't scale and failover for server proxies
util the `Memory Broker` endpoint of coordinator is switched to the replica.

Let's say you have already run a coordinator:
```
$ RUST_LOG=undermoon=info,coordinator=info target/debug/coordinator conf/coordinator.toml
```

Then you can change master to replica by connecting to coordinator in Redis protocol
and changing the config.
```
# 6699 is the port of coordinators.
$ redis-cli -p 6699 CONFIG SET brokers 127.0.0.1:8899
```

The newest metadata of the master memory broker
has not been replicated to the replica memory broker and fail.
We can't recover the lost data but we can bump the metadata epoch
by collecting the epoch from all the recorded proxies
to recover the service.

So we also need to call this API after reconfiguring the coordinator.
```
$ curl -XPUT localhost:7799/api/v3/epoch/recovery
```
Now the system should be able to work again.
