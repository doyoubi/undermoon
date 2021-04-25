# Test Migration Locally
We can set up two redis servers and server proxies to test migration locally.

## Run them all
Run them in 4 different shell session:
```
redis-server --port 7001
```

```
redis-server --port 7002
```

```
UNDERMOON_ADDRESS=127.0.0.1:6001 UNDERMOON_ANNOUNCE_ADDRESS=127.0.0.1:6001 RUST_LOG=undermoon=debug,server_proxy=debug \
    target/debug/server_proxy conf/server-proxy.toml
```

```
UNDERMOON_ADDRESS=127.0.0.1:6002 UNDERMOON_ANNOUNCE_ADDRESS=127.0.0.1:6002 RUST_LOG=undermoon=debug,server_proxy=debug \
    target/debug/server_proxy conf/server-proxy.toml
```

## Write Some Data to Source Redis
Here we use redis 7001 and server proxy 6001 as the source part(the part migrating out the slots).

```
for i in {0..50}; do redis-cli -p 7001 set $i $i; done
```

## Start Migration
```
redis-cli -p 6001 UMCTL SETCLUSTER v2 2 NOFLAGS mydb \
    127.0.0.1:7001 1 0-8000 \
    127.0.0.1:7001 migrating 1 8001-16383 2 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002 \
    PEER 127.0.0.1:6002 importing 1 8001-16383 2 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002
redis-cli -p 6002 UMCTL SETCLUSTER v2 2 NOFLAGS mydb \
    127.0.0.1:7002 importing 1 8001-16383 2 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002 \
    PEER 127.0.0.1:6001 1 0-8000 \
    127.0.0.1:6001 migrating 1 8001-16383 2 127.0.0.1:6001 127.0.0.1:7001 127.0.0.1:6002 127.0.0.1:7002
```

Read the logs to see whether there's any unexpected error.

Check the data of both redis
```
redis-cli -p 7001 keys '*'
echo '====================='
redis-cli -p 7002 keys '*'
```

## Commit Migration
```
redis-cli -p 6001 umctl setcluster v2 3 noflags mydb \
    127.0.0.1:7001 1 0-8000 PEER 127.0.0.1:7002 1 8001-16383
redis-cli -p 6002 umctl setcluster v2 3 noflags mydb \
    127.0.0.1:7002 1 8001-16383 PEER 127.0.0.1:6001 1 0-8000
```

Check the logs again.

Check the data of both redis again
```
redis-cli -p 7001 keys '*'
echo '====================='
redis-cli -p 7002 keys '*'
```
