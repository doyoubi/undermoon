# Test Migration Locally
We can set up two redis servers and server proxies to test migration locally.

## Simple Tests
### Run them all
```
./scripts/run_two_shards.sh
```

The logs will be inside `./local_tests`.

### Write Some Data to Source Redis
Here we use redis 7001 and server proxy 6001 as the source part(the part migrating out the slots).

```
for i in {0..50}; do redis-cli -p 7001 set $i $i; done
```

### Start Migration
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

### Commit Migration
```
redis-cli -p 6001 umctl setcluster v2 3 noflags mydb \
    127.0.0.1:7001 1 0-8000 PEER 127.0.0.1:6002 1 8001-16383
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

## Keep Running Migration
```
./scripts/run_two_shards.sh
```

Run this in another terminal:
```
./scripts/loop_migration_test.sh
```

The second script will keep moving the slots between 2 server proxies.

Then we can use [checker](https://github.com/doyoubi/undermoon-operator/tree/master/checker)
to see whether there's any data consistency problem.
