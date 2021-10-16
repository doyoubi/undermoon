# Migration Benchmarking compared to Redis

## Redis Cluster Migration
Run a redis cluster with 2 nodes
```bash
./scripts/run_redis_cluster.sh
```

At first all slots are on the migrating redis node 7001.
```bash
redis-cli -p 7001 cluster nodes
migrating_redis_________________________ 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-16383
importing_redis_________________________ 127.0.0.1:7002@17002 master - 0 1634309831944 2 connected
```

Insert 1GB data (512 * 2097152) to 7001
```bash
redis-benchmark -h 127.0.0.1 -p 7001 -c 20 -P 32 -t set -d 512 -n 2097152 -r 100000000
```

Start to reshard:
```
start_time=$(date)
redis-cli --cluster reshard 127.0.0.1:7001 \
    --cluster-from migrating_redis_________________________ \
    --cluster-to importing_redis_________________________ \
    --cluster-slots 8192 \
    --cluster-yes \
    --cluster-pipeline 16
echo 'Start time:' ${start_time}
echo 'End time:' "$(date)"
```

## Undermoon Cluster Migration
Run a undermoon redis cluster with 2 nodes
```bash
./scripts/run_two_shards.sh
```

Insert 1GB data (512 * 2097152) to 7001 just same as redis cluster above
```bash
redis-benchmark -h 127.0.0.1 -p 7001 -c 20 -P 32 -t set -d 512 -n 2097152 -r 100000000
```

Start migration:
```bash
start_time=$(date)
./scripts/loop_migration_test.sh one_shot
echo 'Start time:' ${start_time}
echo 'End time:' "$(date)"
```
