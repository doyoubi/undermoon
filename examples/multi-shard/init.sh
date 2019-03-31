#!/usr/bin/env bash

function wait_redis() {
    redis-cli -h $1 -p $2 ping
}

declare -a redis_list=('redis1' 'redis2' 'redis3')

for name in "${redis_list[@]}"; do
    until wait_redis ${name} 6379; do
        echo "waiting for ${name}"
        sleep 1
    done
done

declare -a proxy_list=('server_proxy1' 'server_proxy2' 'server_proxy3')

for i in "${!proxy_list[@]}"; do
    name=${proxy_list[${i}]}
    port=$((6001+${i}))
    until wait_redis ${name} ${port}; do
        echo "waiting for ${name}"
        sleep 1
    done
done

redis-cli -h server_proxy1 -p 6001 UMCTL SETDB 1 FORCE mydb redis1:6379 0-5461
redis-cli -h server_proxy2 -p 6002 UMCTL SETDB 1 FORCE mydb redis2:6379 5462-10922
redis-cli -h server_proxy3 -p 6003 UMCTL SETDB 1 FORCE mydb redis3:6379 10923-16383

redis-cli -h server_proxy1 -p 6001 UMCTL SETPEER 1 FORCE mydb server_proxy2:6002 5462-10922 mydb server_proxy3:6003 10923-16383
redis-cli -h server_proxy2 -p 6002 UMCTL SETPEER 1 FORCE mydb server_proxy1:6001 0-5461 mydb server_proxy3:6003 10923-16383
redis-cli -h server_proxy3 -p 6003 UMCTL SETPEER 1 FORCE mydb server_proxy1:6001 0-5461 mydb server_proxy2:6002 5462-10922
