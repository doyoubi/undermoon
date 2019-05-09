#!/usr/bin/env bash
random_time=$(date +%s)

for i in {0..9}
do
    redis-cli -h server_proxy1 -p 6001 -a mydb -c SET "test:${random_time}:${i}" "test:${random_time}:${i}"
done

function test_value() {
    for i in {0..9}
    do
        key="test:${random_time}:${i}"
        value=$(redis-cli -h server_proxy2 -p 6002 -a mydb -c GET ${key})
        [ "$value" != "$key" ] && echo "wrong value ${value}, expecting ${key}"
    done
}

echo '############################'
echo 'testing after setting values'
echo '############################'
test_value

docker ps | grep 'server_proxy1' | awk '{print $1}' | xargs docker kill

echo '############################'
echo 'waiting for failover'
echo '############################'
redis-cli -h server_proxy2 -p 6002 -a mydb -c cluster nodes
sleep 6
redis-cli -h server_proxy2 -p 6002 -a mydb -c cluster nodes
echo '############################'
echo 'testing after killing proxy1'
echo '############################'
test_value

docker-compose -f examples/docker-compose-coordinator.yml start server_proxy1
echo '############################'
echo 'waiting for recover'
echo '############################'
sleep 6
redis-cli -h server_proxy2 -p 6002 -a mydb -c cluster nodes

echo '############################'
echo 'testing after restarting proxy1'
echo '############################'
test_value
