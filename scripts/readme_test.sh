./examples/mem-broker/init.sh

curl -XPOST -H 'Content-Type: application/json' http://localhost:7799/api/v3/clusters/meta/mycluster -d '{"node_number": 4}'
curl -XPATCH -H 'Content-Type: application/json' http://localhost:7799/api/v3/clusters/nodes/mycluster -d '{"node_number": 4}'
curl -XPOST http://localhost:7799/api/v3/clusters/migrations/expand/mycluster

echo '\n###### Before failover'
curl -s http://localhost:7799/api/v3/clusters/meta/mycluster | jq '.cluster.nodes[].proxy_address' | uniq
echo '#######'

docker ps | grep server_proxy5 | awk '{print $1}' | xargs docker kill
sleep 3

echo '\n###### After failover'
curl -s http://localhost:7799/api/v3/clusters/meta/mycluster | jq '.cluster.nodes[].proxy_address' | uniq
echo '#######'

