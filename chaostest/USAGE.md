# Chaos Testing

## Set Up

- Install Python 3
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop)

#### (1) Install Python Dependencies
```
pip install -r chaostest/requirements.txt
```

#### (2) Generate docker-compose.yaml
```
python chaostest/render_compose.py
```

#### (3) Build Docker Images

Build `overmoon`:
```
git clone https://github.com/doyoubi/overmoon
make build-docker
```

Build `undermoon`:
```
make docker-rebuild-bin
```

Start Docker Swarm:
```
docker swarm init
```

Add the following config in `/etc/hosts`.
```
127.0.0.1 server_proxy7000
127.0.0.1 server_proxy7001
127.0.0.1 server_proxy7002
127.0.0.1 server_proxy7003
127.0.0.1 server_proxy7004
127.0.0.1 server_proxy7005
127.0.0.1 server_proxy7006
127.0.0.1 server_proxy7007
127.0.0.1 server_proxy7008
127.0.0.1 server_proxy7009
127.0.0.1 server_proxy7010
127.0.0.1 server_proxy7011
127.0.0.1 server_proxy7012
127.0.0.1 server_proxy7013
127.0.0.1 server_proxy7014
127.0.0.1 server_proxy7015
127.0.0.1 server_proxy7016
127.0.0.1 server_proxy7017
127.0.0.1 server_proxy7018
127.0.0.1 server_proxy7019
127.0.0.1 server_proxy7020
127.0.0.1 server_proxy7021
127.0.0.1 server_proxy7022
127.0.0.1 server_proxy7023
```

## Run Test

Deploy our service `chaos`:
```
docker stack deploy --compose-file chaostest/chaos-docker-compose.yml chaos
```

List services:
```
docker stack services chaos
```

Run the command above, you can see some services got killed occasionally.

Run the test script. It will randomly create cluster, remove cluster, and start migration.
```
python chaostest/random_test.py
```

Stop the docker-compose:
```
docker stack rm chaos
```

## Debuging

You can use `monitor_all_redis.py` to debug what commands are running on all the Redis.
