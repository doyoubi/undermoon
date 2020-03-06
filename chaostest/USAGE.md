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
docker-build-image
```

Or rebuild if the source codes are changed:
```
make docker-rebuild-bin
```

Start Docker Swarm:
```
docker swarm init
```

Config the `/etc/hosts`.
```
python chaostest/gen_hosts.py >> /etc/hosts

# You might need sudo
sudo sh -c 'python chaostest/gen_hosts.py >> /etc/hosts'
```

## Run Test

Deploy our service `chaos`:
```
make start-func-test
```

List services:
```
make list-chaos-services
```

Run the test script. It will randomly create cluster, remove cluster, and start migration.
```
make func-test
```
There will not be any fault injection. Any error log indicates there are bugs in the codes!
Upon error it will stop immediately.

stop-chaos:
```
docker stack rm chaos
```

## Run Test with Fault Injection
Or Deploy our service `chaos` with fault injection:
```
make start-chaos
```
In this case, any error should be able to recover.

List services:
```
make list-chaos-services
```

Run the command above, you can see some services got killed occasionally.

Run the test script. It will randomly create cluster, remove cluster, and start migration.
```
make chaos-test
```
Upon error it will not stop. But the error should be able to recover soon.

stop-chaos:
```
docker stack rm chaos
```

## Debugging

You can use `monitor_all_redis.py` to debug what commands are running on all the Redis.
```
python chaostest/monitor_all_redis.py
```
