REDIS_NUM = 12
REDIS_PORT_RANGE_START = 6000
REDIS_PORT_RANGE_END = REDIS_PORT_RANGE_START + REDIS_NUM

SERVER_PROXY_NUM = REDIS_NUM // 2
SERVER_PROXY_RANGE_START = 7000
SERVER_PROXY_RANGE_END = SERVER_PROXY_RANGE_START + SERVER_PROXY_NUM

COORDINATOR_NUM = 3


DOCKER_COMPOSE_CONFIG = {
    'redis_maxmemory': '100MB',
    'redis_ports': list(range(REDIS_PORT_RANGE_START, REDIS_PORT_RANGE_END)),
    'server_proxy_ports': list(range(SERVER_PROXY_RANGE_START, SERVER_PROXY_RANGE_END)),
    'coordinator_num': COORDINATOR_NUM,
    'overmoon_port': 7799,
    'overmoon_address': 'overmoon:7799',
    'etcd_port': 2379,
    'pumba_commands': {
        'kill': "--random --interval 120s kill 're2:(server_proxy|coordinator|overmoon).*'",
        'delay': "--random --interval 20s netem --duration 5s delay 're2:(server_proxy|coordinator|overmoon).*'",
        'loss': "--random --interval 20s netem --duration 5s loss 're2:(server_proxy|coordinator|overmoon).*'",
        'rate': "--random --interval 20s netem --duration 5s rate 're2:(server_proxy|coordinator|overmoon).*'",
        'duplicate': "--random --interval 20s netem --duration 5s duplicate 're2:(server_proxy|coordinator|overmoon).*'",
        'corrupt': "--random --interval 20s netem --duration 5s corrupt 're2:(server_proxy|coordinator|overmoon).*'",
    },
}
