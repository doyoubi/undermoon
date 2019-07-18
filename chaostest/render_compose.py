from jinja2 import Environment, FileSystemLoader


REDIS_NUM = 12
REDIS_PORT_RANGE_START = 6000
REDIS_PORT_RANGE_END = REDIS_PORT_RANGE_START + REDIS_NUM

SERVER_PROXY_NUM = REDIS_NUM // 2
SERVER_PROXY_RANGE_START = 7000
SERVER_PROXY_RANGE_END = SERVER_PROXY_RANGE_START + SERVER_PROXY_NUM

COORDINATOR_NUM = 3


config = {
    'redis_maxmemory': '100MB',
    'redis_services': list(range(REDIS_PORT_RANGE_START, REDIS_PORT_RANGE_END)),
    'server_proxy_services': list(range(SERVER_PROXY_RANGE_START, SERVER_PROXY_RANGE_END)),
    'coordinator_num': COORDINATOR_NUM,
    'overmoon_port': 7799,
    'overmoon_address': 'overmoon:7799',
    'etcd_port': 2379,
}


def render_docker_compose(docker_compose_yml):
    env = Environment(loader=FileSystemLoader('./'))
    template = env.get_template('chaostest/test_stack.yml.j2')
    output = template.render(config)
    with open(docker_compose_yml, 'w') as f:
        f.write(output)


render_docker_compose('chaostest/chaos-docker-compose.yml')
