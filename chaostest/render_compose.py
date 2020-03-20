import sys

from jinja2 import Environment, FileSystemLoader

import config


def render_docker_compose(docker_compose_yml, is_mem_broker):
    env = Environment(loader=FileSystemLoader('./'))
    if is_mem_broker:
        template = env.get_template('chaostest/test_stack_mem_broker.yml.j2')
    else:
        template = env.get_template('chaostest/test_stack.yml.j2')
    output = template.render(config.DOCKER_COMPOSE_CONFIG)
    with open(docker_compose_yml, 'w') as f:
        f.write(output)


if __name__ == '__main__':
    # Usage:
    #     python chaostest/render_compose.py [overmoon|mem_broker] [enable_failure]
    is_mem_broker = len(sys.argv) >= 2 and sys.argv[1] == 'mem_broker'
    enable_failure = len(sys.argv) >= 3 and sys.argv[2] == 'enable_failure'
    if not enable_failure:
        print("Disable fault injection")
        config.DOCKER_COMPOSE_CONFIG['pumba_commands'] = {}
    else:
        print("Enable fault injection")
    render_docker_compose('chaostest/chaos-docker-compose.yml', is_mem_broker)
