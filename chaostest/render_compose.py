import sys
import argparse

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
    #     python chaostest/render_compose.py -t [overmoon|mem_broker] [-f] [-a]
    parser = argparse.ArgumentParser(description='Render docker-compose file for chaos testing')

    parser.add_argument('-t', action='store', dest='test_type',default='mem_broker')
    parser.add_argument('-f', action='store_true', dest="enable_failure_injection", default=False)
    parser.add_argument('-a', action="store_true", dest="active_redirection", default=False)

    results = parser.parse_args()

    is_mem_broker = results.test_type == 'mem_broker'
    enable_failure = results.enable_failure_injection
    active_redirection = results.active_redirection

    if not enable_failure:
        print("Disable fault injection")
        config.DOCKER_COMPOSE_CONFIG['pumba_commands'] = {}
    else:
        print("Enable fault injection")

    if active_redirection:
        config.DOCKER_COMPOSE_CONFIG['active_redirection'] = True
        print("Enable active redirection")
    else:
        config.DOCKER_COMPOSE_CONFIG['active_redirection'] = False
        print("Disable active redirection")

    render_docker_compose('chaostest/chaos-docker-compose.yml', is_mem_broker)
