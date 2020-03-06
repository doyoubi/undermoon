import sys

from jinja2 import Environment, FileSystemLoader

import config


def render_docker_compose(docker_compose_yml):
    env = Environment(loader=FileSystemLoader('./'))
    template = env.get_template('chaostest/test_stack.yml.j2')
    output = template.render(config.DOCKER_COMPOSE_CONFIG)
    with open(docker_compose_yml, 'w') as f:
        f.write(output)


if __name__ == '__main__':
    # Usage:
    #   Without Fault Injection
    #     python chaostest/render_compose.py
    #   With Fault Injection
    #     python chaostest/render_compose.py enable_failure
    enable_failure = len(sys.argv) >= 2 and sys.argv[1] == 'enable_failure'
    if not enable_failure:
        print("Disable fault injection")
        config.DOCKER_COMPOSE_CONFIG['pumba_commands'] = {}
    else:
        print("Enable fault injection")
    render_docker_compose('chaostest/chaos-docker-compose.yml')
