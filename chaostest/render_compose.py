from jinja2 import Environment, FileSystemLoader

import config


def render_docker_compose(docker_compose_yml):
    env = Environment(loader=FileSystemLoader('./'))
    template = env.get_template('chaostest/test_stack.yml.j2')
    output = template.render(config.DOCKER_COMPOSE_CONFIG)
    with open(docker_compose_yml, 'w') as f:
        f.write(output)


render_docker_compose('chaostest/chaos-docker-compose.yml')
