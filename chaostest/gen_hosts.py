import config

def print_hosts():
    redis_ports = config.DOCKER_COMPOSE_CONFIG['redis_ports']
    server_proxy_ports = config.DOCKER_COMPOSE_CONFIG['server_proxy_ports']
    redis_addresses = ['redis{}'.format(p) for p in redis_ports]
    server_proxy_addresses = ['server_proxy{}'.format(p) for p in server_proxy_ports]

    for addr in redis_addresses + server_proxy_addresses:
        print("127.0.0.1 {}".format(addr))

if __name__ == '__main__':
    print("# Put this in your /etc/hosts")
    print_hosts()
