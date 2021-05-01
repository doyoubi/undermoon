import config

def print_hosts():
    server_proxy_num = config.DOCKER_COMPOSE_CONFIG['server_proxy_num']
    redis_addresses = ['redis{}'.format(p) for p in range(server_proxy_num * 2)]
    server_proxy_addresses = ['server_proxy{}'.format(p) for p in range(server_proxy_num)]

    for addr in redis_addresses + server_proxy_addresses:
        print("127.0.0.1 {}".format(addr))

if __name__ == '__main__':
    print("# Put this in your /etc/hosts")
    print_hosts()
