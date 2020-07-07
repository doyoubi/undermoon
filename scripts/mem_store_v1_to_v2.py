import sys
import json


def upgrade_store_to_v2(v1_json):
    if v1_json['version'] != 'mem-broker-0.1':
        raise Exception('unexpected version: {}'.format(v1_json['version']))
    new_json = v1_json
    new_json['version'] = 'mem-broker-0.2'
    new_json['enable_ordered_proxy'] = False
    for proxy_resource in new_json['all_proxies'].values():
        proxy_resource['index'] = 0
    return new_json


def main():
    if len(sys.argv) != 3:
        print('Usage: python mem_store_v1_to_v2.py src_meta_file dst_meta_file')
        return

    src_file = sys.argv[1]
    dst_file = sys.argv[2]

    with open(src_file, 'r') as src:
        content = src.read()
        src_json = json.loads(content)

    with open(dst_file, 'w') as dst:
        dst.write(json.dumps(upgrade_store_to_v2(src_json)))


if __name__ == '__main__':
    main()
