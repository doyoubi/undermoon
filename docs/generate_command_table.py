import json
import redis


COMMAND_TABLE_FILE = './docs/command_table.json'
MARKDOWN_TABLE_FILE = './docs/command_table.md'


def get_existing_command_table():
    try:
        with open(COMMAND_TABLE_FILE, 'r') as f:
            content = f.read()
            return json.loads(content)
    except IOError:
        return {}


def get_commands_from_redis():
    ''' Use COMMAND to get all the commands from Redis
    '''
    client = redis.StrictRedis()
    commands = client.execute_command("COMMAND")
    return [cmd[0] for cmd in commands]


def generate_markdown(table):
    headers = []
    headers.append('| COMMAND | SUPPORTED | DESCRIPTION |')
    headers.append('|---|---|---|')

    lines = []
    for cmd, fields in table.items():
        lines.append('| {} | {} | {} |'.format(cmd, fields['supported'], fields['desc']))
    lines.sort()

    return '\n'.join(headers + lines)


# Need to run a Redis locally to retrieve the commands.
if __name__ == '__main__':
    table = get_existing_command_table()
    for cmd in get_commands_from_redis():
        if cmd in table:
            continue
        table[cmd] = {
            'supported': False,
            'desc': ''
        }

    content = json.dumps(table, indent=4, sort_keys=True)
    with open(COMMAND_TABLE_FILE, 'w') as f:
        f.write(content)

    with open(MARKDOWN_TABLE_FILE, 'w') as f:
        f.write(generate_markdown(table))
