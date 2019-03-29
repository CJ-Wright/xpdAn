import copy

from xpdan.startup.tomo_server import run_server

from databroker import Broker
import yaml
dbs = {}
for yaml_file in ['raw', 'an']:
    with open(f'{yaml_file}.yml', 'r') as f:
        dbs[yaml_file] = Broker.from_config(yaml.load(f))

db = dbs['raw']
db.prepare_hook = lambda x, y: copy.deepcopy(y)

run_server(db=db)
