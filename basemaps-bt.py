import argparse
import datetime
import requests
import os
import mercantile
from google.cloud import bigtable
from google.cloud.bigtable import column_family
from google.cloud.bigtable import row_filters

URL = 'https://tiles.basemaps.cartocdn.com/vectortiles/carto.streets/v1/{z}/{x}/{y}.mvt'
TABLE_NAME = 'alasarr_tiles'

Z = 11
X_MIN = 550
X_MAX = 650
Y_MIN = 700
Y_MAX = 800
MAX_BT_MUTATIONS = 1000

def tile_row(table, z, x, y):
    url = URL.format(z=z, x=x, y=y)
    response = requests.get(url)
    
    column = 'greeting'.encode()
    row_key = mercantile.quadkey(x, y, z).encode()
    row = table.row(row_key)
    row.set_cell('default', 'mvt', response.content, timestamp=datetime.datetime.utcnow())
    return row

def create_table(table):
    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'default'
    column_families = {column_family_id: max_versions_rule}
    if not table.exists():
        table.create(column_families=column_families)
    else:
        print("Table {} already exists.".format(table_id))

def tilefy(table):
    rows = []
    for x in range(X_MIN, X_MAX):
        for y in range(Y_MIN, Y_MAX):
            rows.append(tile_row(table,Z, x, y))
            if len(rows) == MAX_BT_MUTATIONS -1:
                print(f'Writing rows {MAX_BT_MUTATIONS}')
                table.mutate_rows(rows)
                rows = []
    
    if len(rows) > 0 :
        table.mutate_rows(rows)

current_path = os.path.dirname(os.path.realpath(__file__))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(current_path, 'credentials.json')

parser = argparse.ArgumentParser(description='Boundaries data loader')
parser.add_argument('--project', required=True, type=str, help='Project ID')
parser.add_argument('--instance', required=True, type=str, help='Instance ID')

args = parser.parse_args()

# Instantiate a client.
client = bigtable.Client(project=args.project, admin=True)

# Get an instance by ID.
instance = client.instance(args.instance)

# Get a Cloud Spanner database by ID.
table = instance.table(TABLE_NAME)

# create_table(table)
# save_tile(table, Z, X_MIN, Y_MIN)
tilefy(table)