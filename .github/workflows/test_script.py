import os
import requests.exceptions
import csv
import json

from thoughtspot_rest_api_v1 import *

# Example functions for reporting of various counts of objects
# Useful for reporting various users / groups / domains / orgs for contractual compliance

#
server = os.environ.get('TS_SERVER')       # or type in yourself
print(server)
exit()
# Supply access token from REST API Playground or provide username/password securely
full_access_token = os.environ.get('TS_TOKEN')


ts: TSRestApiV2 = TSRestApiV2(server_url=server)
if full_access_token != "":
    ts.bearer_token = full_access_token

# Get all Tables
search_request = {
    'metadata': {'type': 'LOGICAL_TABLE'},
    'record_offset': 0,
    'record_size': 10  # default is 10
}
tables = ts.metadata_search(request=search_request)

print(json.dumps(tables, indent=2))

