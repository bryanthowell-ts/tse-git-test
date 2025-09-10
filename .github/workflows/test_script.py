import os
import requests.exceptions
import csv
import json

from thoughtspot_rest_api_v1 import *

server = os.environ.get('TS_SERVER') # Passed into ENV from Workflow file, using GitHub secrets
# full_access_token = os.environ.get('TS_TOKEN') # Passed into ENV from Workflow file, using GitHub secrets
username = os.environ.get('TS_USERNAME')
secret_key = os.environ.get('TS_SECRET_KEY')
org_name = os.environ.get('ORG_NAME')

ts: TSRestApiV2 = TSRestApiV2(server_url=server)
# if full_access_token != "":
#    ts.bearer_token = full_access_token

# First get Org_Id: 0 to request orgs list
try:
    auth_token_response = ts.auth_token_full(username=username, secret_key=secret_key,
                                               validity_time_in_sec=3000, org_id=0)
    ts.bearer_token = auth_token_response['token']
except requests.exceptions.HTTPError as e:
    print(e)
    print(e.response.content)
    exit()

# Get token for the specified org_name
try:
    print("Searching for org_id for {}".format(org_name))
    org_search_req = {
        "org_identifier": org_name
    }
    search_resp = ts.orgs_search(request=org_search_req)
except requests.exceptions.HTTPError as e:
    print(e)
    print(e.response.content)
    exit()

if len(search_resp) == 1:
    org_id = search_resp[0]['id']
    print("org_id {} found".format(org_id))
    try:
        auth_resp = ts.auth_token_full(username=username, secret_key=secret_key,
                                       validity_time_in_sec=3000, org_id=org_id)
        ts.bearer_token = auth_resp['token']
    except requests.exceptions.HTTPError as e:
        print(e)
        print(e.response.content)
        exit()

# Wrapper of Export TML of a single item, with lookup via GUID or obj_id, and saving to disk with
# standard naming pattern
def export_tml_with_obj_id(guid:Optional[str] = None,
                           obj_id: Optional[str] = None,
                           save_to_disk=True):
    # Example of metadata search using obj_identifier (the property may be updated?)
    if obj_id is not None:
        search_req = {
            "metadata": (
                {'obj_identifier': obj_id}
            ),
            "sort_options": {
                "field_name": "CREATED",
                "order": "DESC"
            }
        }

        tables = ts.metadata_search(request=search_req)
        if len(tables) == 1:
            guid = tables[0]['metadata_id']
            obj_id = tables[0]['metadata_header']['objId']

        # print(json.dumps(log_tables, indent=2))

    if guid is None:
        raise Exception()

    # export_options allow shifting TML export to obj_id, without any guid references
    exp_opt = {
        "include_obj_id_ref": True,
        "include_guid": False,
        "include_obj_id": True
    }

    yaml_tml = ts.metadata_tml_export(metadata_ids=[guid], edoc_format='YAML',
                                      export_options=exp_opt)

    # Get obj_id from the TML
    lines = yaml_tml[0]['edoc'].splitlines()
    if obj_id is None:
        if lines[0].find('obj_id: ') != -1:
            obj_id = lines[0].replace('obj_id: ', "")

    obj_type = lines[1].replace(":", "")

    if save_to_disk is True:
        print(yaml_tml[0]['edoc'])
        print("-------")

        # Save the file with {obj_id}.{type}.{tml}
        filename = "{}s/{}.{}.tml".format(obj_type, obj_id, obj_type)
        with open(file=filename, mode='w') as f:
            f.write(yaml_tml[0]['edoc'])

    return yaml_tml


# Get Liveboards
search_request = {
    "metadata": [
    {
      "type": "LIVEBOARD"
    }
  ],
  "sort_options": {
    "field_name": "CREATED",
    "order": "DESC"
  },
  "record_size" : 5,
    "record_offset": 0
}

print("Requesting object listing")
try:
    tables = ts.metadata_search(request=search_request)
except requests.exceptions.HTTPError as e:
    print(e)
    print(e.response.content)
    exit()

print("{} objects retrieved".format(len(tables)))

for t in tables:
    export_tml_with_obj_id(guid=t["metadata_id"], save_to_disk=True)

print("Finished bringing all objects to disk")

