import os
import requests.exceptions
import csv
import json

from thoughtspot_rest_api_v1 import *

#
# Values passed into ENV from Workflow file, using GitHub Secrets and Workflow Variables
#
server = os.environ.get('TS_SERVER') 
username = os.environ.get('TS_USERNAME')
secret_key = os.environ.get('TS_SECRET_KEY')
org_name = os.environ.get('TS_ORG_NAME')
author_filter = os.environ.get('AUTHOR_FILTER')
tag_filter = os.environ.get('TAG_FILTER')
record_size = os.environ.get('RECORD_SIZE_LIMIT')
object_type = os.environ.get('OBJECT_TYPE')

# full_access_token = os.environ.get('TS_TOKEN')  #  Tokens are tied to a particular Org, so useful in an environment with only a few Orgs but not single-tenant

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

# Request for LIVEBOARDS
lb_search_request = {
    "metadata": [
    {
      "type": "LIVEBOARD"
    }
  ],
  "sort_options": {
    "field_name": "CREATED",
    "order": "DESC"
  },
    "record_size" : -1,
    "record_offset": 0
}

# Request for ANSWERS
answer_search_request = {
    "metadata": [
    {
      "type": "ANSWER"
    }
  ],
  "sort_options": {
    "field_name": "CREATED",
    "order": "DESC"
  },
    "record_size" : -1,
    "record_offset": 0
}

# Request for Data Objects (Tables, Models, etc.)
# Not differentiated in request, all are "LOGICAL_TABLE
data_object_search_request = {
    "metadata": [
    {
      "type": "LOGICAL_TABLE"
    }
  ],
  "sort_options": {
    "field_name": "CREATED",
    "order": "DESC"
  },
    "record_size" : -1,
    "record_offset": 0
}

obj_type_select = {
    'LIVEBOARD' : liveboard_search_request,
    'ANSWER' : answer_search_request,
    'DATA' : data_object_search_request
}

def retrieve_objects(request, record_size_override=-1): 
    # Add filters if passed from workflow
    if author_filter != "{None}":
        search_request["created_by_user_identifiers"] = [author_filter]
    
    if tag_filter != "{None}":
        search_request["tag_identifiers"] = [tag_filter]

    print("Requesting object listing")
    try:
        objs = ts.metadata_search(request=request)
    except requests.exceptions.HTTPError as e:
        print(e)
        print(e.response.content)
        exit()

    print("{} objects retrieved".format(len(tables)))
    return objs

def export_objects_to_disk(objects):
    for o in objects:
        export_tml_with_obj_id(guid=o["metadata_id"], save_to_disk=True)

def download_objects():
    if object_type == 'ALL':
        for type in obj_type_select:
            objs = retrieve_objects(request=obj_type_select[type], record_size_override=record_size)
            export_objects_to_disk(objects=objs)
            print("Finished bringing all {} objects to disk".format(type)
    else:
        # Only if valid value
        if obj_type in obj_type_select:
            objs = retrieve_objects(request=obj_type_select[type], record_size_override=record_size)
            export_objects_to_disk(objects=objs)
            print("Finished bringing all {} objects to disk".format(type)



