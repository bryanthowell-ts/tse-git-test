import os
import requests.exceptions
import json
import time

from thoughtspot_rest_api_v1 import *

gh_action_none = "{None}"
#
# Values passed into ENV from Workflow file, using GitHub Secrets and Workflow Variables
#
server = os.environ.get('TS_SERVER') 
username = os.environ.get('TS_USERNAME')
secret_key = os.environ.get('TS_SECRET_KEY')
org_name = os.environ.get('TS_ORG_NAME')

object_type = os.environ.get('OBJECT_TYPE')
object_filename = os.environ.get('OBJECT_FILENAME')

# Define the directory names to link to the workflow 
directories_for_objects = {
    "CONNECTION": ["connections"],
    "DATA_MODEL": ["tables", "models", "sql_views", "views"],
    "TABLE": ["tables"],
    "MODEL": ["models"],
    "LIVEBOARD": ["liveboards"],
    "ANSWER" : ["answers"],
    "CONTENT": ["liveboards", "answers"]
}

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

# Read the directories for the objects specified
# We will build an upload with ALL of them, and let ThoughtSpot use the 'etags'

# If filename listed, upload just that file
if object_filename != gh_action_none:
    # Assume everything is named {obj_id}.{obj_type}.tml
    try: 
        with open(file=object_filename, mode='r') as f:
            tml_str= f.read()
    except:
        pass