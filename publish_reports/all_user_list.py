import pandas as pd
import numpy as np
import os
import pathlib
import subprocess
import json
from common import helper
from datetime import datetime
import time
import csv

# ==== global vars
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
export_csv_ref = 'honeywell_user_list.csv'
no_cards = 30
occ = "=" * 30
inst_dfs = {}
no_dataset = 30
export_data = pd.DataFrame()
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def get_all_users(instance, session, client, roles):
  print('its here')
  offset=0
  loop_bool = True
  while loop_bool:
    user_count_payload = {"showCount": True, "count": True, "includeDeleted": False, "includeSupport": False,
                        "limit": no_dataset, "offset": offset, "sort": {"field": "displayName", "order": "ASC"},
                        "filters": [], "attributes": []}

    user_badge = helper.get_users(instance, session, user_count_payload)
    # ['attributes', 'id', 'displayName','roleId', 'emailAddress']
    for i in user_badge['users']:
      print(i)
      email = i['emailAddress'].lower()
      # if not('@csod' in email or '@edcast' in email):
      role_name = [j['name'] for j in roles if j['id'] == i['roleId']]
      data = [{'id': i['id'], 'displayName': i.get("displayName", ''), 
              'roleId':i['roleId'],'role_name': role_name[0], 'email': email, 'client': client,
              'instance_id': instance}]
    
      # Creates DataFrame.
      export_data = pd.DataFrame(data)
      export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
    
    offset +=no_dataset
    loop_bool = not(len(user_badge['users']) < no_dataset)

# ====================

instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)



df = pd.DataFrame(columns=['id', 'displayName', 'roleId','role_name', 'email', 'client', 'instance_id'])
df.to_csv(export_csv_ref,  index=False, line_terminator='\n')

for i, instance in instance_info.iterrows():
  # login to the instance and get the token from helper.get_session_token
  inst_id = instance['instance_id']
  session = helper.get_session_token(inst_id,
                                      instance['username'],
                                      instance['password'])

  # log error if login fails
  if type(session) != str and session[0] is None:
    logging.error(session[1])
    continue

  roles = helper.get_roles(inst_id, session)
  print({j['id']:j['name'] for j in roles })
  # get user counts
  client = instance['client']
  get_all_users(inst_id, session, client, roles )
  print('added users for instance id: {}, client:{}'.format(inst_id, client))
  