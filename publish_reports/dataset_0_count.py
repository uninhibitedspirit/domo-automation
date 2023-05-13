import pandas as pd
import numpy as np
import os
import pathlib
import logging
from common import helper
from datetime import datetime
import csv

# ==== global vars
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
export_csv_ref = 'dataset_0_count.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
# ==== global vars

# ===================== common ======================
def relative_path():
	return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def fetch_all_ds(instance, session, client):
  offset=0
  loop_bool = True
  while loop_bool:

    payload = {
    	"entities":["DATASET"],
    	"filters":[
    		{"filterType":"numeric","field":"rows","longNumber":"0","not":False,"operator":"EQ"}],
    	"combineResults":True,"query":"*",
  		"count":no_dataset,"offset":offset,
  		"sort":{
  			"isRelevance":False,
  			"fieldSorts":[{"field":"create_date","sortOrder":"DESC"}]
  		}
    }

    df_list = helper.fetch_datasets(instance, session, payload)
    datasource = df_list['dataSources']

    for i in datasource:
      owner = i.get("owner", {'id':'', 'name':'', 'type': ''})
      
      data = [{'id': i['id'], 'datasetName': i.get("name", ''),'owner_id': owner.get("id", ''), 
              'owner_name': owner.get("name", ''),'owner_type': owner.get("type", ''),
              'rowCount': i['rowCount'], 'client': client, 'instance_id': instance, 
              'lastUpdated': pd.to_datetime(np.int64(i['lastUpdated']), unit='ms')
              }]
    
      # Creates DataFrame.
      export_data = pd.DataFrame(data)
      export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
    
    offset +=no_dataset
    loop_bool = not(len(datasource) < no_dataset)


# ======================================================================================================================

print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)


instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)

df = pd.DataFrame(columns=['id', 'datasetName', 'owner_id','owner_name','owner_type', 'rowCount', 'client', 'instance_id', 'lastUpdated'])

df.to_csv(export_csv_ref,  index=False, quoting=csv.QUOTE_NONE)

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

  # get user counts
  client = instance['client']

  fetch_all_ds(inst_id, session, client)