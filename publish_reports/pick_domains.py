import pandas as pd
import numpy as np
import os
import pathlib
import subprocess
import json
import requests
import logging
from common import helper
from datetime import datetime
import re
import csv



# ==== global vars
regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
export_csv_ref = 'domain_list.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
dataset_limit = 10000
dataset_offset = 0
inst_df_id = {}
std_list = ['Users [U][1] [Latest Metadata]']
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def check(email):
  if(re.fullmatch(regex, email)):
    return True
  else:
    return False

def fetch_all_emails(instance, session, client, dataset_id, ds_name, owner):
  dataset_offset=0
  loop_bool = True
  while loop_bool:

    dataset_payload = {"querySource":"data_table","useCache":True,
    "query":{
      "columns":[{"exprType":"COLUMN","column":"Email"}],
      "limit":{"limit": dataset_limit,"offset": dataset_offset},
      "orderByColumns":[],"groupByColumns":[],
      "where":{"not":False,"exprType":"IN",
              "leftExpr":{"exprType":"COLUMN","column":"User Account Status"},
              "selectSet":[{"exprType":"STRING_VALUE","value":"active"}]},
              "having":None},
      "context":{"calendar":"StandardCalendar",
      "features":{"PerformTimeZoneConversion":True,"AllowNullValues":True,"TreatNumbersAsStrings":True}},"viewTemplate":None}

    df_list = helper.export_dataset(instance, session, dataset_payload, dataset_id)

    data = [{
              'domain': a[0][a[0].index('@') + 1 : ],
              'ds_name': ds_name,
              'client': client,
              'instance_id': instance,
              'owner_id': owner.get('id'),
              'owner_name': owner.get('name')
            }
            for a in df_list['rows']
              if a[0] !='' and a[0]!=None ]
    print('===============================emails===================== ',)
    # Creates DataFrame.

    export_data = pd.DataFrame(data)
    print("instance={} , client={}, dataset_id={}, ds_name={}, owner={}".format(instance, client, dataset_id, ds_name, owner))
    export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
    
    dataset_offset +=dataset_limit
    loop_bool = not(df_list['numRows'] < dataset_limit)
  
  def find_std(n, is_bool=False):
    ns = re.sub(r"^\d+", "", n) # removed all the number from the begining
    ns = re.sub(r"^\_+", "", ns) # removed _ from the begining
    ns = re.sub(r"^Glue_", "", ns) # removed Glue_ from the begining
    ns = re.sub(r"^Glue", "", ns) # removed Glue_ from the begining
    ns = ns.lstrip(' ')
    if(ns in std_list):
        return True if is_bool else ''
    else:
        print('search string ns = ',ns)
        return False if is_bool else ns

def fetch_all_ds(instance, session, client, current_user_id):
  offset=0

  payload = {
    "entities":["DATASET"],
    "filters":[
      {"filterType": "dateBucket", "field": "last_updated", "value": "LAST_DAY"},
      # {"filterType":"term","field":"owned_by_id","value":"{}".format(current_user_id),"not":False},
      {"field":"name_sort","filterType":"wildcard","query":"*{}*".format(std_list[0])}
    ],
    "combineResults":True,"query":"*",
    "count": no_dataset, "offset": offset,
    "sort":{"isRelevance":False,"fieldSorts":[{"field":"create_date","sortOrder":"DESC"}]}
  }

  ds = helper.fetch_datasets(instance, session, payload)
  print("found {} datasets in the instance {}".format(ds['_metaData']['totalCount'], instance))
  # print('dataSources == ',ds['dataSources'])
  for dataset in ds['dataSources']:
    print('ds.name = ', dataset.get('name',''))
    print('dataset id = ', dataset['id'])
    fetch_all_emails(instance, session, client, dataset['id'], dataset.get('name',''), 
    dataset.get('owner',{'id':'','name':''}))


# ===================== common ======================


# ======================================================================================================================
print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)


instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)

cols = [
        'domain',
        'ds_name',
        'client',
        'instance_id',
        'owner_id',
        'owner_name'
      ]
df = pd.DataFrame(columns=cols)

df.to_csv(export_csv_ref, index=False, quoting=csv.QUOTE_NONE)

for i, instance in instance_info.iterrows():
  # login to the instance and get the token from helper.get_session_token
  inst_id = instance['instance_id']
  auth = helper.get_session_token(inst_id,
                                      instance['username'],
                                      instance['password'], True)

  session = auth['sessionToken']

  # log error if login fails
  if type(session) != str and session[0] is None:
    logging.error(session[1])
    continue

  # get user counts
  client = instance['client']

  fetch_all_ds(inst_id, session, client, auth['userId'])