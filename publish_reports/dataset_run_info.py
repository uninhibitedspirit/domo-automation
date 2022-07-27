import pandas as pd
import numpy as np
import os
import pathlib
import subprocess
import json
from common import helper
from datetime import datetime, timedelta
import requests
from functools import reduce
import logging


# ==== global vars
username = ""
password = ""
instances_ref = "instance_credentials.csv"
export_csv_ref = 'status_last_run_datasets.csv'
export_manual_csv_ref = 'manual_dataflows.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def get_all_dataflows_id(instance_id, session_token, **queryparams):
    search_q = "*"
    field = "data_flow_type"
    entities = ["DATAFLOW"]
    sort_field = "create_date"
    sort_order = "DESC"
    count=no_dataset
    offset=0
    lastrunmil =  datetime.now()
    lastrunmil = lastrunmil.timestamp() * 1000
    for arg in queryparams:
        if (arg == 'q'):
            search_q = queryparams.get(arg, search_q)
            search_q = "*" if search_q == '' else search_q

        if (arg == 'sort_field'):
            sort_field = queryparams.get(arg, sort_field)

        if (arg == 'sort_order'):
            sort_order = queryparams.get(arg, sort_order)

        if (arg == 'count'):
            count = queryparams.get(arg, count)

        if (arg == 'offset'):
            offset = queryparams.get(arg, offset)

        if (arg == 'lastrunmil'):
            lastrunmil = queryparams.get(arg, lastrunmil)
            lastrunmil = datetime.now() - timedelta(days=int(lastrunmil))
            lastrunmil = lastrunmil.timestamp() * 1000

    payload = {"entities": entities,
               "filters": [
                   {"filterType": "term", "field": field, "value": "REDSHIFT", "name": "REDSHIFT", "not": True},
                   {"filterType": "term", "field": field, "value": "ADR", "name": "ADRENALINE", "not": True},
                   {"field": "last_run_date", "filterType": "numeric", "longNumber": lastrunmil, "operator": "LT"}
               ], "combineResults": True, "query": search_q, "count": count,
               # keeping the count 500 assuming all the dataflows are included
               "offset": offset,
               "sort": {"isRelevance": False, "fieldSorts": [{"field": sort_field, "sortOrder": sort_order}]}
               }

    list_DF_API = "https://{}.domo.com/api/search/v1/query".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        df_list = [i['databaseId'] for i in j_ref['searchObjects']]
        return df_list
        logging.info('Successfully fetched all the dataflows')
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def fetch_each_dataflows(instance_id, session_token, dataflow_id):
    list_DF_API = "https://{}.domo.com/api/dataprocessing/v2/dataflows/{}".format(instance_id, dataflow_id)
    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.get(url=list_DF_API, headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        inputs = j_ref.get('inputs',False)
        print('instance_id = ', instance_id, ', dataflow_id = ', dataflow_id)
        inputs = [] if (inputs == False) else [i['executeFlowWhenUpdated'] for i in inputs]
        isManual = (reduce(lambda x,y: x or y, inputs, False) == False) & ('scheduleInfo' not in j_ref.keys())
        ds_list = {'instace_name': instance_id,
                    'name': j_ref['name'],
                    'lastUpdated': pd.to_datetime(np.int64(j_ref['lastExecution']['lastUpdated']), unit='ms'),
                    'state': j_ref['lastExecution'].get('state', None),
                    'failed': j_ref['lastExecution'].get('failed', None),
                    'isManual': isManual,
                    'id': j_ref['id']}
        sr = pd.Series(ds_list)
        return sr
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def fetch_all_dataflows(instance_id, session_token):
    offset=0
    loop_bool = True
    inst_df_id[instance_id] = []
    while loop_bool:
        df_list = get_all_dataflows_id(instance_id, session_token, count=no_dataset, offset=offset)
        inst_df_id[instance_id].extend(df_list)
        offset +=no_dataset
        loop_bool = not(len(df_list) < no_dataset)


# ===================== common =======================


# ======================================================================================================================
print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)

instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)


for i, instance in instance_info.iterrows():
    # login to the instance and get the token from helper.get_session_token
    session = helper.get_session_token(instance['instance_id'],
                                       instance['username'],
                                       instance['password'])

    # log error if login fails
    if type(session) != str and session[0] is None:
        logging.error(session[1])
        continue

    # fetch all the datasets
    fetch_all_dataflows(instance['instance_id'], session)
    inst_df_ids = inst_df_id[instance['instance_id']]
    for i in inst_df_ids:
        s = fetch_each_dataflows(instance['instance_id'], session, i)
        if s.isManual:
            export_data = export_data.append(s,ignore_index=True)
        # print(export_data.head())
        # print(export_data.info())

# export csv for all manual df
export_data.isManual = export_data.isManual.astype('bool')
export_data.failed = export_data.failed.astype('bool')
export_data.id = export_data.id.astype('int32')

# export_data_for_manual = export_data[export_data.isManual == True]
# export_data_for_manual = export_data_for_manual.drop(['isManual'], axis=1)
export_data_for_manual.to_csv(export_manual_csv_ref, index=False)

# export csv for all the dataflows not run for 3 days
last_run_date = datetime.now() - timedelta(days=int(3))
last_run_date = last_run_date.timestamp() * 1000
last_run_date = pd.to_datetime(last_run_date, unit='ms')

print('before export_data.shape == ', export_data.shape)
export_data = export_data[export_data.lastUpdated < last_run_date]
print('after export_data.shape == ', export_data.shape)
export_data.to_csv(export_csv_ref, index=False)
