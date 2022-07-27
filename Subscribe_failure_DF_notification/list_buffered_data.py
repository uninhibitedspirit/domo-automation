import pandas as pd
import os
import pathlib
import subprocess
import json
import requests
import logging
# from common import helper
from datetime import datetime
# === Notes ===
# compatible for many to many reports only if all the parent instances datasets are the same
# get data source(get_data_sets from domo_helper) and page id(check riley's video admin -> pages ) dynamically
#
#
# === Notes ===

# ==== global vars
username = "** some user email **"
password = "** some password **"
instance_to_publish_csv = "ds_is_list_of_instances_to_publish.csv"
occ = "=" * 30
list_of_all_inst_ds = []
# ==== global vars


all_instances = pd.concat(
    map(pd.read_csv, [
                    'instances_to_subscribe_1.csv',
                    'instances_to_subscribe_2.csv',
                    'instances_to_subscribe_3.csv',
                    'instances_to_subscribe_4.csv',
                    'instances_to_subscribe_5.csv',
                    'instances_to_subscribe_6.csv',
                    'instances_to_subscribe_7.csv'
                      ]), ignore_index=True)
# 518 instance removed from roland

all_instances[['username']] = all_instances[['username']].fillna(value=username)
all_instances[['password']] = all_instances[['password']].fillna(value=password)


def get_session_token(domo_instance, email, password):
    print('domo_instance ==', domo_instance)
    auth_api = 'https://{}.domo.com/api/content/v2/authentication'.format(domo_instance)
    auth_body = json.dumps({
        "method": "password",
        "emailAddress": email,
        "password": password
    })
    auth_headers = {'Content-Type': 'application/json'}
    auth_response = requests.post(auth_api, data=auth_body, headers=auth_headers)
    auth_status = auth_response.status_code
    resp = auth_response.json()
    if auth_status == 200:
        if (resp['success'] is False):
            token_error_string = "Failed to login to the instance : {} ,  reason: {}".format(domo_instance,
                                                                                             resp['reason'])
            return (None, token_error_string)
        else:
            logging.info('Session token acquired.')
            return resp['sessionToken']
    else:
        token_error_string = 'Token request ended up with status code {}'.format(auth_status)
        logging.error(token_error_string)
        logging.error(auth_response.text)
        raise Exception(token_error_string)
        return None


def get_all_datasets(instance_id, session_token, **queryparams):
    search_q = "*"
    entities = ["DATASET"]
    sort_field = "create_date"
    sort_order = "DESC"
    match_strs = []
    for arg in queryparams:
        if (arg == 'search_q'):
            search_q = queryparams.get(arg, search_q)
            search_q = "*" if search_q == '' else "*{}*".format(search_q)

        if (arg == 'sort_field'):
            sort_field = queryparams.get(arg, sort_field)

        if (arg == 'sort_order'):
            sort_order = queryparams.get(arg, sort_order)

        if (arg == 'match_strs'):
            match_strs = queryparams.get(arg, match_strs)


    payload = {"entities": entities,
               "filters": [{"field": "name_sort", "filterType": "wildcard", "query": search_q}],
               "combineResults": True,
               "query": "*",
               "count": 30,
               "offset": 0,
               "sort": {"isRelevance": False,
                        "fieldSorts": [{"field": sort_field, "sortOrder": sort_order}]}
               }

    list_DF_API = "https://{}.domo.com/api/data/ui/v3/datasources/search".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        ds_list = [{'instace_name': instance_id,'name': i['name'], 'rowCount': i['rowCount'], 'columnCount': i['columnCount'], 'id': i['id']} for i in j_ref['dataSources'] if i['name']]
        df = pd.DataFrame.from_dict(ds_list)
        df.head()
        return df

        logging.info('Successfully fetched all the dataflows')
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []




# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def update_with_new_value(report_json, key_name, value):
    report_json_str = json.dumps(report_json)
    val_index = report_json_str.find(key_name) + len(key_name) + 4
    old_key_val = report_json_str[val_index:]
    old_key_val = old_key_val[:old_key_val.find('"')]

    report_json_str = report_json_str.replace(old_key_val, value)
    return report_json_str

# ===================== common =======================

# ================== Please note that all the reports should be of the same datasets =============

for index, instance_info in all_instances.iterrows():
    session = get_session_token(instance_info['instance_id'],
                                       instance_info['username'],
                                       instance_info['password'])

    ds_list = get_all_datasets(instance_info['instance_id'], session, search_q='Buffered')
    # print("ds_list == ", ds_list)
    list_of_all_inst_ds.append(ds_list)
print(occ + " Done fetching all the datasets ids " + occ)
# ================== Create blank report of each parent reports for all the child instances =============


all_dataset_info = pd.concat(list_of_all_inst_ds)
print(all_dataset_info.head())
all_dataset_info.to_csv('file_name.csv', index=False)