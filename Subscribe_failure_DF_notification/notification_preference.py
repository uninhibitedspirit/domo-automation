import pandas as pd
import requests
import logging
import json

# ==========
username, password = '',''
# ===========


# ============
# referred from domo_helper.py
def get_session_token(domo_instance, email, password):
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
        if(resp['success'] is False):
            token_error_string = "Failed to login to the instance : {} ,  reason: {}".format(domo_instance, resp['reason'])
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

def get_all_dataflows(instance_id, session_token):

    payload = {"entities":["DATAFLOW"],
               "filters":[
                   {"filterType":"term","field":"data_flow_type","value":"REDSHIFT","name":"REDSHIFT","not":True},
                   {"filterType":"term","field":"data_flow_type","value":"ADR","name":"ADRENALINE","not":True}
               ],"combineResults":True,"query":"*","count":500,#keeping the count 500 assuming all the dataflows are included
               "offset":0,"sort":{"isRelevance":False,"fieldSorts":[{"field":"create_date","sortOrder":"DESC"}]}
               }

    list_DF_API = "https://{}.domo.com/api/search/v1/query".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        return [{'name': i['name'], 'id': i['databaseId']} for i in j_ref['searchObjects']]
        logging.info('Successfully fetched all the dataflows')
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)

def subscribe_for_notifications(instance_id, dataflow_id, dataflow_name, session_token):
    subscription_api = "https://{}.domo.com/api/dataprocessing/v1/dataflows/{}/subscription".format(
        instance_id, dataflow_id)

    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    print("Switching on the notifications for instance id: {}, dataflow_id: {}, dataflow_name: '{}'".format(instance_id,
                                                                                                            dataflow_id,
                                                                                                            dataflow_name))
    resp = requests.post(url=subscription_api, headers=headers)
    cards_status = resp.status_code
    if cards_status == 200:
        print("Subscribed!!! you will be notified when instance id: {}, dataflow_id: {}, dataflow_name: '{}' fails".format(instance_id,dataflow_id,dataflow_name))
        logging.info("Subscribed!!! you will be notified when instance id: {}, dataflow_id: {}, dataflow_name: '{}' fails".format(instance_id,dataflow_id,dataflow_name))
    else:
        error = "Failed to subscribe to instance id: {}, dataflow_id: {}, dataflow_name: '{}' status code {}".format(instance_id,dataflow_id,dataflow_name,cards_status)
        logging.error(error)
        logging.error(resp.text)
        raise Exception(error)
# ======================================================================================================================


all_instances = pd.concat(
    map(pd.read_csv, ['instances_to_subscribe_keerthana.csv',
                      'instances_to_subscribe_roland.csv',
                      'instances_to_subscribe_srini.csv',
                      'instances_to_subscribe_prachi.csv']), ignore_index=True)

all_instances[['username']] = all_instances[['username']].fillna(value=username)
all_instances[['password']] = all_instances[['password']].fillna(value=password)
for i, instance_info in all_instances.iterrows():
    inst_id = instance_info['instance_id']
    inst_username = instance_info['username']
    inst_password = instance_info['password']
    session_token = get_session_token(inst_id, inst_username, inst_password)
    if type(session_token) != str and session_token[0] is None:
        logging.error(session_token[1])
        continue

    dfs = get_all_dataflows(inst_id, session_token)
    print("==================start for {}=================".format(inst_id))
    for df in dfs:
        subscribe_for_notifications(inst_id, df['id'],df['name'], session_token)
    print("================end for {}===================".format(inst_id))