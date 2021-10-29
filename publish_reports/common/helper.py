import json
import logging
import requests
import numpy as np


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


def get_all_dataflows(instance_id, session_token, **queryparams):
    search_q = "*"
    field = "data_flow_type"
    entities = ["DATAFLOW"]
    sort_field = "create_date"
    sort_order = "DESC"
    for arg in queryparams:
        if (arg == 'q'):
            search_q = queryparams.get(arg, search_q)
            search_q = "*" if search_q == '' else search_q

        if (arg == 'sort_field'):
            sort_field = queryparams.get(arg, sort_field)

        if (arg == 'sort_order'):
            sort_order = queryparams.get(arg, sort_order)

    payload = {"entities": entities,
               "filters": [
                   {"filterType": "term", "field": field, "value": "REDSHIFT", "name": "REDSHIFT", "not": True},
                   {"filterType": "term", "field": field, "value": "ADR", "name": "ADRENALINE", "not": True}
               ], "combineResults": True, "query": search_q, "count": 500,
               # keeping the count 500 assuming all the dataflows are included
               "offset": 0,
               "sort": {"isRelevance": False, "fieldSorts": [{"field": sort_field, "sortOrder": sort_order}]}
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
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []


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
        ds_list = [{'name': i['name'], 'id': i['id']} for i in j_ref['dataSources'] if i['name']]

        return np.array(ds_list)

        logging.info('Successfully fetched all the dataflows')
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []


def get_all_df_details(instance_id, session_token, dataflow_id, match_str=[], exclude_str=[]):
    list_DF_API = "https://{}.domo.com/api/dataprocessing/v2/dataflows/{}".format(instance_id, dataflow_id)
    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.get(url=list_DF_API, headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return np.array([{'name': i['dataSourceName'], 'id': i['dataSourceId']} for i in filter(lambda x: get_exact_dataset(x,match_str,exclude_str,'dataSourceName'),j_ref['outputs'] )])
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []


def get_exact_dataset(ds, match_strs, exclude_str, key_name):
    should_include = True
    for arg in match_strs:
        if arg.lower() in ds[key_name].lower():
            should_include = np.logical_and(should_include, True)
        else:
            should_include = np.logical_and(should_include, False)

    for arg in exclude_str:
        if arg.lower() in ds[key_name].lower():
            should_include = np.logical_and(should_include, False)

    return should_include
