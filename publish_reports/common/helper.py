import json
import logging
import requests
import numpy as np


def get_session_token(domo_instance, email, password, full_response=False):
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
            if full_response:
                return resp
            else:
                return resp['sessionToken']
    else:
        token_error_string = 'Token request ended up with status code {}'.format(auth_status)
        # logging.error(token_error_string)
        # logging.error(auth_response.text)
        # raise Exception(token_error_string)
        # print(token_error_string)
        # print(auth_response.text)
        return (None, token_error_string)


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


def get_all_users(instance_id, session_token, offset, filter=[]):

    payload = {"showCount":True,
               "count":False,
               "includeDeleted":False,
               "includeSupport":False,
               "limit":200,
               "offset":offset,
               "sort":{"field":"displayName","order":"ASC"},
               "filters": filter,
               "attributes":["id","roleId","department","title","employeeId","employeeNumber","created","lastActivity","displayName"]}

    list_DF_API = "https://{}.domo.com/api/identity/v1/users/search".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        return [{'name': i['displayName'], 'id': i['id'], 'email': i['emailAddress']} for i in j_ref['users']]
        logging.info('Successfully fetched all the dataflows')
    else:
        error = "There was error in fetching users from instance id: '{}' with status code:{}".format(instance_id,cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)


def change_password(instance_id, session_token, domoUserId, password, name, email):
    payload = {"domoUserId": domoUserId, "password": password}

    list_DF_API = "https://{}.domo.com/api/identity/v1/password".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.put(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code

    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        if(j_ref['success']):
            msg = "Successfully changed the password instance id: '{}' for user_id: {}, name: {}, email: {}".format(
                instance_id, domoUserId, name, email)
            print(msg)
            logging.info(msg)
        else:
            error = j_ref['description'] + " instance id: '{}' for user_id: {}, name: {}, email: {}".format(
                instance_id, domoUserId, name, email)
            logging.error(error)
            print(error)
    else:
        error = "There was error changing dataflows from instance id: '{}' for user: {}, email: {} with status code:{}".format(instance_id,domoUserId,email,cards_status)
        logging.error(error)
        print(error)
        logging.error(df_response.text)

def add_user(instance_id, session_token, new_user):

    list_DF_API = "https://{}.domo.com/api/content/v3/users".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(new_user), headers=cards_headers)
    cards_status = df_response.status_code
    j_ref = json.loads(df_response.text)
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        msg = "{} with emai: '{}' has been successfully added to the instance id: '{}' as '{}'".format(
            new_user['displayName'], new_user["detail"]["email"],instance_id, j_ref['role'])
        print(msg)
        logging.info(msg)
    else:
        error = j_ref.get('EMAIL_DUPLICATE', 'Issue adding user!') + " instance id: '{}', name: '{}', role: {}, email: {}".format(
            instance_id, new_user['displayName'], new_user["roleId"], new_user["detail"]["email"])
        logging.error(error)
        print(error)
        logging.error(df_response.text)

def delete_user(instance_id, session_token, user_details):
    list_DF_API = "https://{}.domo.com/api/identity/v1/users/{}".format(instance_id, user_details['id'])

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}

    df_response = requests.delete(url=list_DF_API, headers=cards_headers)
    cards_status = df_response.status_code
    print("{} - {} - {} : card status {} - instance:{}".format(user_details['id'], user_details['email'], user_details['name'], cards_status, instance_id))
    print('df_response.text = ', df_response.text)
    print('df_response.status_code = ', df_response.status_code)
    print('df_response.url = ', df_response.url)
    print('df_response.encoding = ', df_response.encoding)
    print('df_response.content = ', df_response.content)
    if cards_status == 200:
        resp = df_response.json() if df_response.text!='' else {}
        print('resp = ', resp)

        token_error_string = 'User deleted successfully! name: {}, id: {}, email:{}'.format(user_details['name'], user_details['id'], user_details['email'])
        logging.info(token_error_string)
        if resp.get('success', True) is False:
            token_error_string = 'name: {}, id: {}, email:{}'.format(user_details.name, user_details.id, user_details.email)
            return (None, token_error_string)
        else:
            logging.info(token_error_string)
            return token_error_string
    else:
        error = "There was error in deleting name: {}, id: {}, email:{} with status code:{} from instance:{}".format(
            user_details['name'], user_details['id'], user_details['email'], cards_status, instance_id)
        logging.error(error)
        logging.error(df_response.text)
        return error
        # raise Exception(error)


def get_cards_list(instance_id, session_token, payload, limit=100, skip=0):
    API_URL = "https://{}.domo.com/api/content/v2/cards/adminsummary?limit={}&skip={}".format(instance_id, limit, skip)

    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}

    df_response = requests.post(url=API_URL, data=json.dumps(payload), headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        return j_ref
        logging.info('Successfully fetched users')
    else:
        error = "There was error in fetching cards from instance id: '{}' with status code:{} limit: {}, skip: {}".format(instance_id,status, limit, skip)
        logging.error(error)
        logging.error(df_response.text)
        # raise Exception(error)



def get_users(instance_id, session_token, payload):
    API_URL = "https://{}.domo.com/api/identity/v1/users/search".format(instance_id)

    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}

    df_response = requests.post(url=API_URL, data=json.dumps(payload), headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        return j_ref
        logging.info('Successfully fetched users')
    else:
        error = "There was error in fetching users from instance id: '{}' with status code:{}".format(instance_id,status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)

def get_card_details(instance_id, session_token, query):
    API_URL = "https://{}.domo.com/api/content/v1/cards?{}".format(instance_id,query)

    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}

    df_response = requests.get(url=API_URL, headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        return j_ref
        logging.info('Successfully fetched users')
    else:
        error = "There was error in fetching users from instance id: '{}' with status code:{}".format(instance_id,status)
        logging.error(error)
        logging.error(df_response.text)

def fetch_datasets(instance_id, session_token, payload):
    list_DF_API = "https://{}.domo.com/api/data/ui/v3/datasources/search".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code
    if cards_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return j_ref
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def each_datasets_cards(instance_id, session_token, dataset_id):
    API_URL = "https://{}.domo.com/api/content/v1/datasources/{}/cards?drill=true".format(instance_id,dataset_id)

    headers = {'Content-Type': 'application/json',
               'x-domo-authentication': session_token}

    df_response = requests.get(url=API_URL, headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched cards info')
        return j_ref
    else:
        error = "There was error in fetching cards info of dataset:{} from instance id: '{}' with status code:{}".format(
        dataset_id, instance_id, status)
        logging.error(error)
        logging.error(df_response.text)

def get_cards_users(instance_id, session_token, query):
    API_URL = "https://{}.domo.com/api/content/v3/users?{}".format(instance_id, query)

    headers = {'Content-Type': 'application/json',
               'x-domo-authentication': session_token}

    df_response = requests.get(url=API_URL, headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched cards info')
        return j_ref
    else:
        error = "There was error in fetching cards info of dataset:{} from instance id: '{}' with status code:{}".format(
        dataset_id, instance_id, status)
        logging.error(error)
        logging.error(df_response.text)

def get_roles(instance_id, session_token):
    API_URL = "https://{}.domo.com/api/authorization/v1/roles".format(instance_id)

    headers = {'Content-Type': 'application/json',
               'x-domo-authentication': session_token}

    df_response = requests.get(url=API_URL, headers=headers)
    status = df_response.status_code
    if status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched cards info')
        return j_ref
    else:
        error = "There was error in fetching cards info of dataset:{} from instance id: '{}' with status code:{}".format(
        dataset_id, instance_id, status)
        logging.error(error)
        logging.error(df_response.text)

def add_to_group(instance_id, session_token, group_id, user_id, email):
    payload = [{"groupId":group_id,"addMembers":[{"type":"USER","id":user_id}]}]

    list_DF_API = "https://{}.domo.com/api/content/v2/groups/access".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.put(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    cards_status = df_response.status_code

    if cards_status == 200:
        msg = "Successfully added email: '{}' for user_id: {} to group_id: {} for instance:'{}'".format(
            email, user_id, group_id, instance_id)
        # print(msg)
        # logging.info(msg)
    else:
        error = "There was error adding user from instance id: '{}' to group_id: '{}', user_id: {}, email: {} with status code:{}".format(instance_id, group_id, user_id, email, cards_status)
        logging.error(error)
        print(error)
        # logging.error(df_response.text)

def get_dataflows(instance_id, session_token, payload):
    list_DF_API = "https://{}.domo.com/api/search/v1/query".format(instance_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API, data=json.dumps(payload), headers=cards_headers)
    resp_status = df_response.status_code
    if resp_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return j_ref
    else:
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                          resp_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def export_dataset(instance_id, session_token, payload, dataset_id):
    list_DF_API = "https://{}.domo.com/api/query/v1/execute/{}".format(instance_id, dataset_id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.post(url=list_DF_API,  data=json.dumps(payload),headers=cards_headers)
    resp_status = df_response.status_code
    if resp_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return j_ref
    else:
        error = "There was error in downloading csv instance id: '{}' with status code:{}".format(instance_id, resp_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def get_pdp_details(instance_id, session_token, payload, dataset_id):
    list_DF_API = "https://{}.domo.com/api/query/v1/data-control/{}/filter-groups?{}".format(instance_id, dataset_id,payload)
    
    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.get(url=list_DF_API, headers=headers)
    resp_status = df_response.status_code
    if resp_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return j_ref
    else:
        error = "There was error in downloading csv instance id: '{}' with status code:{}".format(instance_id, resp_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []

def get_users_with_id(instance_id, session_token, payload):
    list_DF_API = "https://{}.domo.com/users/index?{}".format(instance_id,payload)
    
    headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    df_response = requests.get(url=list_DF_API, headers=headers)
    resp_status = df_response.status_code
    if resp_status == 200:
        j_ref = json.loads(df_response.text)
        logging.info('Successfully fetched all the dataflows')
        return j_ref
    else:
        error = "There was error in downloading csv instance id: '{}' with status code:{}".format(instance_id, resp_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)
        return []