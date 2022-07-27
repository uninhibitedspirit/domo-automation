import pandas as pd
import requests
import logging
import json
from common import helper

def get_all_users(instance_id, session_token, offset):

    payload = {"showCount":True,
               "count":False,
               "includeDeleted":False,
               "includeSupport":False,
               "limit":200,
               "offset":offset,
               "sort":{"field":"displayName","order":"ASC"},
               "filters":[],
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
        error = "There was error in fetching dataflows from instance id: '{}' with status code:{}".format(instance_id,cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)

def delete_user(instance_id, session_token, user_details):


    list_DF_API = "https://{}.domo.com/api/identity/v1/users/{}".format(instance_id, user_details.id)

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}

    df_response = requests.delete(url=list_DF_API, headers=cards_headers)
    cards_status = df_response.status_code
    print("{} - {} - {} : card status {}".format(user_details.id, user_details.email, user_details.name, cards_status))
    # resp = df_response.json()
    if cards_status == 200:
        token_error_string = 'name: {}, id: {}, email:{}'.format(user_details.name, user_details.id, user_details.email)
        logging.info(token_error_string)
        # if (resp['success'] is False):
        #     token_error_string = 'name: {}, id: {}, email:{}'.format(user_details.name, user_details.id, user_details.email)
        #     return (None, token_error_string)
        # else:
        #     logging.info('Session token acquired.')
        #     return resp['sessionToken']
    else:
        error = "There was error in deleting name: {}, id: {}, email:{} with status code:{}".format(user_details.name, user_details.id, user_details.email, cards_status)
        logging.error(error)
        logging.error(df_response.text)
        raise Exception(error)

delete_list = pd.read_excel('NGC people list.xlsx')
maintain_access = [
    'My Learning Experience Support',
    'Kristen Englert',
    'Danelle Koster',
    'Diana Latino',
    'Charnetta Williams',
    'Shrey Thakkar',
    'Bridget Crawford',
    'Marissa Duncan',
    'Carrie Marsh',
    'Terez Madden',
    'GBSD Skills Development',
    'Technical Development Paths',
    'Jim Durbano',
    'April Isabelle',
    'Milton Chen',
    'Amanda Muller',
    'Jonathan Dyer'
]
maintain_access_email = delete_list.email[delete_list.email.apply(lambda x: '@gmail.com' in x)] # email domain reference
print('before = ', len(delete_list))
delete_list = delete_list[-(delete_list.email.isin(maintain_access_email))]
print('after = ', len(delete_list))
delete_list = delete_list[-(delete_list.displayName.isin(maintain_access))]
print('after1 = ', len(delete_list))
# =========================
inst_id = "" # instance domain name meaning the one in curly braces = {domainname}.domo.com
inst_username = ""
inst_password = ""
session_token = helper.get_session_token(inst_id, inst_username, inst_password)

list_of_users = get_all_users(inst_id, session_token, 0)
list_of_users = list_of_users + get_all_users(inst_id, session_token, 200)
list_of_users = list_of_users + get_all_users(inst_id, session_token, 400)


df = pd.DataFrame.from_dict(list_of_users)
# print(df.head())
df = df[-(df.name.isin(maintain_access))]
df = df[-(df.email.isin(maintain_access_email))]

# print(len(df))
# print(len(delete_list))

for i, instance_info in df.iterrows():
    delete_user(inst_id, session_token, instance_info)