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

export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
inst_id = "edcast-524"
customer="Chanel(cc)"
# 
# dataset_name = "Evaluation_Programs_ConsolidatedMaster"
# dataset_id = "8de71efb-af96-434c-958d-e194efb4628e"
# 
# 
# dataset_name = "EvaluationPrograms_Master"
# dataset_id = "5242c432-3861-4818-a61f-3531070b442e"
# 
# 
dataset_name = "PDP - Restricted 100072_Glue_Channels [CH][2]"
dataset_id = "a50199af-6f79-4666-91b9-f45960bdd360"

export_csv_ref = "pdp_filter_{}_{}_{}.csv".format(customer,inst_id, dataset_name)
payload="options=load_associations,include_open_policy,load_filters,sort"
# ==== global vars

#


cols = ['customer',
'instance_id',
'dataset_id',
'dataset_name',
'user_id',
'user_email',
'user_displayName',
'pdp_name',
'pdp_filter_group_id',
'pdp_data_source_permission',
'pdp_value',
'pdp_applied_on',
'pdp_filter_name']
df = pd.DataFrame(columns=cols)

df.to_csv(export_csv_ref, index=False, quoting=csv.QUOTE_NONE)



#

# login to the instance and get the token from helper.get_session_token
session = helper.get_session_token(inst_id,
                                    username,
                                    password)

if type(session) != str and session[0] is None:
    logging.error(session[1])

pdp_list = helper.get_pdp_details(inst_id, session, payload, dataset_id)
# user_id for sublist in user_ids for user_id in sublist
# print('pdp_list = ', pdp_list)

def fetch_all_users(instance, session, customer, group_id):
    dataset_offset=0
    dataset_limit = 10
    loop_bool = True
    users = []
    while loop_bool:
        payload = "ascending=true&group={}&limit={}&offset={}".format(group_id,dataset_limit,dataset_offset)

        df_list = helper.get_users_from_group_id(instance, session, payload)
        # print('total - ', df_list['groupUserCount'])

        users = users + [i['userId'] for i in df_list['groupUserList']]
        # print('current count - ', len(users))

        dataset_offset +=dataset_limit
        loop_bool = len(users) < df_list['groupUserCount']

    return users

def get_user_ids(i):
    if 'groupIds' in i.keys() and (len(i['groupIds']) > 0):
        print('======== groupIds ========', i['groupIds'])
        print('======== pdp name ========', i['name'])
        all_grp_usr_ids = []
        for g in i['groupIds']:
            users = fetch_all_users(inst_id, session, customer, g)
            all_grp_usr_ids = all_grp_usr_ids + users

        if 'userIds' in i.keys() and (len(i['userIds']) > 0):
            print('======== groupIds with userIds ========')
            all_grp_usr_ids = all_grp_usr_ids + i['userIds']

        new_pdp_record = i
        new_pdp_record['userIds'] = all_grp_usr_ids
        return new_pdp_record
    else:
        return i


pdp_list = [ get_user_ids(i) for i in pdp_list] # manupulate pdp_list by adding users from the group mentioned
user_ids = [ i['userIds'] for i in pdp_list]
user_ids=[user_id for sublist in user_ids for user_id in sublist]
user_ids={str(i) for i in user_ids}
user_details = helper.get_users_with_id(inst_id, session, "cvUserIds={}".format(','.join(user_ids)))


user_details = {i['userId']:{'email':i['emailAddress'], 'name':i['displayName'], 'user_id':i['userId']} for i in user_details}

def export_csv(customer='', inst_id='', dataset_id='',
                dataset_name='',i=0, u_details='',
                pdp_record='',params=''):
    data = [{
                'customer': customer,
                'instance_id': inst_id,
                'dataset_id': dataset_id,
                'dataset_name': dataset_name,
                'user_id':i,
                'user_email': u_details['email'],
                'user_displayName': u_details['name'],
                'pdp_name': pdp_record['name'],
                'pdp_filter_group_id': pdp_record['filterGroupId'],
                'pdp_data_source_permission': pdp_record['dataSourcePermissions'],
                'pdp_value': params.get('value',''),
                'pdp_applied_on': params.get('type',''),
                'pdp_filter_name': params.get('name','')
            }]
    # print('===')
    # print(data)
    # print('===')
    export_data = pd.DataFrame(data)
    export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
# print(user_details)
for pdp_record in pdp_list:
    print('====')
    print(pdp_record)
    print('====')
    for i in pdp_record['userIds']:
        u_details = user_details[i]
        # print(pdp_record['parameters'])
        if 'parameters' in pdp_record.keys():
            params = pdp_record['parameters']
            for val in params:
                
                # print(params)
                
                # data = [{
                #             'customer': customer,
                #             'instance_id': inst_id,
                #             'dataset_id': dataset_id,
                #             'dataset_name': dataset_name,
                #             'user_id':i,
                #             'user_email': u_details['email'],
                #             'user_displayName': u_details['name'],
                #             'pdp_name': pdp_record['name'],
                #             'pdp_filter_group_id': pdp_record['filterGroupId'],
                #             'pdp_data_source_permission': pdp_record['dataSourcePermissions'],
                #             'pdp_value': val,
                #             'pdp_applied_on': params['type'],
                #             'pdp_filter_name': params['name']
                #         }]
                # # print('===')
                # # print(data)
                # # print('===')
                # export_data = pd.DataFrame(data)
                # export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
                export_csv(customer=customer, inst_id=inst_id, dataset_id=dataset_id,
                dataset_name=dataset_name,i=i, u_details=u_details,
                pdp_record=pdp_record,params=val)
        else:
            export_csv(customer=customer, inst_id=inst_id, dataset_id=dataset_id,
                dataset_name=dataset_name,i=i, u_details=u_details,
                pdp_record=pdp_record,params={})