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




# ==== global vars
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
std_etls_list = 'std_etls_list.csv'
export_csv_ref = 'custom_etls.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
std_list = [
 'Channel [CH][0] [Core Event Data - Buffered]',
 'Channel [U][0] [Core Event Data - Buffered]',
 'Groups [G][0] [Core Event Data - Buffered]',
 'Searches [SE][0] [Core Event Data - Buffered]',
 'UserCards [U][0] [Core Event Data - Buffered]',
 'Content [C][1] [Latest Metadata]',
 'Users [U][1] [Latest Metadata]',
 'Groups [G][1] [Latest Metadata]',
 'User Roles [U][1]',
 'Searches [SE][1] [Search User Details]',
 'Groups [G][2] [Group Users]',
 'Users [U][2] [LXP Roles]',
 'User_Custom_Fields_Dim_M[0]',
 'Users [U][0] [Non-Active Accounts]',
 'Channel_cards_dim_M[0]',
 'Card_Pack_Relation_Dim[0]',
 'Daily Calendar',
 'Skills_Users [U][0]',
 'Profiles_dim_m[0]',
 'Journey_pack_relations[0]',
 'Users_dim_m[0]',
 'Users [U][3] [Total Registered Users By Month]',
 'Structures_dim[0]',
 'Structured_Items_Fact[0]',
 'Groups[G][0] Group_Assignments_Performance_i',
 'Content [C][2] [Structure Metadata]',
 'Badgings_Dim [0]',
 'Skills_ Users [U][1]',
 'CLC [2.0]',
 'Cards_dim[0]',
 'Users [U][0] [Custom Metadata]',
 'Users[U][0] User_Assignments_Performance_i',
 'UCC_Merged_Buffered [Temp]',
 'LeaderBoard_Data',
 'Users [U][3][User Funnel Data Explorer]',
 'Content [C][0][Training Cards Latest Metadata]',
 'Structures_S[0]',
    
# ==from custom fields update list==
 'Users [U][2] [User Performance Data Explorer]',
 'Channels [CH][2] [Channel Performance Data Explorer]',
 'Users [U][3] [Overall Onboarding Status]',
 'Searches [SE][1] [Search User Details]',
 'Users [U][2] [Skills & Learning Goals]',
 'Users [U][2] [Assignment Status]',
 'Users [U][2] [Pathway & Journey Progression Data Explorer]',
 'Users [U][3] [Structured Content Performance Data Explorer]',
 'Users [U][3] [Structured Pathway Progression Data Explorer]',
 'Groups [G][3] [Assignment Status]',
 'Users [U][3] [Pathway & Journey Progression Overview]',
 'Users [U][4] [Structured Pathway & Journey Progression Overview]',
 'Users [U][2] [Quiz & Poll Responses]',
 'Quiz[U][2][Data Explorer]',
 'Users [U][1] [Project Cards]',
 'Users [U][1] [Training Cards]',
 'Users [U][2] [MKP Registration]',
 'Users [U][3] [Structured Journey Progression Data Explorer]',
 'Users [U][2][Scorm Data Explorer]',
 'User_content_Completions_dim[0]',
 'User[U][3] Adoption_Details']
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def fetch_all_ds(instance, session, client):
  offset=0
  loop_bool = True
  while loop_bool:
    payload = {
      "entities":["DATAFLOW"],
      "filters":[{"filterType":"term","field":"data_flow_type","value":"REDSHIFT","name":"REDSHIFT","not":True}],
      "combineResults":True,"query":"*",
      "count":no_dataset,"offset":offset,
      "sort":{"isRelevance":False,"fieldSorts":[{"field":"create_date","sortOrder":"DESC"}]}
    }

    df_list = helper.get_dataflows(instance, session, payload)
    dataflows = df_list['searchObjects']

    for i in dataflows:
      data = [{
        'abandoned': i['abandoned'], 
        'customer': i['customer'], 
        'dataFlowType': i['dataFlowType'], 
        'databaseId': i['databaseId'],
        'inputCount': i['inputCount'], 
        'name': i['name'], 
        'outputCount': i['outputCount'], 
        'ownedById': i['ownedById'], 
        'ownedByName': i['ownedByName'], 
        'ownedByType': i['ownedByType'], 
        'paused': i['paused'], 
        'runCount': i['runCount'], 
        'status': i['status'], 
        'winnerText': i['winnerText'],
        'lastModified': pd.to_datetime(np.int64(i['lastModified']), unit='ms'),
        'client': client, 
        'instance_id': instance,
      }]
    
      # Creates DataFrame.
      
      export_data = pd.DataFrame(data)
      export_data['filter_out'] = c_list.name.apply(lambda n: find_std(n, True))
      export_data['filter_text'] = c_list.name.apply(lambda n: find_std(n))
      export_data = export_data[export_data.filter_out == False]
      export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
    
    offset +=no_dataset
    loop_bool = not(len(dataflows) < no_dataset)
  
  def find_std(n, is_bool=False):
    ns = re.sub(r"^\d+", "", n) # removed all the number from the begining
    ns = re.sub(r"^\_+", "", ns) # removed _ from the begining
    ns = re.sub(r"^Glue_", "", ns) # removed Glue_ from the begining
    ns = re.sub(r"^Glue", "", ns) # removed Glue_ from the begining
    ns = ns.lstrip(' ')
    if(ns in std_list):
        return True if is_bool else ''
    else:
        print(ns)
        return False if is_bool else ns
    

# ===================== common ======================


# ======================================================================================================================
print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)


instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)

std_list = pd.read_csv(relative_path() + '/' + std_etls_list)
std_list = std_list.name
cols = [
        'abandoned', 
        'customer', 
        'dataFlowType', 
        'databaseId',
        'inputCount',
        'name', 
        'outputCount', 
        'ownedById', 
        'ownedByName', 
        'ownedByType', 
        'paused', 
        'runCount', 
        'status', 
        'winnerText',
        'lastModified',
        'client', 
        'instance_id'
      ]
df = pd.DataFrame(columns=cols)

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

  # get user counts
  client = instance['client']

  fetch_all_ds(inst_id, session, client)