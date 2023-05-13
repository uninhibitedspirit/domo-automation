import pandas as pd
import os
import pathlib
import subprocess
import json
import requests
import logging
# from common import helper
from datetime import datetime




# ==== global vars
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
export_csv_ref = 'dataset_row_count.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

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
        print(j_ref)
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


# ======================================================================================================================
print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)


instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)


for i, instance in instance_info.iterrows():
  
  session = helper.get_session_token(instance['instance_id'],
                                      instance['username'],
                                      instance['password'])

  # log error if login fails
  if type(session) != str and session[0] is None:
      logging.error(session[1])
      continue

  # fetch all the datasets
  last_run_date = datetime.now() - timedelta(days=int(3))
  last_run_date = last_run_date.timestamp() * 1000
  last_run_date = pd.to_datetime(last_run_date, unit='ms')
  get_all_datasets(instance['instance_id'], session)