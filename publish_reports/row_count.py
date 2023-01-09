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
import csv
import math




# ==== global vars
username = ""
password = ""
instances_ref = "inst_client_creds.csv"
export_csv_ref = 'row_count.csv'
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
# ==== global vars


# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))



def get_instance_rows(instance_id, session_token, **queryparams):
  list_DF_API = "https://{}.domo.com/api/data/ui/v1/warehouse/".format(instance_id)
  cards_headers = {'Content-Type': 'application/json',
                    'x-domo-authentication': session_token}
  df_response = requests.get(url=list_DF_API, headers=cards_headers)
  r_status = df_response.status_code
  if r_status == 200:
      j_ref = json.loads(df_response.text)
      logging.info('Successfully fetched all the dataflows')
      
      return j_ref
  else:
    error = "There was error in fetching wearehouse details from instance id: '{}' with status code:{}".format(instance_id,
                                                                                                      cards_status)
    logging.error(error)
    logging.error(df_response.text)
    raise Exception(error)
    return []
# ===================== common ======================

# ====init
# headerList = [
#             'instace_name',
#             'client',
#             'dataSetCount',
#             'dataSetRowCount']
# headerList.sort()

# with open(export_csv_ref, 'wt', newline ='') as file:
#     writer = csv.writer(file, delimiter=',')
#     writer.writerow(i for i in headerList)
# ====



# ======================================================================================================================
print(occ,"Starting script row_count now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)

instance_info = pd.read_csv(relative_path() + '/' + instances_ref)

instance_info[['username']] = instance_info[['username']].fillna(value=username)
instance_info[['password']] = instance_info[['password']].fillna(value=password)


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
    
    row_det = get_instance_rows(inst_id, session)
    print("inst_id {} == ".format(inst_id), row_det['summary'])
    row_det = row_det['summary']
    obj = {
        'instace_name': inst_id,
        'client': instance['client'],
        'dataSetCount': row_det['dataSetCount'],
        'dataSetRowCount': row_det['dataSetRowCount'],
    }
    row_record = pd.Series(obj)
    export_data = pd.DataFrame()
    export_data = export_data.append(row_record,ignore_index=True,sort=False)
    export_data.dataSetCount = export_data.dataSetCount.astype('int32')
    export_data.dataSetRowCount = export_data.dataSetRowCount.astype('int32')
    export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)