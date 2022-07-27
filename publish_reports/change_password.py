import pandas as pd
from common import helper
import logging

# ==== global vars
instances_ref = "instance_credentials.csv"
export_data = pd.DataFrame()
no_dataset = 30
occ = "=" * 30
last_no_days = 3
inst_df_id = {}
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))


instances_ref = pd.read_csv('instance_credentials.csv')
for i, instance_info in instances_ref.iterrows():
    # login to the instance and get the token from helper.get_session_token
    session = helper.get_session_token(instance_info['instance_id'],
                                       instance_info['username'],
                                       instance_info['password'])

    if type(session) != str and session[0] is None:
        logging.error(session[1])
        continue

    # search_text=instance_info['username']
    search_text = "" #email reference
    ds_list = helper.get_all_users(instance_info['instance_id'], session, 0, [{"filterType":"text", "text": search_text}])
    # Fetch domoadmin's id from 'edcast-{instance_id}.domo.com/api/identity/v1/users/search' API
    print(ds_list)
    user_id = ds_list[0]['id']
    password = "" #Add Temporary password
    helper.change_password(instance_info['instance_id'], session, user_id, password, ds_list[0]['name'], ds_list[0]['email'])