import pandas as pd
from common import helper
import logging

user_to_add = pd.read_csv('user_to_add.csv')

instances_ref = pd.read_csv('inst_client_creds.csv')

for i, instance_info in instances_ref.iterrows():
    # login to the instance and get the token from helper.get_session_token
    session = helper.get_session_token(instance_info['instance_id'],
                                       instance_info['username'],
                                       instance_info['password'])

    if type(session) != str and session[0] is None:
        logging.error(session[1])
        continue

    for j,val in user_to_add.iterrows():
        # roleId: 1 - admin
        # roleId: 3 - Editor

        new_user = {"displayName": val['displayName'], "detail": {"email": val['email']}, "roleId": 1}
        helper.add_user(instance_info['instance_id'], session, new_user)