import pandas as pd
from common import helper
import logging

email_to_add = '' # email reference
full_name = "" #Name of the user


instances_ref = pd.read_csv('instance_credentials.csv')
for i, instance_info in instances_ref.iterrows():
    # login to the instance and get the token from helper.get_session_token
    session = helper.get_session_token(instance_info['instance_id'],
                                       instance_info['username'],
                                       instance_info['password'])

    if type(session) != str and session[0] is None:
        logging.error(session[1])
        continue

    new_user = {"displayName": full_name, "detail": {"email": email_to_add}, "roleId": 1} #role 1 means admin
    helper.add_user(instance_info['instance_id'], session, new_user)