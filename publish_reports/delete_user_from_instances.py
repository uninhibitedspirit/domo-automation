import pandas as pd
from common import helper
import logging

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
    user_email = [] # list of emails abc@domain.com, efg@domain.com
    for email_id in user_email:
        search_text = email_id
        ds_list = helper.get_all_users(instance_info['instance_id'], session, 0, [{"filterType":"text", "text": search_text}])
        # Fetch domoadmin's id from 'edcast-{instance_id}.domo.com/api/identity/v1/users/search' API
        print(ds_list)
        if len(ds_list) > 0:
            print(helper.delete_user(instance_info['instance_id'], session, ds_list[0]))
        else:
            print("No user found!")