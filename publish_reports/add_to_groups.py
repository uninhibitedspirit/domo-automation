import pandas as pd
from common import helper
import logging

user_to_add = pd.read_csv('user_to_add.csv')
group_list = pd.read_csv('group_list.csv')

instance_info = {'instance_id':'' , 'username': '', 'password': ''}


session = helper.get_session_token(instance_info['instance_id'],
                                    instance_info['username'],
                                    instance_info['password'])

if type(session) != str and session[0] is None:
  logging.error(session[1])
  exit()

for email_id in user_to_add['email']:
  search_text = email_id
  ds_list = helper.get_all_users(instance_info['instance_id'], session, 0, [{"filterType":"text", "text": search_text}])
  user = ds_list[0]
  # Fetch domoadmin's id from 'edcast-{instance_id}.domo.com/api/identity/v1/users/search' API
  print(ds_list)
  if len(ds_list) > 0:
    for group_id in group_list['group_id']:
      helper.add_to_group(instance_info['instance_id'], session, group_id, user['id'], user['email'])
      print("==added==")
  else:
    print("No user found!")