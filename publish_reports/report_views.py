import pandas as pd
import numpy as np
import os
import pathlib
import subprocess
import json
from common import helper
from datetime import datetime
import time
import csv

# ==== global vars
username = ""
password = ""
instances_ref = "report_view.csv"
export_csv_ref = 'card_views.csv'
no_cards = 30
occ = "=" * 30
inst_dfs = {}
export_data = pd.DataFrame()
# ==== global vars

# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def fetch_all_ds(instance_id, session):
    offset=0
    loop_bool = True
    inst_dfs[instance_id] = []


    while loop_bool:
        card_list_payload = {"entities": ["DATASET"], "filters": [
            {"filterType": "numeric", "field": "card_count", "longNumber": "0", "not": False, "operator": "GT"}
        ], "combineResults": True, "query": "*", "count": no_cards,
         "offset": offset, "sort": {"isRelevance": False,
        "fieldSorts": [{"field": "create_date", "sortOrder": "DESC"}]}}

        id_list = helper.fetch_datasets(instance['instance_id'], session, card_list_payload)
        print("Fetching All Card ids from instance: {}".format(instance_id))
        id_list = [i['id'] for i in id_list['dataSources']]
        inst_dfs[instance_id].extend(id_list)
        offset += no_cards
        loop_bool = not(len(id_list) < no_cards)

    print("Done Fetching All Card ids from instance: {}".format(instance_id))
# ===================== common ======================

print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)

headerList = [
            'card_id',
            'card_title',
            'creator',
            'created_at',
            'instance_id',
            'owners',
            'access',
            'viewCount',
            'user_count',
            'page_id',
            'page_name',
            'dataset_name',
            'dataset_id',
            'dashboard_name',
            'dashboard_id']
headerList.sort()

with open(export_csv_ref, 'wt', newline ='') as file:
    writer = csv.writer(file, delimiter=',')
    writer.writerow(i for i in headerList)


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

    # get user counts
    user_count_payload = {"showCount": True, "count": True, "includeDeleted": False, "includeSupport": False,
                          "limit": 1, "offset": 0, "sort": {"field": "displayName", "order": "ASC"},
                          "filters": [], "attributes": []}
    user_count = helper.get_users(instance['instance_id'], session, user_count_payload)['count']

    # iterate over cards
    fetch_all_ds(inst_id, session)
    print("Start fetching cards from each dataset of instance : {}".format(inst_id))
    for dataset_id in inst_dfs[inst_id]:
        cards = helper.each_datasets_cards(inst_id, session, dataset_id)
        for card in cards:
            i = card['id']
            show_empty = False
            query = "parts=adminAllPages&parts=resourceAccess&parts=subscriptions&parts=viewInfo&parts=certification&parts=datasources&parts=owners&urns={}".format(i)
            card_details = helper.get_card_details(inst_id, session, query)

            card_details = card_details[0]
            adminAllPages = card_details['adminAllPages']
            created_at = datetime.fromtimestamp(int(card_details['created']))
            creator = helper.get_cards_users(inst_id, session, "id={}".format(card_details['creatorId']))
            creator = creator[0]
            # del creator['invitorUserId']
            print("Adding card_id : {} from instance: {} to the csv".format(i, inst_id))

            if 'subscriptions' not in card_details.keys():
                show_empty = True

            if len(adminAllPages) > 0:
                page_id = None if show_empty else adminAllPages[0].get('pageId', None)
                page_name = '' if show_empty else adminAllPages[0].get('title', '')
                dashboard_name = '' if show_empty else adminAllPages[0].get('parentPageTitle', '')
                dashboard_id = None if show_empty else adminAllPages[0].get('parentPageId', None)
            else:
                page_id = None
                page_name = ''
                dashboard_name = ''
                dashboard_id = None

            card_obj = {
                'card_id': np.int64(card_details['id']),
                'instance_id': inst_id,
                'card_title': card_details['title'],
                'creator': creator,
                'created_at': created_at,
                'owners': card_details['owners'],
                'access': card_details['resourceAccess']['userIds'],
                'viewCount': card_details['viewInfo']['totalViewCount'],
                'user_count': user_count,
                'page_id': page_id,
                'page_name': page_name,
                'dataset_name': '' if show_empty else card_details['datasources'][0].get('dataSourceName', ''),
                'dataset_id': '' if show_empty else card_details['datasources'][0].get('dataSourceId', ''),
                'dashboard_name': dashboard_name,
                'dashboard_id': dashboard_id
            }
            card_record = pd.Series(card_obj)
            export_data = pd.DataFrame()
            export_data = export_data.append(card_record,ignore_index=True,sort=False)

            export_data.card_id = export_data.card_id.astype(int)
            export_data.user_count = export_data.user_count.astype(int)
            export_data.viewCount = export_data.viewCount.astype(int)
            export_data.dashboard_id = export_data.dashboard_id.fillna(0.0).astype(int)
            export_data.page_id = export_data.page_id.fillna(0.0).astype(int)

            export_data.dashboard_id = export_data.dashboard_id.replace(to_replace = 0, value = np.nan)
            export_data.page_id = export_data.page_id.replace(to_replace = 0, value = np.nan)

            export_data.to_csv(export_csv_ref, mode='a', index=False, header=False)
            print("Successfully appended card_id : {} from instance: {} to the csv".format(i, inst_id))
    del inst_dfs[inst_id]
print(occ,"Script completed now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)