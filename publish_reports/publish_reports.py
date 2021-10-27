import pandas as pd
import os
import pathlib
import subprocess
import json
import requests
import logging

# === Notes ===
# compatible for 1 to 1 & 1 to many reports only at the moment
# not compatible for many to many reports
# rethink about the datasource if reference instances and while copying json reports
# get data source(get_data_sets from domo_helper) and page id(check riley's video admin -> pages ) dynamically
# === Notes ===

# ==== global vars
report_instance = ""
username = "** some user email **"
password = "** some password **"
parent_report_csv = "parent_reports_to_be_published.csv"
instance_to_publish_csv = "list_of_instances_to_publish_reports.csv"
blank_json = None
occ = "=" * 30
publish_report_instances = None
reports_json_to_download = None
# ==== global vars


# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))


def publish_with_jar(instance_id, page_id, json_path):
    command_to_run = "connect -u {} -p {} -s {}.domo.com\nrestore-card -i {} -f '{}'\nquit".format(
        username, password, instance_id, page_id, json_path.as_posix())
    # !!!!!CAUTION!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))
    # !!!!!CAUTION!!!!


# referred from domo_helper.py
def get_session_token(domo_instance, email, password):
    auth_api = 'https://{}.domo.com/api/content/v2/authentication'.format(domo_instance)
    auth_body = json.dumps({
        "method": "password",
        "emailAddress": email,
        "password": password
    })
    auth_headers = {'Content-Type': 'application/json'}
    auth_response = requests.post(auth_api, data=auth_body, headers=auth_headers)
    auth_status = auth_response.status_code
    resp = auth_response.json()
    if auth_status == 200:
        if (resp['success'] is False):
            token_error_string = "Failed to login to the instance : {} ,  reason: {}".format(domo_instance,
                                                                                             resp['reason'])
            return None, token_error_string
        else:
            logging.info('Session token acquired.')
            return resp['sessionToken']
    else:
        token_error_string = 'Token request ended up with status code {}'.format(auth_status)
        logging.error(token_error_string)
        logging.error(auth_response.text)
        raise Exception(token_error_string)
        return None


def update_with_new_value(report_json, key_name, value):
    report_json_str = json.dumps(report_json)
    val_index = report_json_str.find(key_name) + len(key_name) + 4
    old_key_val = report_json_str[val_index:]
    old_key_val = old_key_val[:old_key_val.find('"')]

    report_json_str = report_json_str.replace(old_key_val, value)
    return report_json_str


# ===================== common =======================


def generate_report_json(report_json):
    # create folder to save files
    folder_name = 'reports_to_be_published'
    json_name = report_json['json_name']

    project_dir = pathlib.Path(relative_path())
    publish_dir = project_dir / folder_name

    if not os.path.exists(publish_dir):
        os.mkdir(publish_dir)

    pathlib.Path(str(publish_dir) + '/' + json_name).touch()

    json_path = publish_dir / json_name

    command_to_run = "connect -u {} -p {} -s {}.domo.com\nbackup-card -i {} -f '{}'\nquit".format(
        username, password, report_json['instance_id'], report_json['report_id'], json_path.as_posix())
    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))
    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!





def generate_blank_report_and_publish(instance_info, blank_json, pd_index, session_token, all_publish_report_instances):
    # ================== Manupulate blank Json =================
    # replace datasource id with the one in the csv
    blank_json['dataProvider']['dataSourceId'] = instance_info['datasource_id']
    blank_json['definition']['title'] = blank_json['definition']['title'] + "_from_script_0"


    cards_api = "https://{}.domo.com/api/content/v3/cards/kpi?newContainer=true&pageId={}".format(
        instance_info['instance_id'], instance_info['page_id'])

    cards_headers = {'Content-Type': 'application/json',
                     'x-domo-authentication': session_token}
    cards_response = requests.put(url=cards_api, data=json.dumps(blank_json), headers=cards_headers)
    cards_status = cards_response.status_code
    if cards_status == 200:
        cards_details = json.loads(cards_response.text)
        card_id = cards_details['id']

        # replace card_id value in publish_report_instances
        all_publish_report_instances.loc[pd_index, 'card_id'] = card_id
        logging.info('Blank report successfully created id {}'.format(card_id))

        # ================== Publish Parent instance reports to child instance =================
        print(occ + "Publish reports to instance/s" + occ)
        publish_report(instance_info)
        print(occ + "Done publishing reports to instance/s" + occ)
    else:
        error = 'Card/Report creation PUT request ended up with status code {}'.format(cards_status)
        logging.error(error)
        logging.error(cards_response.text)
        raise Exception(error)


def publish_report(instance_info):
    project_dir = pathlib.Path(relative_path())
    publish_dir = project_dir / 'reports_to_be_published'
    for json_name in os.listdir(publish_dir.as_posix()):
        if 'json' in json_name:
            json_path = publish_dir / json_name

            with open(json_path) as f:
                report_json = json.load(f)

            # ================== Manipulate Json =================
            # replace 'cardId' with the blank card/report id(int) we have created on the new instance
            report_json['cardId'] = instance_info['card_id']

            # replace 'urn' with the blank card/report id(string) we have created on the new instance
            report_json['urn'] = str(instance_info['card_id'])

            # replace 'dataSourceId' key throughout the json
            report_json_str = update_with_new_value(report_json, 'dataSourceId', instance_info['datasource_id'])

            # replace json_path file with the manipulated one

            with open(json_path, "w") as outfile:
                outfile.write(report_json_str)

            # refer the same json file again in the command_to_run
            command_to_run = "connect -u {} -p {} -s {}.domo.com\nrestore-card -i {} -f '{}'\nquit".format(
                username, password, instance_info['instance_id'], instance_info['page_id'], json_path.as_posix())
            # # !!!!!CAUTION!!!!
            subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))

# ======================================================================================================================

print(occ + " Start creating Json for Report/s  " + occ)
reports_json_to_download = pd.read_csv(relative_path() + '/' + parent_report_csv)

reports_json_to_download[['username']] = reports_json_to_download[['username']].fillna(value=username)
reports_json_to_download[['password']] = reports_json_to_download[['password']].fillna(value=password)

for index, report_json in reports_json_to_download.iterrows():
    generate_report_json(report_json)

print(occ + " Report/s Json Created! " + occ)


# ================ Get info of instance/s where Report/s are to be published ===============
print(occ + "Get info of instance/s where Report/s are to be published" + occ)
publish_report_instances = pd.read_csv(relative_path() + '/' + instance_to_publish_csv)

publish_report_instances[['card_id']] = publish_report_instances[['card_id']].fillna(value=0)
publish_report_instances[['username']] = publish_report_instances[['username']].fillna(value=username)
publish_report_instances[['password']] = publish_report_instances[['password']].fillna(value=password)

publish_report_instances.astype({"card_id": 'int64'})
print(occ + "Received info of instance/s where Report/s are to be published" + occ)

# ================== Create blank report of each parent reports for all the child instances =============

for index, report_json in reports_json_to_download.iterrows():

    all_publish_report_instances = publish_report_instances.copy()
    # ================== Create blank report =============
    # create a blank report(can be automated) with the same dataset
    print(occ + "Generate a blank report to instance/s where Report/s are to be published" + occ)

    with open('blank_report.json') as f:
        blank_json = json.load(f)

    for index, instance_info in all_publish_report_instances.iterrows():
        session_token = get_session_token(instance_info['instance_id'], instance_info['username'],
                                          instance_info['password'])
        if type(session_token) != str and session_token[0] is None:
            logging.error(session_token[1])
            continue

        generate_blank_report_and_publish(instance_info, blank_json, index, session_token, all_publish_report_instances)
    print(occ + "Done creating blank reports on the instance/s where Report/s are to be published" + occ)


# ====================================





