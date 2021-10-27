import pandas as pd
import os
import pathlib
import subprocess
import json

# === Notes ===
# compatible for many to many reports only if all the parent instances datasets are the same
# get data source(get_data_sets from domo_helper) and page id(check riley's video admin -> pages ) dynamically
#
#
# === Notes ===

# ==== global vars
username = "** some user email **"
password = "** some password **"
parent_report_csv = "parent_reports_to_be_published.csv"
instance_to_publish_csv = "list_of_instances_to_publish_reports.csv"
occ = "=" * 30
publish_report_instances = None
reports_json_to_download = None
# ==== global vars


# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

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
        report_json['username'], report_json['password'], report_json['instance_id'], report_json['report_id'], json_path.as_posix())
    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))
    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!

def publish_report(instance_info, json_name):
    project_dir = pathlib.Path(relative_path())
    publish_dir = project_dir / 'reports_to_be_published'
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
        instance_info['username'], instance_info['password'], instance_info['instance_id'],
        instance_info['page_id'], json_path.as_posix())
    # # !!!!!CAUTION!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))

# ======================================================================================================================

reports_json_to_download = pd.read_csv(relative_path() + '/' + parent_report_csv)

reports_json_to_download[['username']] = reports_json_to_download[['username']].fillna(value=username)
reports_json_to_download[['password']] = reports_json_to_download[['password']].fillna(value=password)


# ================ Get info of instance/s where Report/s are to be published ===============
print(occ + "Get info of instance/s where Report/s are to be published" + occ)
publish_report_instances = pd.read_csv(relative_path() + '/' + instance_to_publish_csv)

publish_report_instances[['card_id']] = publish_report_instances[['card_id']].fillna(value=0)
publish_report_instances[['username']] = publish_report_instances[['username']].fillna(value=username)
publish_report_instances[['password']] = publish_report_instances[['password']].fillna(value=password)

publish_report_instances.astype({"card_id": 'int64'})
print(occ + "Received info of instance/s where Report/s are to be published" + occ)

# ================== Create blank report of each parent reports for all the child instances =============

for parent_index, report_json in reports_json_to_download.iterrows():

    print(occ + " Start creating Json for Report/s  " + occ)
    generate_report_json(report_json)
    print(occ + " Report/s Json Created! " + occ)

    all_publish_report_instances = publish_report_instances.copy()

    for index, instance_info in all_publish_report_instances.iterrows():
        publish_report(instance_info, report_json['json_name'])
    print(occ + "Done creating report: {} on all given instances".format(report_json['json_name'].strip('.json')) + occ)


# ====================================





