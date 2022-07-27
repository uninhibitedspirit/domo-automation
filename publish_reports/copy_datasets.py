import pandas as pd
import os
import pathlib
import subprocess
import json
from common import helper
from datetime import datetime
# === Notes ===
#
# get data source(get_data_sets from domo_helper) and page id(check riley's video admin -> pages ) dynamically
#
#
# === Notes ===

# ==== global vars
username = ""
password = ""
parent_dataflow_csv = "parent_dataset_ref.csv"
instance_to_publish_csv = "paste_dataflows_ref.csv"
folder_name = 'created_datasets'
occ = "=" * 30
paste_dataset_instances = None
dataset_json_to_download = None
# ==== global vars


# ===================== common ======================
def relative_path():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

def update_with_new_value(dataset_json, key_name, value):
    dataset_json_str = json.dumps(dataset_json)
    val_index = dataset_json_str.find(key_name) + len(key_name) + 4
    old_key_val = dataset_json_str[val_index:]
    old_key_val = old_key_val[:old_key_val.find('"')]

    dataset_json_str = dataset_json_str.replace(old_key_val, value)
    return dataset_json_str

# ===================== common =======================


def generate_dataset_json(dataset_json):
    # create folder to save files
    json_name = dataset_json['json_name']
    project_dir = pathlib.Path(relative_path())
    publish_dir = project_dir / folder_name

    if not os.path.exists(publish_dir):
        os.mkdir(publish_dir)

    pathlib.Path(str(publish_dir) + '/' + json_name).touch()

    json_path = publish_dir / json_name

    command_to_run = "connect -s {}.domo.com -u {} -p {}\nlist-dataflow -i {} -f '{}'\nquit".format(
        dataset_json['instance_id'],dataset_json['username'], dataset_json['password'], dataset_json['dataset_id'], json_path.as_posix())

    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))
    # !!!!!CAUTION!!!!
    # !!!!!will replace the existing files with the same name!!!!!

def paste_dataset(instance_info, json_name):
    project_dir = pathlib.Path(relative_path())
    publish_dir = project_dir / folder_name
    json_path = publish_dir / json_name

    # refer the same json file again in the command_to_run
    command_to_run = "connect -s {}.domo.com -u {} -p {}\nset-dataflow-properties -d '{}' -c\nquit".format(
        instance_info['instance_id'], instance_info['username'], instance_info['password'],
        json_path.as_posix())
    print(command_to_run)
    # # !!!!!CAUTION!!!!
    subprocess.run(["java", "-jar", "domoUtil.jar"], input=bytes(command_to_run, 'utf-8'))

# ======================================================================================================================
print(occ,"Starting script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)

dataset_json_to_download = pd.read_csv(relative_path() + '/' + parent_dataflow_csv)

dataset_json_to_download[['username']] = dataset_json_to_download[['username']].fillna(value=username)
dataset_json_to_download[['password']] = dataset_json_to_download[['password']].fillna(value=password)


# ================ Get info of instance/s where Dataflow/s are to be created ===============
print(occ + "Get info of instance/s where Dataflow/s are to be created" + occ)
paste_dataset_instances = pd.read_csv(relative_path() + '/' + instance_to_publish_csv)

paste_dataset_instances[['username']] = paste_dataset_instances[['username']].fillna(value=username)
paste_dataset_instances[['password']] = paste_dataset_instances[['password']].fillna(value=password)

print(occ + "Received info of instance/s where Dataflow/s are to be published" + occ)

# ================== Create blank dataflow of each parent reports for all the child instances =============

for parent_index, dataset_json in dataset_json_to_download.iterrows():

    print(occ + " Start creating Json for Report/s  " + occ)
    generate_dataset_json(dataset_json)
    print(occ + " Dataflow Created! " + occ)

    for index, instance_info in paste_dataset_instances.iterrows():
        print(occ + "Creating {} report for the instance {}".format(
            dataset_json['json_name'].strip('.json'), instance_info['instance_id']) + occ)
        paste_dataset(instance_info, dataset_json['json_name'])
        print(occ + "Done creating {} report for the instance {}".format(
            dataset_json['json_name'].strip('.json'), instance_info['instance_id']) + occ)
    print(occ + "Done creating report: {} on all given instances".format(dataset_json['json_name'].strip('.json')) + occ)

print(occ,"Ended script now == ", datetime.now().strftime("%d-%b-%Y %H:%M:%S"), occ)
# ====================================