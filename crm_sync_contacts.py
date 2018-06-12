import os
import sys
import requests
import json
import csv
import collections
import _mysql

from datetime import datetime

reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def build_contact_data(data, api_key):
    print(data)
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = data[2]
    d["overwrite"] = "0"
    return d


def lookup_crm_id_by_api(url, data, auth_header):
    data["api_action"] = "contact_view_email"
    r = requests.post(url, headers=auth_header, data=data)
    if r.status_code == 200 and r.json()["result_code"] != 0:
        return r.json()["id"]
    else:
        return None


def lookup_crm_id_by_sql(email, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    sql = """SELECT crm_id FROM crm_linker_contacts WHERE email = \'%s\'""" % (email)
    db.query(sql)
    r = db.store_result()
    try:
        crm_id = int(r.fetch_row()[0][0])
    except Exception:
        crm_id = None
        print("CRM_ID LOOKUP FAILED!!", email)
    db.close()
    return crm_id


def save_crm_id(email, crm_id, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    sql = """INSERT INTO crm_linker_contacts SET email=\'%s\', crm_id=%s""" % (email, crm_id)
    db.query(sql)
    db.close()


def update_contact_in_crm(url, auth_header, data, configs):
    # crm_id = lookup_crm_id_by_sql(data["email"], configs)
    # if not crm_id:
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if not crm_id:
        return None
    save_crm_id(data["email"], crm_id, configs)
    print(data["email"], crm_id)
    data['id'] = crm_id
    #r = requests.post(url, headers=auth_header, data=data)
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_crm_contacts_sync = configs["last_crm_contacts_sync"]
    contacts_url = "https://heliumcomedy.api-us1.com/admin/api.php"

    #load data from MySQL
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    db.query("""SELECT * FROM contacts_mv WHERE sys_entry_date = '0000-00-00 00:00:00' AND email_address != '' AND subscriber_key != ''""")
    r = db.store_result()
    more_rows = True
    while more_rows:
        contact_info = r.fetch_row()[0]
        print(contact_info)
        if contact_info:
            # build contact JSON
            contact_data = build_contact_data(contact_info, configs["Api-Token"])
            if contact_data:
                # PUT/POST the JSON objects to AC server
                crm_id = update_contact_in_crm(contacts_url, auth_header, contact_data, configs)
                print("CONTACT LOOKUP FAILED!", str(contact_data))
            else:
                print("BUILD CONTACT DATA FAILED!", str(contact_info))
        else:
            more_rows = False

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_contacts_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Contacts Sync Completed - " + configs['last_crm_contacts_sync'])


if __name__ == '__main__':
    active_campaign_sync()
