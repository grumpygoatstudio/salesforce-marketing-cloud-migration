import os
import sys
import requests
import json
import csv
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


def build_contact_json(connection, data):
    try:
        cust_data = {
          "contact": {
            "connectionid": connection,
            "externalid": str(data[6]),
            "email": data[2]
          }
        }
    except Exception:
        cust_data = None
    return cust_data


def lookup_crm_id(se_id, venue_id, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    sql = """SELECT crm_id FROM crm_linker_contacts WHERE se_id = \'%s\'""" % (se_id)
    db.query(sql)
    r = db.store_result()
    try:
        crm_id = int(r.fetch_row()[0][0])
    except Exception:
        crm_id = None
        print("CRM_ID LOOKUP FAILED!!", se_id)
    db.close()
    return crm_id


def save_crm_id(se_id, crm_id, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    sql = """INSERT INTO crm_linker_contacts SET se_id=\'%s\', crm_id=%s""" % (se_id, crm_id)
    db.query(sql)
    db.close()


def update_contact_in_crm(url, auth_header, data, configs):
    se_id = data['ecomUser']["externalid"]
    crm_id = None
    r = requests.post(url, headers=auth_header, data=json.dumps(data))
    if r.status_code != 201:
        if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
            crm_id = lookup_crm_id(se_id, configs)
    else:
        try:
            crm_id = r.json()["ecomUser"]["id"]
            save_crm_id(se_id, int(crm_id), configs)
        except Exception:
            pass
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_crm_contacts_sync = configs["last_crm_contacts_sync"]
    contacts_url = "https://heliumcomedy.api-us1.com/admin/api.php?api_action=contact_edit&api_tokent=" + configs["Api-Token"]

    try:
        file_path = os.path.join(dir_path, 'crm-data', 'contacts-data-dump.csv')
        os.remove(file_path)
    except OSError:
        pass

    # download CSV file from MySQL DB
    sql_cmd = """mysql %s -h %s -P %s -u %s --password=%s -e \"SELECT * FROM contacts_mv WHERE sys_entry_date = '0000-00-00 00:00:00' AND email != '' AND subscriber_key != '';\" > %s""" % (
        configs['db_name'],
        configs['db_host'],
        configs['db_port'],
        configs['db_user'],
        configs['db_password'],
        last_crm_contacts_sync.replace('T', ' '),
        file_path
    )
    os.system(sql_cmd)

    #load data from pulled MySQL dump CSV
    with open(file_path, "rU") as file_data:
        reader = csv.reader(file_data, delimiter='\t')
        next(reader, None)
        for contact_info in reader:
            # build contact JSON
            contact_json = build_contact_json(connection, contact_info)
            if contact_json:
                # PUT/POST the JSON objects to AC server
                crm_id = update_contact_in_crm(contacts_url, auth_header, contact_json, configs)
            else:
                print("BUILD CONTACT JSON FAILED!", str(contact_info))

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_contacts_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Contacts Sync Completed - " + configs['last_crm_contacts_sync'])


if __name__ == '__main__':
    active_campaign_sync()
