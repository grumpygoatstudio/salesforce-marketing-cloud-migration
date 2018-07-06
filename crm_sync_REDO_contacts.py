import os
import sys
import requests
import json
import collections
import csv
import _mysql
import smtplib

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
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = str(data['email']).replace("\r", "")
    d["id"] = None
    d["overwrite"] = "0"
    d["first_name"] = str(data['fname'])
    d["last_name"] = str(data['lname'])
    if data['list'] not in ["None", ""]:
        d["p[%s]" % data['list']] = data['list']
    return d


def fetch_crm_list_mapping(configs):
    list_mappings = {}
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    db.query("""SELECT id, insider_crm_id FROM venues""")
    r = db.store_result()
    more_rows = True
    while more_rows:
        try:
            venue_info = r.fetch_row(how=2)[0]
            list_mappings[venue_info["venues.id"]
                          ] = venue_info["venues.insider_crm_id"]
        except IndexError:
            more_rows = False
    return list_mappings


def lookup_crm_id_by_api(url, data, auth_header):
    data["api_action"] = "contact_view_email"
    r = requests.post(url, headers=auth_header, data=data)
    if r.status_code == 200 and r.json()["result_code"] != 0:
        return r.json()["id"]
    else:
        return None


def update_contact_in_crm(url, auth_header, data, configs, list_subcrip):
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if crm_id:
        data['id'] = crm_id
        r = requests.post(url, headers=auth_header, data=data)
        if r.status_code == 200 and r.json()["result_code"] != 0:
            return True
        else:
            print("ERROR: Updating contact via API failed.", data['email'])
            return False
    else:
        try:
            data["api_action"] = "contact_add"
            data.pop("id", None)
            if list_subcrip not in ["None", ""]:
                r = requests.post(url, headers=auth_header, data=data)
                if r.status_code == 200 and r.json()["result_code"] != 0:
                    return True
                else:
                    print("ERROR: Creating contact via API failed.", data['email'])
                    return False
        except Exception:
            print("ERROR: Creating contact via API failed.", data['email'])
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_crm_contacts_sync = configs["last_crm_contacts_sync"]
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    list_mappings = fetch_crm_list_mapping(configs)

    contact_err = 0
    contact_count = 0
    file_path = os.path.join(dir_path, 'historic_lists', 'se_cust_truth_Jul6.csv')
    with open(file_path, "r") as file_data:
        file_data = csv.reader(file_data, delimiter=',')
        for l in file_data:
                contact_info = {
                    'email': l[0],
                    'fname': l[1],
                    'lname': l[2],
                    'venue': l[3],
                    'list': l[4],
                }
                list_subcrip = str(l[4])
                contact_data = build_contact_data(contact_info, configs["Api-Token"])
                if contact_data:
                    update_contact_in_crm(
                        url, auth_header, contact_data, configs, list_subcrip)
                    contact_count += 1
                else:
                    contact_err += 1
                    print("BUILD CONTACT DATA FAILED!", str(contact_info['email']))

    # setup a completion email notifying Kevin and Jason that a Month of Venue pushes has finished
    sender = "kevin@matsongroup.com"
    recipients = ["flygeneticist@gmail.com", "jason@matsongroup.com"]
    header = 'From: %s\n' % sender
    header += 'To: %s\n' % ", ".join(recipients)
    header += 'Subject: Completed a MASSIVE UPDATE of Contacts - SeatEngine AWS\n'
    msg = header + "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that a push to REDO Contact syncs have completed:\nContacts pushed (SUCCESS qty: %s, ERROR qty: %s)\n" % (contact_count, contact_err)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
    server.sendmail(sender, recipients, msg)
    server.quit()


if __name__ == '__main__':
    active_campaign_sync()
