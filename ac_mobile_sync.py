import os
import sys
import requests
import json
import collections
import _mysql
from simplejson import JSONDecodeError
from datetime import datetime
from optparse import OptionParser
from time import sleep


parser = OptionParser()
parser.add_option("-p", "--postprocess", dest="postprocess", type="string",
                  help="Run only postprocessing scripts for show attendees in last 24 hours", metavar="postprocess")
parser.add_option("-e", "--extrapush", dest="extrapush", type="string",
                  help="push the contacts to AC again, don't update last_sync date", metavar="extrapush")

reload(sys)
sys.setdefaultencoding('utf-8')
# sentry_sdk.init("https://8e6ee04ac6b14915a677ced85ab320f0@sentry.io/1286483")


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def build_contact_data(data, api_key, mobile_status):
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = str(data['cm.email_address']).replace("\r", "").strip()
    d["id"] = None
    d["overwrite"] = "0"
    # all custom contacts fields
    d["field[%MOBILE_NUMBER%,0]"] = str(data['cm.mobile_number'])
    d["field[%MOBILE_OPTIN%,0]"] = mobile_status
    return d

def lookup_crm_id_by_api(url, data, auth_header):
    data["api_action"] = "contact_view_email"
    try:
        r = requests.post(url, headers=auth_header, data=data)
        try:
            if r.status_code == 200 and r.json()["result_code"] != 0:
                return r.json()["id"]
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!", str(data["email"]))
            return None
    except Exception:
        return None


def add_tag_to_contact(url, crm_id, tag, api_key, auth_header):
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_tag_add"
    d["api_output"] = "json"
    d["id"] = crm_id
    d["tags"] = tag
    try:
        r = requests.post(url, headers=auth_header, data=d)
        try:
            if r.status_code == 200 and r.json()["result_code"] != 0:
                return True
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!", str(d["id"]))
            return None
    except Exception:
        return None


def lookup_list_by_api(url, crm_id, api_key, auth_header):
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_view"
    d["api_output"] = "json"
    d["id"] = crm_id
    try:
        r = requests.post(url, headers=auth_header, data=d)
        try:
            if r.status_code == 200 and r.json()["result_code"] != 0:
                return r.json()["listid"]
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!", str(d["id"]))
            return None
    except Exception:
        return None


def update_contact_tags(url, auth_header, data, tag, configs):
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if crm_id:
        add_tag_to_contact(url, crm_id, tag, data['api_key'], auth_header)
    else:
        print("ERROR: Missing CRM ID. Create contact via API failed.", data['email'])
        return "err_other"
    return crm_id


def update_contact_in_crm(url, auth_header, data, configs):
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if crm_id:
        list_id = lookup_list_by_api(url, crm_id, data['api_key'], auth_header)
        if list_id:
            try:
                data['id'] = crm_id
                field = "p[%s]" % list_id
                data[field] = list_id
                data["api_action"] = "contact_edit"
                r = requests.post(url, headers=auth_header, data=data)
                if r.status_code == 200:
                    try:
                        if r.json()["result_code"] != 0:
                            print("%s - %s" % (crm_id, data['field[%MOBILE_OPTIN%,0]']))
                            return "success"
                        else:
                            print("ERROR: Updating contact via API failed.", data['email'])
                            return "err_update"
                        return 'err_other'
                    except (Exception, JSONDecodeError) as e:
                        print("ERROR: Other error.", data['email'], e)
                        return 'err_other'
                else:
                    print("ERROR: Updating contact via API failed.", data['email'])
                    return "err_update"
            except JSONDecodeError:
                print("JSON DECODE ERROR!", str(data["email"]))
                return "err_other"
        else:
            print("ERROR: Missing CRM List. Create contact via API failed.", data['email'])
            return "err_list"
    else:
        print("ERROR: Missing CRM ID. Create contact via API failed.", data['email'])
        return "err_other"
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_ac_mobile_sync = configs["last_ac_mobile_sync"]
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])

    print("~~~~~ PROCESSING CONTACTS ~~~~~")
    db.query(
        """SELECT
            cm.email_address, cm.mobile_number,
            cm.mobile_status as mobile_status, cm.last_message_date as mobile_date,
            ac.optin_status as ac_status, ac.date_last_updated as ac_date
        FROM contacts_mobile_mv cm
        LEFT JOIN ac_mobile_contacts ac ON (ac.email = cm.email_address)
        WHERE cm.last_message_date BETWEEN \'%s\' AND NOW()
        AND cm.email_address IS NOT NULL
        ORDER BY cm.email_address, cm.mobile_number, cm.last_message_date;
        """ % (last_ac_mobile_sync.replace('T', ' '))
    )
    r = db.store_result()
    contacts = []
    contact_count = 0
    contact_err = {"list": [], "add": [], "update": [], "ssl": [], "other": []}
    chunk_size = 5000
    chunk_num = 0

    more_rows = True
    while more_rows:
        try:
            contacts.append(r.fetch_row(how=2)[0])
        except IndexError:
            more_rows = False
    db.close()

    for contact_info in contacts:
        mobile_status = None
        mu_status = 'Yes' if contact_info["cm.mobile_status"] == 'Subscribed' else 'No'

        # Check for:
        # 1. SE Contact w/o AC Mobile Info --> take MU status
        # 2. Different statuses btwn AC & Mobile Uploads (TO DO!!)
        #    --> take status from most recent update date
        if not contact_info["ac.ac_status"]:
            mobile_status = mu_status
        elif contact_info["ac.ac_status"]:
            ac_status = contact_info["ac.ac_status"].capitalize()
            # ensure statuses are different...
            if ac_status != mu_status:
                mu_date = datetime.strptime(contact_info['cm.mobile_date'], '%Y-%m-%d %H:%M:%S')
                ac_date = datetime.strptime(contact_info['ac.ac_date'], '%Y-%m-%d %H:%M:%S')
                if ac_date > mu_date:
                    mobile_status = ac_status
                elif mu_date > ac_date:
                    mobile_status = mu_status

        # if we've established a definitive status, move formward
        if mobile_status:
            try:
                contact_data = build_contact_data(contact_info, configs["Api-Token"], mobile_status)
            except Exception:
                contact_err['other'].append(str(contact_info['cm.email_address']))
                print("BUILD CONTACT DATA FAILED!", str(contact_info["cm.email_address"]))
            else:
                try:
                    if contact_data:
                        updated = update_contact_in_crm(url, auth_header, contact_data, configs)
                        if updated == 'success':
                            contact_count += 1
                        else:
                            if updated == 'err_list':
                                contact_err['list'].append(str(contact_info['cm.email_address']))
                            elif updated == 'err_add':
                                contact_err['add'].append(str(contact_info['cm.email_address']))
                            elif updated == 'err_update':
                                contact_err['update'].append(str(contact_info['cm.email_address']))
                            else:
                                contact_err['other'].append(str(contact_info['cm.email_address']))
                except requests.exceptions.SSLError:
                    contact_err["ssl"].append(str(contact_info['cm.email_address']))

        if contact_count % 500 == 0:
            print('Check in - #%s' % contact_count)

        if contact_count % chunk_size == 0:
            chunk_num += 1
            print("Done chunk(#%s)! Sleeping for 30 min to avoid SSL issues..." % chunk_num)
            sleep(1800) # sleep for 30 min to avoid SSL Errors

    d = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    configs['last_ac_mobile_sync'] = d

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    # write_config(configs, dir_path)

    print("AC Mobile Sync Completed - " + configs['last_ac_mobile_sync'] + '\n')

    # setup a completion email notifying Jason that a Month of Venue pushes has finished
    # sender = "kevin@matsongroup.com"
    # recipients = ["jason@matsongroup.com", "flygeneticist@gmail.com"]
    # header = 'From: %s\n' % sender
    # header += 'To: %s\n' % ", ".join(recipients)
    # header += 'Subject: Completed DAILY AC Mobile Push - SeatEngine AWS\n'
    # msg = header + \
    #     "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the daily AC Mobile syncs have completed:\n\nSUCCESS: %s\nERRORS:\nAdd Errors:%s\nUpdate Errors:%s\nList Errors:%s\nSSL Errors:%s\nOther Errors:%s\n\n" % (
    #         contact_count, len(contact_err['add']), len(contact_err['update']), len(contact_err['list']), len(contact_err['ssl']), len(contact_err['other']))
    # msg += "\n\n----- ERROR DETAILS -----\nContacts:\nAdd: %s\nUpdate: %s\nList: %s\nSSL: %s\nOther: %s\n" % (
    #     str(contact_err['add']), str(contact_err['update']), str(contact_err['list']), str(contact_err['ssl']), str(contact_err['other']))
    # msg += "\n\n----- END OF REPORT -----"
    # server = smtplib.SMTP('smtp.gmail.com', 587)
    # server.ehlo()
    # server.starttls()
    # server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
    # server.sendmail(sender, recipients, msg)
    # server.quit()


if __name__ == '__main__':
    active_campaign_sync()

