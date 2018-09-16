import os
import sys
import requests
import json
import collections
import _mysql
import smtplib

from datetime import datetime, timedelta
from optparse import OptionParser
from time import sleep

parser = OptionParser()
parser.add_option("-p", "--postprocess", dest="postprocess", type="string",
                  help="Run only postprocessing scripts for show attendees in last 24 hours", metavar="postprocess")

reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def build_contact_data(data, api_key, last_venue, list_mappings):
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = str(data['contacts_mv.email_address']).replace("\r", "").strip()
    d["id"] = None
    d["overwrite"] = "0"
    d["first_name"] = str(data['contacts_mv.cust_name_first'])
    d["last_name"] = str(data['contacts_mv.cust_name_last'])
    # all custom contacts fields
    d["field[%SEAT_ENGINE_NAME%,0]"] = str(data['contacts_mv.cust_name'])
    d["field[%SEAT_ENGINE_PHONE%,0]"] = str(data['contacts_mv.phone'])
    d["field[%SE_TOTAL_ORDERS%,0]"] = str(data['contacts_mv.total_orders'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_MONDAYS%,0]"] = str(data['contacts_mv.shows_attended_M'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_TUESDAYS%,0]"] = str(data['contacts_mv.shows_attended_T'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_WEDNESDAYS%,0]"] = str(data['contacts_mv.shows_attended_W'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_THURSDAYS%,0]"] = str(data['contacts_mv.shows_attended_R'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_FRIDAYS%,0]"] = str(data['contacts_mv.shows_attended_F'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_SATURDAYS%,0]"] = str(data['contacts_mv.shows_attended_S'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_SUNDAYS%,0]"] = str(data['contacts_mv.shows_attended_U'])
    d["field[%FIRST_SHOW_ATTENDED_NAME%,0]"] = str(data['contacts_mv.first_event_title'])
    d["field[%LAST_SHOW_ATTENDED_NAME%,0]"] = str(data['contacts_mv.last_event_title'])
    d["field[%NEXT_SHOW_ATTENDING_NAME%,0]"] = str(data['contacts_mv.next_event_title'])
    first_show = str(data['contacts_mv.first_show_attended']).replace('T', ' ')
    last_show = str(data['contacts_mv.last_show_attended']).replace('T', ' ')
    next_show = str(data['contacts_mv.next_show_attending']).replace('T', ' ')
    last_order = str(data['contacts_mv.last_ordered_date']).replace('T', ' ')
    last_comp = str(data['contacts_mv.last_comp_show_date']).replace('T', ' ')
    d["field[%FIRST_SHOW_ATTENDED_DATE%,0]"] = "" if first_show == "None" else first_show
    d["field[%LAST_SHOW_ATTENDED_DATE%,0]"] = "" if last_show == "None" else last_show
    d["field[%NEXT_SHOW_ATTENDING_DATE%,0]"] = "" if next_show == "None" else next_show
    d["field[%LAST_ORDER_DATE%,0]"] = "" if last_order == "None" else last_order
    d["field[%LAST_COMP_SHOW_DATE%,0]"] = "" if last_comp == "None" else last_comp
    d["field[%TOTAL_NUMBER_OF_COMP_TICKETS%,0]"] = str(data['contacts_mv.total_lifetime_comp_tickets'])
    d["field[%TOTAL_NUMBER_OF_COMP_ORDERS%,0]"] = str(data['contacts_mv.total_lifetime_comp_orders'])
    d["field[%TOTAL_NUMBER_OF_PAID_TICKETS%,0]"] = str(data['contacts_mv.total_lifetime_paid_tickets'])
    d["field[%TOTAL_NUMBER_OF_PAID_ORDERS%,0]"] = str(data['contacts_mv.total_lifetime_paid_orders'])
    d["field[%TOTAL_COMP_ORDER_COUNT_LAST_360_DAYS%,0]"] = str(data['contacts_mv.comp_orders_count_360'])
    d["field[%TOTAL_PAID_ORDER_COUNT_LAST_360_DAYS%,0]"] = str(data['contacts_mv.paid_orders_count_360'])
    d["field[%TOTAL_REVENUE_LAST_360_DAYS%,0]"] = str(data['contacts_mv.paid_orders_revenue_360'])
    d["field[%TOTAL_COMP_ORDER_COUNT_LAST_90_DAYS%,0]"] = str(data['contacts_mv.comp_orders_count_90'])
    d["field[%TOTAL_PAID_ORDER_COUNT_LAST_90_DAYS%,0]"] = str(data['contacts_mv.paid_orders_count_90'])
    d["field[%TOTAL_REVENUE_LAST_90_DAYS%,0]"] = str(data['contacts_mv.paid_orders_revenue_90'])
    d["field[%SE_TOTAL_REVENUE%,0]"] = str(data['contacts_mv.total_revenue'])
    d["field[%COUNT_OF_SPECIAL_EVENTS_ATTENDED%,0]"] = str(data['contacts_mv.count_shows_special'])
    d["field[%COUNT_OF_PRESENTS_SHOWS_ATTENDED%,0]"] = str(data['contacts_mv.count_shows_persents'])
    d["field[%AVERAGE_NUMBER_OF_PAID_TICKETS_PER_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_paid_order'])
    d["field[%AVERAGE_NUMBER_OF_TICKETS_PER_COMP_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_comp_order'])
    d["field[%AVERAGE_NUMBER_OF_DAYS_BETWEEN_PURCHASE_AND_EVENT_DATES%,0]"] = str(data['contacts_mv.avg_purchase_to_show_days'])
    d["field[%AVERAGE_TICKETS_PER_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_order'])

    if not options.postprocess and last_venue not in ["None", ""]:
        list_id = list_mappings[last_venue]
        field = "p[%s]" % list_id
        d[field] = list_id
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


def update_contact_in_crm(url, auth_header, data, configs, last_venue):
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if last_venue not in ["None", ""]:
        if crm_id:
            data['id'] = crm_id
            r = requests.post(url, headers=auth_header, data=data)
            if r.status_code == 200:
                try:
                    if r.json()["result_code"] != 0:
                        return "success"
                    else:
                        print("ERROR: Updating contact via API failed.", data['email'])
                        return "err_update"
                except Exception as e:
                    print("ERROR: Other error.", data['email'], e)
                    return 'err_other'
            else:
                print("ERROR: Updating contact via API failed.", data['email'])
                return "err_update"
        else:
            data["api_action"] = "contact_add"
            data.pop("id", None)
            r = requests.post(url, headers=auth_header, data=data)
            if r.status_code == 200:
                try:
                    if r.json()["result_code"] != 0:
                        return "success"
                    else:
                        print("ERROR: Creating contact via API failed.", data['email'])
                        return "err_add"
                except Exception as e:
                    print("ERROR: Other error.", data['email'], e)
                    return 'err_other'
            else:
                print("ERROR: Creating contact via API failed.", data['email'])
                return "err_add"
    else:
        print("ERROR: Missing list. Create contact via API failed.", data['email'])
        return "err_list"
    return crm_id


def active_campaign_sync(postprocess=False):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_crm_contacts_sync = configs["last_crm_contacts_sync"]
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    list_mappings = fetch_crm_list_mapping(configs)
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])

    if postprocess:
        print("~~~~~ POST-PROCESSING CONTACTS ~~~~~")
        d360 = (datetime.now() - timedelta(days=360)).strftime("%Y-%m-%dT%H:%M:%S").replace('T', ' ')
        d90 = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%S").replace('T', ' ')
        db.query(
        """SELECT  * FROM contacts_mv WHERE email_address != '' AND email_address IN (SELECT DISTINCT email FROM orders_mv WHERE orderDate BETWEEN \'%s\' AND \'%s\');"""
        % (last_crm_contacts_sync.replace('T', ' '), d360, d90))
    else:
        print("~~~~~ PROCESSING CONTACTS ~~~~~")
        db.query(
        """SELECT * FROM contacts_mv WHERE email_address != ''
                AND (sys_entry_date > \'%s\' OR
                email_address in (SELECT DISTINCT email FROM orders_mv
                WHERE orderproduct_category IN (SELECT id FROM shows_processed
                WHERE start_date_formatted BETWEEN \'%s\' AND NOW()
            )));"""
        % (last_crm_contacts_sync.replace('T', ' '), last_crm_contacts_sync.replace('T', ' ')))

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
        last_venue = str(contact_info['contacts_mv.last_event_venue'])
        next_venue = str(contact_info['contacts_mv.next_event_venue'])
        if next_venue != "None":
            home_venue = next_venue
        elif last_venue != "None":
            home_venue = last_venue
        else:
            home_venue = ""
        try:
            contact_data = build_contact_data(contact_info, configs["Api-Token"], home_venue, list_mappings)
            if contact_data:
                updated = update_contact_in_crm(
                    url, auth_header, contact_data, configs, home_venue)
                if updated == 'success':
                    contact_count += 1
                else:
                    if updated == 'err_list':
                        contact_err['list'].append(str(contact_info['contacts_mv.email_address']))
                    elif updated == 'err_add':
                        contact_err['add'].append(str(contact_info['contacts_mv.email_address']))
                    elif updated == 'err_update':
                        contact_err['update'].append(str(contact_info['contacts_mv.email_address']))
                    else:
                        contact_err['other'].append(str(contact_info['contacts_mv.email_address']))
        except requests.exceptions.SSLError:
            contact_err["ssl"].append(str(contact_info['contacts_mv.email_address']))
        except Exception:
            contact_err['other'].append(str(contact_info['contacts_mv.email_address']))
            print("BUILD CONTACT DATA FAILED!", str(contact_info["contacts_mv.email_address"]))

        if contact_count % 500 == 0:
            print('Check in - #%s' % contact_count)

        if contact_count % chunk_size == 0:
            chunk_num += 1
            print("Done chunk(#%s)! Sleeping for 30 min to avoid SSL issues..." % chunk_num)
            sleep(1800) # sleep for 30 min to avoid SSL Errors

    if not postprocess:
        # WRITE NEW DATETIME FOR LAST CRM SYNC
        d = datetime.now()
        configs['last_crm_contacts_sync'] = d.strftime("%Y-%m-%dT%H:%M:%S")
        write_config(configs, dir_path)
        print("CRM Contacts Sync Completed - " + configs['last_crm_contacts_sync'] + '\n')

        # setup a completion email notifying Jason that a Month of Venue pushes has finished
        sender = "kevin@matsongroup.com"
        recipients = ["jason@matsongroup.com", 'flygeneticist@gmail.com']
        header = 'From: %s\n' % sender
        header += 'To: %s\n' % ", ".join(recipients)
        header += 'Subject: Completed DAILY Contacts Push - SeatEngine AWS\n'
        msg = header + \
            "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the daily CRM Contact syncs have completed:\nSUCCESS: %s\n\nERRORS:\nAdd Errors:%s\nUpdate Errors:%s\nList Errors:%s\nSSL Errors:%s\nOther Errors:%s\n\n" % (
                contact_count, len(contact_err['add']), len(contact_err['update']), len(contact_err['list']), len(contact_err['ssl']), len(contact_err['other']))
        msg += "\n\n----- ERROR DETAILS -----\nContacts:\nAdd: %s\nUpdate: %s\nList: %s\nSSL: %s\nOther: %s\n" % (
            str(contact_err['add']), str(contact_err['update']), str(contact_err['list']), str(contact_err['ssl']), str(contact_err['other']))
        msg += "\n\n----- END OF REPORT -----"
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
        server.sendmail(sender, recipients, msg)
        server.quit()
    else:
        print("CRM Post-Attendees Contacts Sync Completed - " + configs['last_crm_contacts_sync'] + '\n')


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.postprocess:
        active_campaign_sync(True)
    else:
        active_campaign_sync()
