import os
import sys
import requests
import json
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
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = str(data['contacts_mv.email_address'])
    d["id"] = None
    d["overwrite"] = "0"
    # all custom contacts fields
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_MONDAYS%,0]"] = str(data['contacts_mv.shows_attended_M'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_TUESDAYS%,0]"] = str(data['contacts_mv.shows_attended_T'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_WEDNESDAYS%,0]"] = str(data['contacts_mv.shows_attended_W'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_THURSDAYS%,0]"] = str(data['contacts_mv.shows_attended_R'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_FRIDAYS%,0]"] = str(data['contacts_mv.shows_attended_F'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_SATURDAYS%,0]"] = str(data['contacts_mv.shows_attended_S'])
    d["field[%NUMBER_OF_SHOWS_ATTENDED_ON_SUNDAYS%,0]"] = str(data['contacts_mv.shows_attended_U'])

    d["field[%FIRST_SHOW_ATTENDED_DATE%,0]"] = str(data['contacts_mv.first_show_attended'])
    d["field[%FIRST_SHOW_ATTENDED_NAME%,0]"] = str(data['contacts_mv.first_event_title'])
    # d['FIRST_EVENT_VENUE'] = str(data['contacts_mv.first_event_venue'])
    # d['LAST_EVENT_VENUE'] = str(data['contacts_mv.last_event_venue'])
    d["field[%LAST_SHOW_ATTENDED_DATE%,0]"] = str(data['contacts_mv.last_show_attended'])
    d["field[%LAST_SHOW_ATTENDED_NAME%,0]"] = str(data['contacts_mv.last_event_title'])

    d["field[%NEXT_SHOW_ATTENDING_DATE%,0]"] = str(data['contacts_mv.next_show_attending'])
    d["field[%NEXT_SHOW_ATTENDING_NAME%,0]"] = str(data['contacts_mv.next_event_title'])

    d["field[%LAST_ORDER_DATE%,0]"]= str(data['contacts_mv.last_ordered_date'])
    d["field[%LAST_COMP_SHOW_DATE%,0]"] = str(data['contacts_mv.last_comp_show_date'])
    d["field[%TOTAL_NUMBER_OF_COMP_TICKETS%,0]"] = str(data['contacts_mv.total_lifetime_comp_tickets'])
    d["field[%TOTAL_NUMBER_OF_PAID_TICKETS%,0]"] = str(data['contacts_mv.total_lifetime_paid_tickets'])
    d["field[%TOTAL_NUMBER_OF_COMP_ORDERS%,0]"] = str(data['contacts_mv.total_lifetime_comp_orders'])
    d["field[%TOTAL_NUMBER_OF_PAID_ORDERS%,0]"] = str(data['contacts_mv.total_lifetime_paid_orders'])

    d["field[%TOTAL_COMP_ORDER_COUNT_LAST_360_DAYS%,0]"] = str(data['contacts_mv.comp_orders_count_360'])
    d["field[%TOTAL_COMP_ORDER_COUNT_LAST_180_DAYS%,0]"] = str(data['contacts_mv.comp_orders_count_180'])
    d["field[%TOTAL_COMP_ORDER_COUNT_LAST_90_DAYS%,0]"] = str(data['contacts_mv.comp_orders_count_90'])
    d["field[%TOTAL_PAID_ORDERS_LAST_90_DAYS%,0]"] = str(data['contacts_mv.paid_orders_count_90'])
    d["field[%TOTAL_PAID_ORDERS_LAST_180_DAYS%,0]"] = str(data['contacts_mv.paid_orders_count_180'])
    d["field[%TOTAL_PAID_ORDERS_LAST_360_DAYS%,0]"] = str(data['contacts_mv.paid_orders_count_360'])
    d["field[%TOTAL_REVENUE_LAST_90_DAYS%,0]"] = str(data['contacts_mv.paid_orders_revenue_90'])
    d["field[%TOTAL_REVENUE_LAST_180_DAYS%,0]"] = str(data['contacts_mv.paid_orders_revenue_180'])
    d["field[%TOTAL_REVENUE_LAST_360_DAYS%,0]"] = str(data['contacts_mv.paid_orders_revenue_360'])

    d["field[%COUNT_OF_SPECIAL_EVENTS_ATTENDED%,0]"] = str(data['contacts_mv.count_shows_special'])
    d["field[%COUNT_OF_PRESENTS_SHOWS_ATTENDED%,0]"] = str(data['contacts_mv.count_shows_persents'])
    d["field[%AVERAGE_NUMBER_OF_PAID_TICKETS_PER_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_paid_order'])
    d["field[%AVERAGE_NUMBER_OF_TICKETS_PER_COMP_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_comp_order'])
    d["field[%AVERAGE_NUMBER_OF_DAYS_BETWEEN_PURCHASE_AND_EVENT_DATES%,0]"] = str(data['contacts_mv.avg_purchase_to_show_days'])
    d["field[%AVERAGE_TICKETS_PER_ORDER%,0]"] = str(data['contacts_mv.avg_tickets_per_order'])
    return d


def lookup_crm_id_by_api(url, data, auth_header):
    data["api_action"] = "contact_view_email"
    r = requests.post(url, headers=auth_header, data=data)
    if r.status_code == 200 and r.json()["result_code"] != 0:
        return r.json()["id"]
    else:
        return None


def update_contact_in_crm(url, auth_header, data, configs):
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if crm_id:
        data['id'] = crm_id
        r = requests.post(url, headers=auth_header, data=data)
        if r.status_code == 200 and r.json()["result_code"] != 0:
            print("SUCCESS: Updated contact via API", data['email'])
        else:
            print("Updating contact via API failed.")
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    last_crm_contacts_sync = configs["last_crm_contacts_sync"]
    contacts_url = "https://heliumcomedy.api-us1.com/admin/api.php"

    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    db.query("""SELECT * FROM contacts_mv WHERE email_address in (SELECT DISTINCT(email) FROM orders_mv WHERE email != '' AND venue_id = 297)""")
    r = db.store_result()
    more_rows = True
    while more_rows:
        contact_info = r.fetch_row(how=2)[0]
        if contact_info:
            contact_data = build_contact_data(contact_info, configs["Api-Token"])
            if contact_data:
                update_contact_in_crm(contacts_url, auth_header, contact_data, configs)
            else:
                print("BUILD CONTACT DATA FAILED!", str(contact_info["contacts_mv.email_address"]))
        else:
            more_rows = False

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_contacts_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Contacts Sync Completed - " + configs['last_crm_contacts_sync'])


if __name__ == '__main__':
    active_campaign_sync()
