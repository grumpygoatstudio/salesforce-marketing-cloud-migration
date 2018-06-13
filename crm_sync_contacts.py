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
    d = collections.OrderedDict()
    d["api_key"] = api_key
    d["api_action"] = "contact_edit"
    d["api_output"] = "json"
    d["email"] = str(data['contacts_mv.email_address'])
    d["id"] = None
    d["overwrite"] = "0"
    # all custom contacts fields
    # d['LAST_EVENT_TITLE'] = data['contacts_mv.last_event_title']
    # d['LAST_ORDERED_DATE'] = data['contacts_mv.last_ordered_date']
    # d['TOTAL_LIFETIME_COMP_TICKETS'] = data['contacts_mv.total_lifetime_comp_tickets']
    # d['PAID_ORDERS_COUNT_90'] = data['contacts_mv.paid_orders_count_90']
    # d['TOTAL_REVENUE'] = data['contacts_mv.total_revenue']
    # d['PAID_ORDERS_REVENUE_360'] = data['contacts_mv.paid_orders_revenue_360']
    # d['TOTAL_LIFETIME_PAID_TICKETS'] = data['contacts_mv.total_lifetime_paid_tickets']
    # d['AVG_TICKETS_PER_PAID_ORDER'] = data['contacts_mv.avg_tickets_per_paid_order']
    # d['LAST_EVENT_VENUE'] = data['contacts_mv.last_event_venue']
    # d['TOTAL_LIFETIME_COMP_ORDERS'] = data['contacts_mv.total_lifetime_comp_orders']
    # d['NEXT_SHOW_ATTENDING'] = data['contacts_mv.next_show_attending']
    # d['COMP_ORDERS_COUNT_360'] = data['contacts_mv.comp_orders_count_360']
    # d['FIRST_SHOW_ATTENDED'] = data['contacts_mv.first_show_attended']
    # d['COMP_ORDERS_COUNT_180'] = data['contacts_mv.comp_orders_count_180']
    # d['COMP_ORDERS_COUNT_90'] = data['contacts_mv.comp_orders_count_90']
    # d['FIRST_EVENT_TITLE'] = data['contacts_mv.first_event_title']
    # d['LAST_COMP_SHOW_DATE'] = data['contacts_mv.last_comp_show_date']
    # d['PAID_ORDERS_REVENUE_90'] = data['contacts_mv.paid_orders_revenue_90']
    # d['COUNT_SHOWS_SPECIAL'] = data['contacts_mv.count_shows_special']
    # d['COUNT_SHOWS_PERSENTS'] = data['contacts_mv.count_shows_persents']
    # d['FIRST_EVENT_VENUE'] = data['contacts_mv.first_event_venue']
    # d['PAID_ORDERS_COUNT_180'] = data['contacts_mv.paid_orders_count_180']
    # d['SHOWS_ATTENDED_M'] = data['contacts_mv.shows_attended_M']
    # d['SHOWS_ATTENDED_T'] = data['contacts_mv.shows_attended_T']
    # d['SHOWS_ATTENDED_W'] = data['contacts_mv.shows_attended_W']
    # d['SHOWS_ATTENDED_R'] = data['contacts_mv.shows_attended_R']
    # d['SHOWS_ATTENDED_F'] = data['contacts_mv.shows_attended_F']
    # d['SHOWS_ATTENDED_S'] = data['contacts_mv.shows_attended_S']
    # d['SHOWS_ATTENDED_U'] = data['contacts_mv.shows_attended_U']
    # d['PAID_ORDERS_REVENUE_180'] = data['contacts_mv.paid_orders_revenue_180']
    # d['PAID_ORDERS_COUNT_360'] = data['contacts_mv.paid_orders_count_360']
    # d['LAST_SHOW_ATTENDED'] = data['contacts_mv.last_show_attended']
    # d['AVG_TICKETS_PER_COMP_ORDER'] = data['contacts_mv.avg_tickets_per_comp_order']
    # d['AVG_PURCHASE_TO_SHOW_DAYS'] = data['contacts_mv.avg_purchase_to_show_days']
    # d['NEXT_EVENT_TITLE'] = data['contacts_mv.next_event_title']
    # d['AVG_TICKETS_PER_ORDER'] = data['contacts_mv.avg_tickets_per_order']
    # d['TOTAL_LIFETIME_PAID_ORDERS'] = data['contacts_mv.total_lifetime_paid_orders']
    return d


def lookup_crm_id_by_api(url, data, auth_header):
    data["api_action"] = "contact_view_email"
    r = requests.post(url, headers=auth_header, data=data)
    if r.status_code == 200 and r.json()["result_code"] != 0:
        return r.json()["id"]
    else:
        return None


# def lookup_crm_id_by_sql(email, configs):
#     db = _mysql.connect(user=configs['db_user'],
#                         passwd=configs['db_password'],
#                         port=configs['db_port'],
#                         host=configs['db_host'],
#                         db=configs['db_name'])
#     sql = """SELECT crm_id FROM crm_linker_contacts WHERE email = \'%s\'""" % (email)
#     db.query(sql)
#     r = db.store_result()
#     try:
#         crm_id = int(r.fetch_row()[0][0])
#     except Exception:
#         crm_id = None
#     db.close()
#     return crm_id


# def save_crm_id(email, crm_id, configs):
#     db = _mysql.connect(user=configs['db_user'],
#                         passwd=configs['db_password'],
#                         port=configs['db_port'],
#                         host=configs['db_host'],
#                         db=configs['db_name'])
#     sql = """INSERT INTO crm_linker_contacts SET email=\'%s\', crm_id=%s""" % (email, crm_id)
#     db.query(sql)
#     db.close()


def update_contact_in_crm(url, auth_header, data, configs):
    # crm_id = lookup_crm_id_by_sql(data["email"], configs)
    # if not crm_id:
    crm_id = lookup_crm_id_by_api(url, data, auth_header)
    if crm_id:
        # save_crm_id(data["email"], crm_id, configs)
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
        contact_info = r.fetch_row(how=2)[0]
        print(contact_info)
        if contact_info:
            # build contact JSON
            contact_data = build_contact_data(contact_info, configs["Api-Token"])
            if contact_data:
                # PUT/POST the JSON objects to AC server
                crm_id = update_contact_in_crm(contacts_url, auth_header, contact_data, configs)
                #print("CONTACT LOOKUP FAILED!", str(contact_data))
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
