import os
import sys
import requests
import json
import collections
import _mysql
import smtplib


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
            if r.status_code == 200 and r.json()["result_code"] != 0:
                return "success"
            else:
                print("ERROR: Updating contact via API failed.", data['email'])
                return "err_update"
        else:
            data["api_action"] = "contact_add"
            data.pop("id", None)
            r = requests.post(url, headers=auth_header, data=data)
            if r.status_code == 200 and r.json()["result_code"] != 0:
                return "success"
            else:
                print("ERROR: Creating contact via API failed.", data['email'])
                return "err_add"
    else:
        print("ERROR: Missing list. Create contact via API failed.",
              data['email'])
        return "err_list"
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    list_mappings = fetch_crm_list_mapping(configs)
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    venue_target = ("\'1, \'5,\' \'6,\' \'7,\' \'21\', \'23\', \'53\', \'63\', \'131\', \'133\', \'297\'")
    db.query("""SELECT  * FROM contacts_mv WHERE email_address != '' AND email_address IN (SELECT DISTINCT email FROM orders_mv WHERE venue_id in (%s));"""
    % venue_target)
    r = db.store_result()
    more_rows = True
    contact_err = {"list": 0, "add": 0, "update": 0, "other": 0}
    contact_count = 0

    while more_rows:
        try:
            contact_info = r.fetch_row(how=2)[0]
            last_venue = str(contact_info['contacts_mv.last_event_venue'])
            next_venue = str(contact_info['contacts_mv.next_event_venue'])
            if next_venue != "None":
                home_venue = next_venue
            elif last_venue != "None":
                home_venue = last_venue
            else:
                home_venue = ""

            contact_data = build_contact_data(
                contact_info, configs["Api-Token"], home_venue, list_mappings)
            if contact_data:
                updated = update_contact_in_crm(
                    url, auth_header, contact_data, configs, home_venue)
                if updated == 'success':
                    contact_count += 1
                else:
                    if updated == 'err_list':
                        contact_err['list'] += 1
                    elif updated == 'err_add':
                        contact_err['add'] += 1
                    elif updated == 'err_update':
                        contact_err['update'] += 1
                    else:
                        contact_err['other'] += 1
            else:
                contact_err['other'] += 1
                print("BUILD CONTACT DATA FAILED!", str(
                    contact_info["contacts_mv.email_address"]))
        except IndexError:
            more_rows = False
    # setup a completion email notifying Jason that a Month of Venue pushes has finished
    sender = "kevin@matsongroup.com"
    recipients = ["jason@matsongroup.com", 'flygeneticist@gmail.com']
    header = 'From: %s\n' % sender
    header += 'To: %s\n' % ", ".join(recipients)
    header += 'Subject: Completed a MASSIVE UPDATE of Contacts - SeatEngine AWS\n'
    msg = header + "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that a push to REDO Contact syncs have completed:\n\nTARGET VENUE: %s\n\nContacts pushed (SUCCESS qty: %s, ERROR qty: %s)\n" % (venue_target, contact_count, contact_err)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
    server.sendmail(sender, recipients, msg)
    server.quit()


if __name__ == '__main__':
    active_campaign_sync()
