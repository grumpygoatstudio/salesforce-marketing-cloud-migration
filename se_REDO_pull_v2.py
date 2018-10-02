import os
import sys
import requests
import json
import _mysql
import smtplib

from datetime import datetime, timedelta
from dateutil.parser import parse
from optparse import OptionParser


parser = OptionParser()
parser.add_option("-s", "--shows_pull", dest="shows_pull", type="string",
                  help="Run only script portion for fetching shows", metavar="shows")


reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def create_objects_from_orders(orders, show_id):
    customers_info = []
    orders_info = []
    orderlines_info = []
    sys_entry_time = "1999-12-31T23:59:59"  # Y2K ;)

    for order in orders:
        temp_cust = {}
        temp_cust['email_address'] = str(order['customer']['email']).replace("\r", "").strip().lower()
        temp_cust['name'] = str(order['customer']['name']).strip().replace("\"", "").replace(",", " ").replace('\'','`').strip()
        try:
            temp_cust['name_first'] = str(order["customer"]["first_name"]).strip().replace("\"", "").replace(",", " ").replace('\'','`').strip()
            temp_cust['name_last'] = str(order["customer"]["last_name"]).strip().replace("\"", "").replace(",", " ").replace('\'','`').strip()
        except Exception:
            temp_cust['name_first'] = ""
            temp_cust['name_last'] = ""
        temp_cust['sys_entry_date'] = sys_entry_time
        customers_info += [temp_cust]
    return (orders_info, orderlines_info, customers_info)


def sql_insert_contacts(db, contacts):
    stats = {"ok": 0, "err": 0}
    for c in [c for c in contacts if c['email_address'] not in ["", "None", None]]:
        try:
            query = '''INSERT INTO contacts (email_address, name, name_first, name_last, sys_entry_date)
                        VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                c['email_address'], c['name'], c['name_first'], c['name_last'], c['sys_entry_date']
            )
            db.query(query)
            stats['ok'] += 1
        except Exception as err:
            try:
                query = '''UPDATE contacts SET
                            name = \'%s\',
                            name_first = \'%s\',
                            name_last = \'%s\',
                            sys_entry_date = \'%s\'
                            WHERE email_address = \'%s\';''' % (
                    c['name'], c['name_first'], c['name_last'], c['sys_entry_date'], c['email_address']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT & UPDATE FAILED - CONTACT", c['email_address'], err2)
                stats['err'] += 1
    return stats


def get_shows_from_db(db, venue_id, pull_limit):
    query = """SELECT * FROM shows
                WHERE event_id IN (SELECT id FROM events WHERE venue_id = \'%s\')
                AND start_date_time >= \'%s\' - INTERVAL 90 DAY;
            """ % (venue_id, pull_limit.replace('T', ' '))
    db.query(query)
    return db.store_result()


def main(shows_pull=None):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}
    pull_limit = "2008-08-14T00:00:00"

    # connect to the AWS database
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    db.autocommit(True)

    # COLLECT AND PROCESS ALL DATA FROM API SOURCE
    venues = [297, 1, 5, 6, 7, 21, 23, 53, 63, 131, 133]
    data = {
        "events": [],
        "shows": [],
        "orderlines": [],
        "orders": [],
        "contacts": []
    }

    for venue_id in venues:
        # reset the venue data info
        data['orders'] = []
        data['orderlines'] = []
        data['contacts'] = []

        print("~~~~ UPDATING VENUE %s ~~~~" % venue_id)
        r = get_shows_from_db(db, venue_id, pull_limit)
        more_rows = True
        while more_rows:
            try:
                show = r.fetch_row(how=2)[0]
                show_id = show['shows.id']
                show_orders = get_show_orders(venue_id, show_id, auth_header)
                if show_orders:
                    order_info_objs = create_objects_from_orders(show_orders, show_id)
                    data['contacts'] += order_info_objs[2]
            except IndexError:
                more_rows = False

        # UPLOAD ALL DATA TO AWS RDS SERVER
        contacts_stats = sql_insert_contacts(db, data["contacts"])

        # setup a completion email
        sender = "kevin@matsongroup.com"
        recipients = ["flygeneticist@gmail.com", "jason@matsongroup.com"]
        header = 'From: %s\n' % sender
        header += 'To: %s\n' % ", ".join(recipients)
        header += 'Subject: Completed REDO CONTACTS NAMES SeatEngine PULL - SeatEngine AWS\n'
        msg = header + \
            "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the REDO SEATENGINE Contacts pull has completed:\n\n"
        # add details about venue pull to report email
        msg = msg + "-- VENUE %s -- \n" % (venue_id)
        msg = msg + "CONTACTS:\nSUCCESS: %s\nERRORS: %s\n\n" % (contacts_stats["ok"], contacts_stats["err"])

        # Send the venue report email out
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
        server.sendmail(sender, recipients, msg)
        server.quit()

    print("Data Pull Completed - 1999-12-31T23:59:59 Y2K ;)")

    # TRIGGER POST-PROCESSING FOR SQL TABLES
    sql_post_processing()

def sql_post_processing():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    # POST PROCESS ALL TABLES FOR FASTER QUERYING
    sql_cmd = """mysql %s -h %s -P %s -u %s --password=%s -e \"CALL refresh_processed_tables_now(@rc);\"""" % (
        configs['db_name'],
        configs['db_host'],
        configs['db_port'],
        configs['db_user'],
        configs['db_password']
    )
    os.system(sql_cmd)
    print("SQL Post-Processing Completed - " + configs['last_pull'])


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.shows_pull:
        main(shows_pull=True)
    else:
        main()
