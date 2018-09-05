import os
import sys
import requests
import json
import collections
import csv
import _mysql
import smtplib

from datetime import datetime, timedelta
from dateutil.parser import parse
from csv import DictWriter
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-b", "--backload", dest="backload", type="string",
                  help="Backload shows and events", metavar="backload")
parser.add_option("-s", "--sql", dest="sql", type="string",
                  help="Upload all CSV files to SQL server only", metavar="sql")
parser.add_option("-n", "--name", dest="names", type="string",
                  help="Fix missing names in database", metavar="names")
parser.add_option("-r", "--rebuild", dest="rebuild", type="string",
                  help="Rebuild all orders/orderlines in database", metavar="rebuild")
parser.add_option("-p", "--postprocess", dest="postprocess", type="string",
                  help="Run only postprocessing scripts", metavar="postprocess")

reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def get_venue_events_and_shows(venue_id, pull_limit, header):
    url = "https://api-2.seatengine.com/api/venues/%s/events" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)['data']
        events = []
        shows = []
        for event in data:
            temp_event = collections.OrderedDict()
            temp_event['id'] = event['id']
            temp_event['venue_id'] = str(venue_id)
            temp_event['name'] = event['name'].strip().replace("\"", "").replace(",", " ")
            temp_event['logo_url'] = event['image_url']
            events.append(temp_event)
            for show in event['shows']:
                if parse(show['start_date_time'], ignoretz=True) > pull_limit:
                    shows.append(show['id'])
        return (events, shows)
    else:
        return []

def get_venue_events_and_shows_from_list(venue_id, file_path, header):
    with open(file_path, "r") as file_data:
        f = csv.reader(file_data, delimiter=',')
        events = []
        shows = []
        for event in f:
            if event[0] != "event id":
                for show_id in event[1:]:
                    shows.append(show_id)
                url = "https://api-2.seatengine.com/api/venues/%s/events/%s" % (venue_id, event[0])
                res = requests.get(url, headers=header)
                if res.status_code == 200:
                    event_data = json.loads(res.text)['data']
                    temp_event = collections.OrderedDict()
                    temp_event['id'] = event_data['id']
                    temp_event['venue_id'] = str(venue_id)
                    temp_event['name'] = event_data['name'].strip().replace("\"", "").replace(",", " ")
                    temp_event['logo_url'] = event_data['image_url']
                    events.append(temp_event)
        return (events, shows)

def get_show_information(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        show = json.loads(res.text)['data']
        temp_show = collections.OrderedDict()
        temp_show['id'] = str(show_id)
        temp_show['event_id'] = show['event_id']
        temp_show['start_date_time'] = str(show['start_date_time'])
        temp_show['sold_out'] = show["sold_out"]
        temp_show['cancelled_at'] = str(show['event']["cancelled_at"])
        return temp_show
    else:
        return False


def get_show_orders(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s/willcall" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)
        return data
    else:
        return False


def create_objects_from_orders(orders, show_id, pull_limit, db):
    customers_info = []
    orders_info = []
    orderlines_info = []
    sys_entry_time = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

    for order in orders:
        temp_cust = collections.OrderedDict()
        # temp_cust['subscriber_key'] = str(order['customer']['id'])
        temp_cust['email_address'] = str(order['customer']['email']).replace("\r", "").strip().lower()
        temp_cust['name'] = str(order['customer']['name']).strip().replace("\"", "").replace(",", " ")
        try:
            temp_cust['name_first'] = str(order["delivery_data"]["first_name"]).strip().replace("\"", "").replace(",", " ")
            temp_cust['name_last'] = str(order["delivery_data"]["last_name"]).strip().replace("\"", "").replace(",", " ")
        except Exception:
            try:
                names = str(order['customer']['name']).strip().split(", ")
                temp_cust['name_first']=" ".join(names[1:]).replace("\"", "").replace(",", " ")
                temp_cust['name_last'] = names[0].replace("\"", "").replace(",", " ")
            except Exception:
                temp_cust['name_first'] = ""
                temp_cust['name_last'] = ""
        temp_cust['sys_entry_date'] = sys_entry_time
        try:
            payment_method = str(order["payments"][0]['payment_method'])
        except Exception:
            payment_method = ""

        temp_order = collections.OrderedDict()
        temp_order['id'] = str(order['id'])
        temp_order['show_id'] = str(show_id)
        temp_order['order_number'] = str(order['order_number'])
        temp_order['cust_id'] = str(order['customer']['id'])
        temp_order['email'] = str(order['customer']['email']).strip().lower()
        temp_order['phone'] = str(order['customer']['phone'])
        temp_order['purchase_date'] = str(order['purchase_at'])
        temp_order['payment_method'] = payment_method
        temp_order['booking_type'] = str(order['booking_type'])
        temp_order['order_total'] = 0
        temp_order['new_customer'] = str(order['customer']['new_customer'])
        temp_order['sys_entry_date'] = sys_entry_time
        temp_order['addons'] = "\t".join([str(a['name']) for a in order['addons']]) if order['addons'] != [] else ""

        for tix_type in order['tickets']:
            c = 1
            for tix in order['tickets'][tix_type]:
                temp_orderline = collections.OrderedDict()
                temp_orderline['id'] = str(order['order_number']) + "-%s" % c
                temp_orderline['order_number'] = str(order['order_number'])
                temp_orderline['ticket_name'] = str(tix_type).strip().replace(",", " ")
                temp_orderline['ticket_price'] = tix['price']
                temp_orderline['printed'] = tix['printed']
                temp_orderline['promo_code_id'] = tix['promo_code_id']
                temp_orderline['checked_in'] = tix['checked_in']
                # add tickets purchased to ORDERLINE
                orderlines_info += [temp_orderline]
                # add ticket cost to ORDER total
                try:
                    temp_order['order_total'] += int(tix['price'])
                except Exception:
                    pass
                c += 1
        orders_info += [temp_order]
        customers_info += [temp_cust]

    return (orders_info, orderlines_info, customers_info)


def sql_insert_events(db, events):
    stats = {"ok": 0, "err": 0}
    for e in events:
        try:
            query = '''UPDATE events SET
                        venue_id = \'%s\',
                        name = \'%s\',
                        logo_url = \'%s\'
                        WHERE id = \'%s\';''' % (
                e['venue_id'], e['name'], e['logo_url'], e['id']
            )
            db.query(query)
            stats['ok'] += 1
        except Exception as err:
            print("SQL UPDATE FAILED - EVENT - TRYING INSERT FALLBACK", e['id'], err)
            try:
                query = '''INSERT INTO events (id, venue_id, name, logo_url)
                            VALUES (\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                    e['id'], e['venue_id'], e['name'], e['logo_url']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT FAILED - EVENT", e['id'], err2)
                stats['err'] += 1
    return stats


def sql_insert_shows(db, shows):
    stats = {"ok": 0, "err": 0}
    for s in shows:
        try:
            query = '''UPDATE shows SET
                        event_id = \'%s\',
                        start_date_time = \'%s\',
                        sold_out = \'%s\',
                        cancelled_at = \'%s\'
                        WHERE id = \'%s\';''' % (
                s['event_id'], s['start_date_time'], s['sold_out'], s['cancelled_at'], s['id']
            )
            db.query(query)
            stats['ok'] += 1
        except Exception as err:
            print("SQL UPDATE FAILED - SHOW - TRYING INSERT FALLBACK", s['id'], err)
            try:
                query = '''INSERT INTO shows (id, event_id, start_date_time, sold_out, cancelled_at)
                            VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                    s['id'], s['event_id'], s['start_date_time'], s['sold_out'], s['cancelled_at']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT FAILED - SHOW", s['id'], err2)
                stats['err'] += 1
    return stats


def sql_insert_contacts(db, contacts):
    stats = {"ok": 0, "err": 0}
    for c in [c for c in contacts if c['email_address'] != ""]:
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
        except Exception as err:
            print("SQL UPDATE FAILED - CONTACT - TRYING INSERT FALLBACK", c['email_address'], err)
            try:
                query = '''INSERT INTO contacts (email_address, name, name_first, name_last, sys_entry_date)
                            VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                    c['email_address'], c['name'], c['name_first'], c['name_last'], c['sys_entry_date']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT FAILED - CONTACT", c['email_address'], err2)
                stats['err'] += 1
    return stats


def sql_insert_orders(db, orders):
    stats = {"ok": 0, "err": 0}
    for o in orders:
        try:
            query = '''UPDATE orders SET
                        id = \'%s\',
                        show_id = \'%s\',
                        order_number = \'%s\',
                        cust_id = \'%s\',
                        email = \'%s\',
                        phone = \'%s\',
                        purchase_date = \'%s\',
                        payment_method = \'%s\',
                        booking_type = \'%s\',
                        order_total = \'%s\',
                        new_customer = \'%s\',
                        sys_entry_date = \'%s\',
                        addons = \'%s\'
                        WHERE id = \'%s\';''' % (
                o['show_id'], o['order_number'], o['cust_id'], o['email'], o['phone'], o['purchase_date'], o['payment_method'], o['booking_type'], o['order_total'], o['new_customer'], o['sys_entry_date'], o['addons'], o['id']
            )
            db.query(query)
            stats['ok'] += 1
        except Exception as err:
            print("SQL UPDATE FAILED - ORDER - TRYING INSERT FALLBACK", o['id'], err)
            try:
                query = '''INSERT INTO orders (id, show_id, order_number, cust_id, email, phone, purchase_date, payment_method, booking_type, order_total, new_customer, sys_entry_date, addons)
                            VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                    o['id'], o['show_id'], o['order_number'], o['cust_id'], o['email'], o['phone'], o['purchase_date'], o['payment_method'], o['booking_type'], o['order_total'], o['new_customer'], o['sys_entry_date'], o['addons']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT FAILED - ORDER", o['id'], err2)
                stats['err'] += 1
    return stats


    def sql_insert_orderlines(db, orderlines):
    stats = {"ok": 0, "err": 0}
    for ol in orderlines:
        try:
            query = '''UPDATE orderlines SET
                        order_number = \'%s\',
                        ticket_name = \'%s\',
                        ticket_price = \'%s\',
                        printed = \'%s\',
                        promo_code_id = \'%s\',
                        checked_in = \'%s\'
                        WHERE id = \'%s\';''' % (
                ol['order_number'], ol['ticket_name'], ol['ticket_price'], ol['printed'], ol['promo_code_id'], ol['checked_in'], ol['id']
            )
            db.query(query)
            stats['ok'] += 1
        except Exception as err:
            print("SQL UPDATE FAILED - ORDERLINE - TRYING INSERT FALLBACK", ol['id'], err)
            try:
                query = '''INSERT INTO orderlines (id, order_number, ticket_name, ticket_price, printed, promo_code_id, checked_in)
                            VALUES (\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\');''' % (
                    ol['id'], ol['order_number'], ol['ticket_name'], ol['ticket_price'], ol['printed'], ol['promo_code_id'], ol['checked_in']
                )
                db.query(query)
                stats['ok'] += 1
            except Exception as err2:
                print("SQL INSERT FAILED - ORDERLINE", ol['id'], err2)
                stats['err'] += 1
    return stats


def main():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}
    pull_limit = parse(configs['last_pull'], ignoretz=True)
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    db.autocommit(True)

    venues = [133]
    data_types = ["events", "shows", "orderlines", "orders", "contacts"]

    # COLLECT AND PROCESS ALL DATA FROM API SOURCE
    for venue_id in venues:
        data = {
            "events": [],
            "shows": [],
            "orderlines": [],
            "orders": [],
            "contacts": []
        }
        # data['events'] += []
        shows = [80123]
        for show_id in shows:
            show_info = get_show_information(venue_id, show_id, auth_header)
            if show_info:
                data['shows'] += [show_info]
            show_orders = get_show_orders(venue_id, show_id, auth_header)
            if show_orders:
                order_info_objs = create_objects_from_orders(show_orders, show_id, pull_limit, db)
                data['orders'] += order_info_objs[0]
                data['orderlines'] += order_info_objs[1]
                data['contacts'] += order_info_objs[2]

        for dt in data_types:
            try:
                file_path = os.path.join(dir_path, 'api-data', dt + '-' + str(venue_id) + 'TEST.csv')
                os.remove(file_path)
            except OSError:
                pass

            # BUILD CSV FILES FROM THE API SOURCE DATA COLLECTED
            the_file = open(file_path, "w")
            if len(data[dt]) > 0:
                writer = DictWriter(the_file, data[dt][0].keys())
                writer.writeheader()
                writer.writerows(data[dt])
            the_file.close()

    # UPLOAD ALL SQL FILES TO AWS RDS SERVER
    events_stats = sql_insert_events(db, data["events"])
    shows_stats = sql_insert_shows(db, data["shows"])
    contacts_stats = sql_insert_contacts(db, data["contacts"])
    orders_stats = sql_insert_orders(db, data["orders"])
    orderlines_stats = sql_insert_orderlines(db, data["orderlines"])

    # sql_upload()

    # WRITE NEW DATETIME FOR LAST PULLED TIME
    print("TEST Data Pull Completed - " + configs['last_pull'])

    # TRIGGER POST-PROCESSING FOR SQL TABLES
    # sql_post_processing()

    # setup a completion email notifying Jason that a Month of Venue pushes has finished
    sender = "kevin@matsongroup.com"
    recipients = ["flygeneticist@gmail.com"] #"jason@matsongroup.com"
    header = 'From: %s\n' % sender
    header += 'To: %s\n' % ", ".join(recipients)
    header += 'Subject: Completed DAILY SeatEngine PULL - SeatEngine AWS\n'
    msg = header + \
        "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the daily SEATENGINE Orders, Events and Shows have completed:\n\n"
    msg = msg + "EVENTS:\nSUCCESS: %s\nERRORS: %s\n\n" % (events_stats["ok"], events_stats["err"])
    msg = msg + "SHOWS:\nSUCCESS: %s\nERRORS: %s\n\n" % (shows_stats["ok"], shows_stats["err"])
    msg = msg + "CONTACTS:\nSUCCESS: %s\nERRORS: %s\n\n" % (contacts_stats["ok"], contacts_stats["err"])
    # msg = msg + "ORDERS:\nSUCCESS: %s\nERRORS: %s\n\n" % (orders_stats["ok"], orders_stats["err"])
    # msg = msg + "ORDERLINES:\nSUCCESS: %s\nERRORS: %s\n\n" % (orderlines_stats["ok"], orderlines_stats["err"])
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
    server.sendmail(sender, recipients, msg)
    server.quit()


def sql_upload(backload=False):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    if backload == "rebuild":
        venues = [6, 7, 23,63, 297] # 1,5,21,53,133
        data_types = ["orderlines"]
    else:
        venues = [1, 5, 6, 7, 21, 23, 53, 63, 131, 133, 297]
        data_types = ["events", "shows", "orderlines", "orders", "contacts"]

    # UPLOAD ALL SQL FILES TO AWS RDS SERVER
    for venue_id in venues:
        for dt in data_types:
            if backload == "rebuild":
                file_path = os.path.join(
                    dir_path, dt + '-' + str(venue) + '-rebuild.csv')
            elif backload:
                file_path = os.path.join(dir_path, 'bdir', dt + '-' + str(venue_id) + '.csv')
            else:
                file_path = os.path.join(dir_path, 'api-data', dt + '-' + str(venue_id) + '.csv')
            # upload new CSV file to the MySQL DB
            sql_cmd = """mysql %s -h %s -P %s -u %s --password=%s -e \"LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\"' IGNORE 1 LINES; SHOW WARNINGS\"""" % (
                configs['db_name'],
                configs['db_host'],
                configs['db_port'],
                configs['db_user'],
                configs['db_password'],
                file_path,
                dt
            )
            os.system(sql_cmd)


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
    main()
