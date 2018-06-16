import os
import sys
import requests
import json
import random
import collections
import csv
import _mysql

from datetime import datetime
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


def create_objects_from_orders(orders, show_id, pull_limit):
    customers_info = []
    orders_info = []
    orderlines_info = []
    sys_entry_time = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

    for order in orders:
        # verify that order hasn't already been processed before
        if parse(order['purchase_at'], ignoretz=True) > pull_limit:
            temp_cust = collections.OrderedDict()
            temp_cust['subscriber_key'] = str(order['customer']['id'])
            temp_cust['name'] = str(order['customer']['name']).strip().replace("\"", "").replace(",", " ")
            temp_cust['email_address'] = str(order['customer']['email'])
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
            temp_order['email'] = str(order['customer']['email'])
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


def rebuild_orderlines():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}
    pull_limit = parse("1900-01-01T00:00:01", ignoretz=True)
    data_types = ["orderlines"]
    data = {"orderlines": []}
    venues = [1, 5, 6, 7, 21, 23, 53, 63, 131, 133, 297]
    for venue in venues:
        db = _mysql.connect(user=configs['db_user'],
                            passwd=configs['db_password'],
                            port=configs['db_port'],
                            host=configs['db_host'],
                            db=configs['db_name'])
        db.query("""SELECT DISTINCT venue_id, orderproduct_category FROM orders_mv WHERE venue_id = %s ORDER BY venue_id, orderproduct_category""" % venue)
        r = db.store_result()
        more_rows = 1
        while more_rows:
            try:
                show_info = r.fetch_row(how=2)[0]
                venue_id = show_info['orders_mv.venue_id']
                show_id = show_info['orders_mv.orderproduct_category']
                show_orders = get_show_orders(venue_id, show_id, auth_header)
                if show_orders:
                    order_info_objs = create_objects_from_orders(show_orders, show_id, pull_limit)
                    data['orderlines'] += order_info_objs[1]
                print("Venue %s - Processed: %s" % (venue_id, more_rows))
                more_rows += 1
            except IndexError:
                more_rows = False
        db.close()

        for dt in data_types:
            try:
                file_path = os.path.join(
                    dir_path, dt + '-' + str(venue) + '-rebuild.csv')
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
    sql_upload()
    print("Orderlines Rebuild Completed - " +
          datetime.today().strftime("%Y-%m-%dT%H:%M:%S"))


def backload():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}
    venues = [1, 5, 6, 7, 21, 23, 53, 63, 131, 133, 297]
    data_types = ["events", "shows", "orderlines", "orders", "contacts"]
    pull_limit = parse("1900-01-01T00:00:01", ignoretz=True)

    for venue_id in venues:
        data = {
            "events": [],
            "shows": [],
            "orderlines": [],
            "orders": [],
            "contacts": []
        }
        file_path = os.path.join(dir_path, 'historic_lists', str(venue_id) + '.csv')
        events_and_shows = get_venue_events_and_shows_from_list(venue_id, file_path, auth_header)
        data['events'] += events_and_shows[0]
        shows = events_and_shows[1]
        for show_id in shows:
            try:
                show_info = get_show_information(venue_id, show_id, auth_header)
                if show_info:
                    data['shows'] += [show_info]
                show_orders = get_show_orders(venue_id, show_id, auth_header)
                if show_orders:
                    order_info_objs = create_objects_from_orders(show_orders, show_id, pull_limit)
                    data['orders'] += order_info_objs[0]
                    data['orderlines'] += order_info_objs[1]
                    data['contacts'] += order_info_objs[2]
            except:
                pass

        for dt in data_types:
            try:
                file_path = os.path.join(dir_path, 'bdir', dt + '-' + str(venue_id) + '.csv')
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
    sql_upload(True, True)
    print("Old Data Pull Completed - " + datetime.today().strftime("%Y-%m-%dT%H:%M:%S"))

    # TRIGGER POST-PROCESSING FOR SQL TABLES
    sql_post_processing()


def main():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}

    if (configs['last_pull'] == "" or not configs['last_pull']):
        pull_limit = datetime.today()
    else:
        pull_limit = parse(configs['last_pull'], ignoretz=True)

    venues = [1, 5, 6, 7, 21, 23, 53, 63, 131, 133, 297]
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
        events_and_shows = get_venue_events_and_shows(venue_id, pull_limit, auth_header)
        data['events'] += events_and_shows[0]
        shows = events_and_shows[1]
        for show_id in shows:
            show_info = get_show_information(venue_id, show_id, auth_header)
            if show_info:
                data['shows'] += [show_info]
            show_orders = get_show_orders(venue_id, show_id, auth_header)
            if show_orders:
                order_info_objs = create_objects_from_orders(show_orders, show_id, pull_limit)
                data['orders'] += order_info_objs[0]
                data['orderlines'] += order_info_objs[1]
                data['contacts'] += order_info_objs[2]

        for dt in data_types:
            try:
                file_path = os.path.join(dir_path, 'api-data', dt + '-' + str(venue_id) + '.csv')
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
    sql_upload()

    # WRITE NEW DATETIME FOR LAST PULLED TIME
    configs['last_pull'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("Data Pull Completed - " + configs['last_pull'])

    # TRIGGER POST-PROCESSING FOR SQL TABLES
    sql_post_processing()


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


def missing_names():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "X-" in e}

    missing_names_file = os.path.join(dir_path, 'cust_missing_names_v2.csv')
    with open(missing_names_file, 'r') as data:
        reader = csv.reader(data, delimiter='\t')
        missing_items = {'1':[],'5':[],'21':[],'53':[],'133':[],'297':[]}
        next(reader,None)
        for l in reader:
            missing_items[l[4]].append((l[3],l[0],))
    for k,v in missing_items.items():
        unique_shows = list(set([i[0] for i in v]))
        missing_items[k] = dict.fromkeys(unique_shows, [])
        for show in unique_shows:
            missing_items[k][show] = [i[1] for i in v if i[0] == show]

    sql_stmnt = '''UPDATE contacts SET name="%s" WHERE subscriber_key="%s";\n'''
    with open('update_customers.sql', 'w') as out_file:
        for venue in missing_items:
            for show, cust_list in missing_items[venue].items():
                orders = get_show_orders(venue, show, auth_header)
                for o in orders:
                    if o['customer']['id'] in cust_list:
                        cust_name = str(o['customer']['name']).strip().replace("\"", "").replace(",", " ").replace("'", "\'")
                        out_file.write(sql_stmnt%(cust_name, o['customer']['id']))

    sql_cmd = """mysql %s -h %s -P %s -u %s --password=%s < update_customers.sql""" % (
        configs['db_name'],
        configs['db_host'],
        configs['db_port'],
        configs['db_user'],
        configs['db_password']
    )
    os.system(sql_cmd)


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.postprocess:
        sql_post_processing()
    else:
        if options.rebuild:
            if options.sql:
                sql_upload("rebuild")
            else:
                rebuild_orderlines()
        else:
            if options.names:
                missing_names()
            else:
                if options.sql:
                    if options.backload:
                        sql_upload(True)
                    else:
                        sql_upload()
                    sql_post_processing()
                elif options.backload:
                    backload()
                else:
                    main()
