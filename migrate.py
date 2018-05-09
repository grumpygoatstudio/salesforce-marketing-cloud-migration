import os
import sys
import requests
import json
import random
import collections
import csv

from datetime import datetime
from dateutil.parser import parse
from csv import DictWriter
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-b", "--backload", dest="backload", type="string",
                  help="Backload shows and events", metavar="backload")

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

    for order in orders:
        # verify that order hasn't already been processed before
        if parse(order['purchase_at'], ignoretz=True) > pull_limit:
            temp_cust = collections.OrderedDict()
            temp_cust['subscriber_key'] = str(order['customer']['id'])
            temp_cust['name'] = str(order['customer']['name']).strip().replace("\"", "").replace(",", " ")
            temp_cust['email_address'] = str(order['customer']['email'])

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
            temp_order['addons'] = "\t".join([str(a['name']) for a in order['addons']]) if order['addons'] != [] else ""

            for tix_type in order['tickets']:
                prices = [tix['price'] for tix in order['tickets'][tix_type]]
                # add ticket costs to ORDER total
                temp_order['order_total'] += sum(prices)
                # add tickets purchased to ORDERLINE
                for tix in order['tickets'][tix_type]:
                    temp_orderline = collections.OrderedDict()
                    temp_orderline['id'] = str(order['order_number']) + "-%0.6d" % random.randint(0, 999999)
                    temp_orderline['order_number'] = str(order['order_number'])
                    temp_orderline['ticket_name'] = str(tix_type).strip().replace(",", " ")
                    temp_orderline['ticket_price'] = tix['price']
                    temp_orderline['printed'] = tix['printed']
                    temp_orderline['promo_code_id'] = tix['promo_code_id']
                    temp_orderline['checked_in'] = tix['checked_in']
                    orderlines_info += [temp_orderline]

            orders_info += [temp_order]
            customers_info += [temp_cust]

    return (orders_info, orderlines_info, customers_info)


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
        print("Processing venue: " + str(venue_id))
        file_path = os.path.join(dir_path, 'historic_lists', str(venue_id) + '.csv')
        events_and_shows = get_venue_events_and_shows_from_list(venue_id, file_path, auth_header)
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
    for venue_id in venues:
        for dt in data_types:
            file_path = os.path.join(dir_path, 'bdir', dt + '-' + str(venue_id) + '.csv')
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
        print("Processing venue: " + str(venue_id))
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
    for venue_id in venues:
        for dt in data_types:
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

    # WRITE NEW DATETIME FOR LAST PULLED TIME
    configs['last_pull'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.backload:
        backload()
    else:
        main()
