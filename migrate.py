import os
import sys
import requests
import json
from datetime import datetime
from dateutil.parser import parse
from csv import DictWriter
import random


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
            # build event objects
            events.append({
                'id': event['id'],
                'venue_id': str(venue_id),
                'name': event['name'].strip().replace("\"", "").replace(",", " "),
                'logo_url': event['image_url'],
            })
            for show in event['shows']:
                if parse(show['start_date_time'], ignoretz=True) > pull_limit:
                    shows.append(show['id'])
        return (events, shows)
    else:
        return []


def get_show_information(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        show = json.loads(res.text)['data']
        return {
            'id': str(show_id),
            'event_id': show['event_id'],
            'start_date_time': str(show['start_date_time']),
            'sold_out': show["sold_out"],
            'cancelled_at': str(show['event']["cancelled_at"])
        }
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


def create_objects_from_orders(orders, event_id, pull_limit):
    customers_info = []
    orders_info = []
    orderlines_info = []

    for order in orders:
        # verify that order hasn't already been processed before
        if parse(order['purchase_at'], ignoretz=True) > pull_limit:
            temp_cust = {
                'subscriber_key': str(order['customer']['id']),
                'name': str(order['customer']['name']).strip().replace("\"", "").replace(",", " "),
                'email_address': str(order['customer']['email']),
                'new-customer': str(order['customer']['new_customer'])
            }

            try:
                payment_method = str(order["payments"][0]['payment_method'])
            except Exception:
                payment_method = ""

            temp_order = {
                'id': str(order['id']),
                'order_number': str(order['order_number']),
                'cust_id': str(order['customer']['id']),
                'email': str(order['customer']['email']),
                'phone': str(order['customer']['phone']),
                'purchase_date': str(order['purchase_at']),
                'payment_method': payment_method,
                'booking_type': str(order['booking_type']),
                'order_total': 0,
                'new_customer': str(order['customer']['new_customer']),
                'addons': ", ".join([str(a) for a in order['addons']])
            }

            for tix_type in order['tickets']:
                prices = [tix['price'] for tix in order['tickets'][tix_type]]
                # add ticket costs to ORDER total
                temp_order['order_total'] += sum(prices)
                # add tickets purchased to ORDERLINE
                for tix in order['tickets'][tix_type]:
                    temp_orderline = {
                        'id': str(order['order_number']) + "-%0.6d" % random.randint(0, 999999),
                        'order_number': str(order['order_number']),
                        'ticket_name': str(tix_type).strip().replace(",", " "),
                        'ticket_price': tix['price'],
                        'printed': tix['printed'],
                        'promo_code_id': tix['promo_code_id'],
                        'checked_in': tix['checked_in'],
                    }
                    orderlines_info += [temp_orderline]

            orders_info += [temp_order]
            customers_info += [temp_cust]

    return (orders_info, orderlines_info, customers_info)


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
        events = events_and_shows[0]
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
    main()
