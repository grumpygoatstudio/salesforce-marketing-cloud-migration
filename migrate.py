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


def get_venue_shows(venue_id, pull_limit, header):
    url = "https://api-2.seatengine.com/api/venues/%s/events" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        events = json.loads(res.text)['data']
        shows = []
        for event in events:
            for show in event['shows']:
                if parse(show['start_date_time'], ignoretz=True) > pull_limit:
                    shows.append(show['id'])
        return shows
    else:
        return []


def get_show_information(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        show = json.loads(res.text)['data']
        # build out events and tickets objects
        event = {}
        event['id'] = str(show_id)
        event['venue_id'] = str(venue_id)
        event['name'] = str(show['event']['name']).strip().replace(",", " "),
        event['cancelled_at'] = str(show['event']["cancelled_at"])
        event['start_date_time'] =  str(show['start_date_time'])
        event["sold_out"] = show["sold_out"]
        return event
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
                'purchase_date': str(order['purchase_at']),
                'payment_method': payment_method,
                'booking_type': str(order['booking_type']),
                'order_total': 0,
            }

            for tix_type in order['tickets']:
                prices = [tix['price'] for tix in order['tickets'][tix_type]]
                # add ticket costs to ORDER total
                temp_order['order_total'] += sum(prices)
                # add tickets purchased to ORDERLINE
                temp_orderline = {
                    'id': str(order['order_number']) + "-%0.6d" % random.randint(0, 999999),
                    'order_number': str(order['order_number']),
                    'line_subtotal': sum(prices),
                    'ticket_price': prices[0],
                    'quantity': len(prices),
                    'ticket_name': str(tix_type).strip().replace(",", " "),
                    'event_id': str(event_id)
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
    data = {
        "venues": [1, 5, 6, 7, 21, 23, 53, 63, 131, 133, 297],
        "events": [],
        "orderlines": [],
        "orders": [],
        "contacts": []
    }

    # collect and process all data from API source
    for venue_id in data['venues']:
        print("Processing venue: " + str(venue_id))
        for show_id in get_venue_shows(venue_id, pull_limit, auth_header):
            show_info = get_show_information(venue_id, show_id, auth_header)
            if show_info:
                data['events'] += [show_info]
            show_orders = get_show_orders(venue_id, show_id, auth_header)
            if show_orders:
                order_info_objs = create_objects_from_orders(show_orders, show_id, pull_limit)
                data['orders'] += order_info_objs[0]
                data['orderlines'] += order_info_objs[1]
                data['contacts'] += order_info_objs[2]

    for dt in data:
        if dt != 'venues':
            try:
                file_path = os.path.join(dir_path, 'api-data', dt + '.csv')
                # remove old CSV file from filesystem if it exists
                os.remove(file_path)
            except OSError:
                pass
            # build csv files from the API source data collected
            the_file = open(file_path, "w")
            if len(data[dt]) > 0:
                writer = DictWriter(the_file, data[dt][0].keys())
                writer.writeheader()
                writer.writerows(data[dt])
            the_file.close()

    for dt in data:
        if dt != 'venues':
            file_path = os.path.join(dir_path, 'api-data', dt + '.csv')
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

    # write new datetime for last pulled time
    configs['last_pull'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)


if __name__ == '__main__':
    main()
