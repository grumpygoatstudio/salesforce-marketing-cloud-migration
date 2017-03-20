import requests
import json
from datetime import datetime
from dateutil.parser import parse
from csv import DictWriter
import paramiko
import numpy as np
import random


def write_config(config):
    with open('config.json', 'r') as f:
        json.dump(config, f)


def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)


def get_venue_shows(venue_id, pull_limit, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)['data']
        return [show['event']['id'] for show in data if parse(show['start_date_time']) > pull_limit]
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
        event['name'] = str(show['event']['name'])
        event['cancelled_at'] = str(show['event']["cancelled_at"])
        event['next_show'] =  str(show["start_date_time"])
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
        # verify that linking customer ID is present && order hasn't already been processed before
        if order['customer']['email'] and parse(order['purchase_at']) > pull_limit:
            temp_cust = {
                'subscriber key': str(order['customer']['id']),
                'name': str(order['customer']['name']),
                'email address': str(order['customer']['email']),
                'new customer': str(order['customer']['new_customer'])
            }

            try:
                payment_method = str(order["payments"][0]['payment_method'])
            except:
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

            temp_orderlines = []

            for tix_type in order['tickets']:
                prices = [tix['price'] for tix in order['tickets'][tix_type]]
                # add ticket costs to ORDER total
                temp_order['order_total'] += sum(prices)
                # add tickets purchased to ORDERLINE
                temp_orderline = {
                    'id': str(order['order_number']) + "-%0.6d" % random.randint(0,999999),
                    'order_number': str(order['order_number']),
                    'line_subtotal': sum(prices),
                    'ticket_price': prices[0],
                    'quantity': len(prices),
                    'ticket_name': str(tix_type),
                    'event_id': str(event_id)
                }
                orderlines_info += [temp_orderline]

            orders_info += [temp_order]
            customers_info += [temp_cust]

    return (orders_info, orderlines_info, customers_info)


def main():
    configs = load_config()
    auth_header = {e:configs[e] for e in configs if "X-" in e}

    if (configs['last_pull'] == "" or not configs['last_pull']):
        pull_limit = datetime.today()
    else:
        pull_limit = parse(configs['last_pull'])

    data = {
        "venues": [1, 5, 6, 7, 21, 23, 53, 63, 131, 133],
        "events": [],
        "orderlines": [],
        "orders": [],
        "contacts": []
    }

    # collect and process all data from API source
    for venue_id in data['venues']:
        print "Processing venue: " + str(venue_id)
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
            # remove old CSV file from filesystem if it exists
            try:
                os.remove("SE_%s.csv" % dt)
            except OSError:
                pass
            # build csv files from the API source data collected
            the_file = open("SE_%s.csv" % dt, "w")
            writer = DictWriter(the_file, data[dt][0].keys())
            writer.writeheader()
            unique = list(np.unique(np.array(data[dt])))
            writer.writerows(unique)
            the_file.close()

            # SFTP push CSV file up to SalesForce Import endpoint folder
            t = paramiko.Transport((configs['host'], configs['port']))
            t.connect(username=configs['username'],password=configs['password'])
            with paramiko.SFTPClient.from_transport(t) as sftp:
                sftp.put('%s/SE_%s.csv' % (configs['source'], dt),'/Import/SE_%s.csv' % dt)


if __name__ == '__main__':
    main()
