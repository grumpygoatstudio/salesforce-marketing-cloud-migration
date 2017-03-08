import requests
import json 
from datetime import datetime
from csv import DictWriter
import pysftp


def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)


def get_venue(venue_id, headers):
    url = "https://api-2.seatengine.com/api/venues/" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)['data']
    else:
        return False


def get_venue_shows(venue_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)['data']
        return [show['event']['id'] for show in data]
    else:
        return []


def get_show_information(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        show = json.loads(res.text)['data']
        # build out events and tickets objects
        event = show['event']   
        event['id'] = show['id']
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


def create_objects_from_orders(orders, event_id, venue_id):
    customers_info = []
    orders_info = []
    orderlines_info = []

    for order in orders:
        temp_cust = {
            'cust_id': str(order['customer']['id']),
            'name': str(order['customer']['name']),
            'email': str(order['customer']['email']),
            'new_customer': str(order['customer']['new_customer'])
        }

        temp_order = {
            'id': str(order['id']),
            'order_number': str(order['order_number']),
            'cust_id': str(order['customer']['id']),
            'purchase_date': str(order['purchase_at']),
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
                'order_number': str(order['order_number']),
                'line_subtotal': sum(prices),
                'ticket_price': prices[0],
                'qty': len(prices),
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
    data = {
        "venues": [5, 1, 5, 6, 7, 21, 23, 53, 63, 131, 133],
        "events": [],
        "orderlines": [],
        "orders": [],
        "customers": []
    }

    # collect and process all data from API source
    for venue_id in data['venues']:
        for show_id in get_venue_shows(venue_id, auth_header):
            show_info = get_show_information(venue_id, show_id, auth_header)
            if show_info:
                data['events'] += [show_info]
            show_orders = get_show_orders(venue_id, show_id, auth_header)
            if show_orders:
                order_info_objs = create_objects_from_orders(show_orders, show_id, venue_id)
                data['orders'] += order_info_objs[0]
                data['orderlines'] += order_info_objs[1]
                data['customers'] += order_info_objs[2]

    # build salesforce ready csv files from the API source data collected
    for dt in data:
        if dt != 'venues':
            the_file = open("SE_%s.csv" % dt, "w")
            writer = DictWriter(the_file, data[dt][0].keys())
            writer.writeheader()
            writer.writerows(data[dt])
            the_file.close()

    # push CSV files up to SalesForce up to Import endpoint folder
    srv = pysftp.Connection(host=configs['SE-url'], 
                            username=configs['SE-user'],
                            password=configs['SE-pass'],
                            log="./temp/pysftp.log")
    with srv.cd('Import'):
        srv.put('SE_*.csv')
        srv.close()


if __name__ == '__main__':
    main()
