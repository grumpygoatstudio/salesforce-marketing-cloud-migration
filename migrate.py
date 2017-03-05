import requests
import json 


def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)


def get_venue_shows(venue_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows" % venue_id
    res = requests.get(url, headers=header)
    if res.status_code == 200:
        data = json.loads(res.text)['data']
        print data
        return {show['event']['id']:[] for show in data}
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


def create_ticketevents_from_orders(orders):
    order_info = []
    for order in orders:
        temp = {
            'order_number': order['order_number'],
            'purchase_date': order['purchase_at'],
            'booking_type': order['booking_type'],
            'new_customer': order['customer']['new_customer'],
            'cust_id': order['customer']['id'],
            'name': order['customer']['name'],
            'email': order['customer']['email'],
            'order_total': 0,
            'tickets': {}
        }
        for tix_type in order['tickets']:
            prices = [tix['price'] for tix in order['tickets'][tix_type]]
            temp['tickets'][tix_type] = prices
            temp['order_total'] += sum(prices)
            # POST NEW ORDERLINE FOR TICKET TYPE
        order_info.append(temp)
    return order_info


def main():
    venues = [5] #[1, 5, 6, 7, 21, 23, 53, 63, 131, 133]
    auth_header = load_config()    
    salesforce_data = {}
    for venue in venues:
        shows = get_venue_shows(venue, auth_header)
        salesforce_data[venue] = shows if shows else []
        for show in salesforce_data[venue]:
            orders = get_show_orders(venue, show, auth_header)
            salesforce_data[venue][show] = create_ticketevents_from_orders(orders) if orders else []
    print salesforce_data


if __name__ == '__main__':
    main()
