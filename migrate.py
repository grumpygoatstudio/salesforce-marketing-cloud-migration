import requests
import json 


def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)


def get_venue_shows(venue_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows" % venue_id
    res = requests.get(url, headers=header)
    data = json.loads(res.text)['data']
    return {show['event']['id']:[] for show in data}


def get_show_orders(venue_id, show_id, header):
    url = "https://api-2.seatengine.com/api/venues/%s/shows/%s/willcall" % (venue_id, show_id)
    res = requests.get(url, headers=header)
    data = json.loads(res.text)
    order_info = []
    for order in data:
        order_info.append({
            'id': order['customer']['id'],
            'first_name': order['customer']['first_name'],
            'last_name': order['customer']['last_name'],
            'email': order['customer']['email'],
            'new_customer': order['customer']['new_customer'],
            'payment_method': order['payments'][0]['payment_method'],
            'order_number': order['order_number'],
            'price': order['price'],
            'purchase_at': order['purchase_at']
        })
    return order_info


def main():
    venues = [1] #, 5, 6, 7, 21, 23, 53, 63, 131, 133]
    auth_header = load_config()    
    salesforce_data = {}
    for venue in venues:
        salesforce_data[venue] = get_venue_shows(venue, auth_header)
        for show in salesforce_data[venue]:
            salesforce_data[venue][show] = get_show_orders(venue, show, auth_header)
    return salesforce_data

if __name__ == '__main__':
    main()
