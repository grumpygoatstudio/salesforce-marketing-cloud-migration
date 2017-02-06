import requests
import json 


def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)


def get_venue_shows(venue_id):
    url = "/api/venues/%s/shows" % venue_id
    res = requests(url, header=auth)
    return json.dumps(res)


def get_show_orders(venue_id, show_id):
    url = "seatengine.com/api/venues/%s/shows/%s/willcall" % (venue_id, show_id)


def main():
    pass


venue_ids = [1, 5, 6, 7, 21, 23, 53, 63, 131, 133]
auth = load_config()
print auth['X-User-Token']

if __name__ == '__main__':
    main()
