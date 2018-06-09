import os
import sys
import requests
import json
import collections
import csv
from datetime import datetime

reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def build_customer_json(connection, data):
    return {
      "ecomCustomer": {
        "connectionid": connection,
        "externalid": data['customerid'],
        "email": data['email']
      }
    }


def build_order_json(connection, data):
    return {
      "ecomOrder": {
        "externalid": data['externalid'],
        "source": "1",
        "email": data['email'],
        "orderNumber": data['orderNumber'],
        "orderProducts": [
          {
            "name": data['orderproduct_name'],
            "price": str(data['orderproduct_price']),
            "quantity": "1",
            # category is a placeholder for order show_id
            "category": data['orderproduct_category']
          } for ol in data
        ],
        "orderDate": data['orderDate'],
        # shippingMethod is a placeholder for order payment method
        "shippingMethod": data['shippingMethod'],
        "totalPrice": sum([ol['ticket_price'] for ol in data]),
        "currency": "USD",
        "connectionid": connection,
        "customerid": data['customerid']
      }
    }


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_crm_sync = configs["last_crm_sync"]
    orders_url = configs['Api-Url'] + 'ecomOrders'
    customers_url = configs['Api-Url'] + 'ecomCustomers'
    #venues = [(1,''), (5,''), (6,''), (7,''), (21,''), (23,''), (53,''), (63,''), (131,''), (133,''), (297,'')]
    venues = [(297, '2')]

    for venue_id, connection in venues:
        # remove old file if exists
        try:
            file_path = os.path.join(dir_path, 'crm-data', str(venue_id) + '.csv')
            os.remove(file_path)
        except OSError:
            pass
        # download CSV file from MySQL DB
        sql_cmd = """mysql %s -h %s -P %s -u %s --password=%s -e \"SELECT * FROM orders_mv WHERE venue_id = %s AND (sys_entry_date = '0000-00-00 00:00:00' OR sys_entry_date > %s);\" > %s""" % (
            configs['db_name'],
            configs['db_host'],
            configs['db_port'],
            configs['db_user'],
            configs['db_password'],
            str(venue_id),
            last_crm_sync,
            file_path
        )
        os.system(sql_cmd)

        #load data from pulled MySQL dump CSV
        with open(file_path, "r") as file_data:
            reader = csv.reader(file_data, delimiter=',')
            next(reader, None)
            # group the orderlines into orders
            orders = collections.defaultdict(list)
            for ol in reader:
                orders[ol.orderNumber].append(ol)
            for o, ols in orders:
                # build order and customer JSON
                customer_json = build_customer_json(connection, ols)
                order_json = build_order_json(connection, ols)
                # POST the JSON objects to AC server
                requests.post(customers_url, headers=auth_header, body=customer_json)
                requests.post(orders_url, headers=auth_header, body=order_json)

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Sync Completed - " + configs['last_crm_sync'])


if __name__ == '__main__':
    active_campaign_sync()
