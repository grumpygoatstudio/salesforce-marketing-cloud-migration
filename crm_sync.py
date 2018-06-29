import os
import sys
import requests
import json
import collections
import _mysql

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
    try:
        cust_data = {
          "ecomCustomer": {
            "connectionid": connection,
            "externalid": str(data[0]["orders_mv.customerid"]),
            "email": data[0]["orders_mv.email"]
          }
        }
    except Exception:
        cust_data = None
    return cust_data


def build_order_json(connection, crm_id, data):
    return {
      "ecomOrder": {
        "externalid": str(data[0]["orders_mv.externalid"]),
        "source": 0,
        "email": data[0]["orders_mv.email"],
        "orderNumber": str(data[0]["orders_mv.orderNumber"]),
        "orderProducts": [
          {
            # name is a placeholder for the "<show number> - <event name> - <ticket type>"
            "name": str(ol["orders_mv.orderproduct_category"]) + " - " + str(unicode(ol["orders_mv.event_name"], errors='ignore')) + " - " + ol["orders_mv.orderproduct_name"],
            "price": int(ol["orders_mv.orderproduct_price"]),
            "quantity": 1,
            # category is a placeholder for ticket type
            "category": ol["orders_mv.orderproduct_name"]
          } for ol in data
        ],
        "orderDate": str(data[0]["orders_mv.orderDate"]),
        # shippingMethod is a placeholder for order payment method
        "shippingMethod": data[0]["orders_mv.shippingMethod"],
        "totalPrice": str(data[0]["orders_mv.totalPrice"]),
        "currency": "USD",
        "connectionid": int(connection),
        "customerid": int(crm_id)
      }
    }


def lookup_crm_id(se_id, venue_id, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])

    sql = """SELECT crm_id FROM crm_linker_custs WHERE se_id = \'%s\' AND venue_id = %s""" % (se_id, venue_id)
    db.query(sql)
    r = db.store_result()
    try:
        crm_id = int(r.fetch_row()[0][0])
    except Exception:
        crm_id = None
    db.close()
    return crm_id


def save_crm_id(se_id, venue_id, crm_id, configs):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    sql = """INSERT INTO crm_linker_custs SET se_id=\'%s\', venue_id=%s, crm_id=%s""" % (se_id, venue_id, crm_id)
    db.query(sql)
    db.close()


def post_object_to_crm(url, auth_header, data, venue_id, configs, obj_type='ecomCustomer'):
    crm_id = None
    se_id = data[obj_type]["externalid"]
    r = requests.post(url, headers=auth_header, data=json.dumps(data))
    if r.status_code != 201:
        if obj_type == 'ecomCustomer':
            if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
                crm_id = lookup_crm_id(se_id, venue_id, configs)
    else:
        if obj_type == 'ecomCustomer':
            try:
                print("New Customer Created", se_id, venue_id, int(crm_id))
                crm_id = r.json()[obj_type]["id"]
                save_crm_id(se_id, venue_id, int(crm_id), configs)
            except Exception:
                pass
        else:
            print("New Order Created", se_id, venue_id)
    return crm_id


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_crm_sync = configs["last_crm_sync"]
    orders_url = configs['Api-Url'] + 'ecomOrders'
    customers_url = configs['Api-Url'] + 'ecomCustomers'
    venues = [(297, '2'), (1, '3'), (5, '4'), (6, '5'), (7, '6'), (21, '7'), (23, '10'),
              (53, '11'), (63, '12'), (131, '9'), (133, '8')]
    new_venues = [1,5,6,7,21,23,53,63,131,133]

    for venue_id, connection in venues:
        print("~~~~~ PROCESSING ORDERS FOR VENUE #%s ~~~~~" % venue_id )
        # download CSV file from MySQL DB
        db = _mysql.connect(user=configs['db_user'],
                            passwd=configs['db_password'],
                            port=configs['db_port'],
                            host=configs['db_host'],
                            db=configs['db_name'])
        if venue_id in new_venues:
            sql = """SELECT * FROM orders_mv WHERE venue_id = %s AND(sys_entry_date = '0000-00-00 00:00:00' OR sys_entry_date > \'%s\') AND email != '' AND customerid != 'None'""" % (
                str(venue_id), last_crm_sync.replace('T', ' ')
            )
        else:
            sql = """SELECT * FROM orders_mv WHERE venue_id = %s AND sys_entry_date > \'%s\' AND email != ''AND customerid != 'None'""" % (
                str(venue_id), last_crm_sync.replace('T', ' ')
            )
        db.query(sql)
        r = db.store_result()
        # group the orderlines into orders
        orders = collections.defaultdict(list)
        more_rows = True
        while more_rows:
            try:
                ol = r.fetch_row(how=2)[0]
                orders[ol["orders_mv.email"]].append(ol)
            except IndexError:
                more_rows = False
        db.close()
        crm_postings = []
        print("~~ POSTING CUSTOMERS ~~")
        for o in orders:
            ols = orders[o]
            # build order and customer JSON and POST the JSON objects to AC server
            customer_json = build_customer_json(connection, ols)
            if customer_json:
                crm_id = post_object_to_crm(customers_url, auth_header, customer_json, venue_id, configs)
                crm_postings += (crm_id, ols,)
            else:
                print("BUILD CUSTOMER JSON FAILED!", str(ols))

        crm_orders = []
        for i in crm_postings:
            if i[0]:
                try:
                    crm_orders += build_order_json(connection, str(i[0]), i[1])
                except:
                    print("BUILD ORDER JSON FAILED!", str(ols[0]["orders_mv.orderNumber"]))

        # post all valid orders to the CRM in one go
        print("~~ POSTING ORDERS PAYLOAD ~~")
        post_object_to_crm(orders_url, auth_header,
                            crm_orders, venue_id, configs, 'ecomOrder')


    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Sync Completed - " + configs['last_crm_sync'])


if __name__ == '__main__':
    active_campaign_sync()
