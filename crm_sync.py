import os
import sys
import requests
import json
import collections
import csv
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
            "externalid": str(data[0][6]),
            "email": data[0][2]
          }
        }
    except Exception:
        cust_data = None
    return cust_data


def build_order_json(connection, crm_id, data):
    return {
      "ecomOrder": {
        "externalid": str(data[0][1]),
        "email": data[0][2],
        "orderNumber": str(data[0][3]),
        "orderProducts": [
          {
            # name is a placeholder for the "<show number> - <event name>"
            "name": str(ol[10]) + " - " + str(unicode(ol[-3], errors='ignore')),
            "price": str(ol[9]),
            "quantity": "1",
            # category is a placeholder for ticket type
            "category": ol[8]
          } for ol in data
        ],
        "orderDate": str(data[0][4]),
        # shippingMethod is a placeholder for order payment method
        "shippingMethod": data[0][7],
        "totalPrice": str(sum([int(ol[9]) for ol in data])),
        "currency": "USD",
        "connectionid": connection,
        "customerid": crm_id
      }
    }


def lookup_crm_id(se_id, venue_id, configs, t):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    if t == 'o':
        sql = """SELECT crm_id FROM crm_linker_orders WHERE se_id = \'%s\' AND venue_id = %s""" % (se_id, venue_id)
    else:
        sql = """SELECT crm_id FROM crm_linker_custs WHERE se_id = \'%s\' AND venue_id = %s""" % (se_id, venue_id)
    db.query(sql)
    r = db.store_result()
    try:
        crm_id = int(r.fetch_row()[0][0])
    except Exception:
        crm_id = None
        print("CRM_ID LOOKUP FAILED!!", se_id, venue_id)
    db.close()
    return crm_id


def save_crm_id(se_id, venue_id, crm_id, configs, t):
    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])
    if t == 'o':
        sql = """INSERT INTO crm_linker_orders SET se_id=\'%s\', venue_id=%s, crm_id=%s""" % (se_id, venue_id, crm_id)
    else:
        sql = """INSERT INTO crm_linker_custs SET se_id=\'%s\', venue_id=%s, crm_id=%s""" % (se_id, venue_id, crm_id)
    db.query(sql)
    db.close()


def post_order_to_crm(url, auth_header, data, venue_id, configs):
    se_id = data['ecomOrder']["externalid"]
    r = requests.post(url, headers=auth_header, data=json.dumps(data))
    if r.status_code != 201:
        if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
            crm_id = lookup_crm_id(se_id, venue_id, configs, 'o')
            requests.put(url+"/"+crm_id, headers=auth_header, data=json.dumps(data))
        else:
            print(r.status_code)
            print(r.json())
            sys.exit(1)


def post_customer_to_crm(url, auth_header, data, venue_id, configs):
    se_id = data['ecomCustomer']["externalid"]
    crm_id = None
    r = requests.post(url, headers=auth_header, data=json.dumps(data))
    if r.status_code != 201:
        if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
            crm_id = lookup_crm_id(se_id, venue_id, configs, 'c')
    else:
        try:
            crm_id = r.json()["ecomCustomer"]["id"]
            save_crm_id(se_id, venue_id, int(crm_id), configs)
        except Exception:
            pass
    return crm_id


def active_campaign_sync(new_venue=None):
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
        if venue_id == new_venue:
            sql = """mysql %s -h %s -P %s -u %s --password=%s -e \"SELECT * FROM orders_mv WHERE venue_id = %s AND sys_entry_date = '0000-00-00 00:00:00' AND email != '' AND customerid != 'None';\" > %s"""
            sql_cmd = sql % (
                configs['db_name'],
                configs['db_host'],
                configs['db_port'],
                configs['db_user'],
                configs['db_password'],
                str(venue_id),
                file_path
            )
        else:
            sql = """mysql %s -h %s -P %s -u %s --password=%s -e \"SELECT * FROM orders_mv WHERE venue_id = %s AND sys_entry_date > \'%s\' AND email != ''AND customerid != 'None';\" > %s"""
            sql_cmd = sql % (
                configs['db_name'],
                configs['db_host'],
                configs['db_port'],
                configs['db_user'],
                configs['db_password'],
                str(venue_id),
                last_crm_sync.replace('T', ' '),
                file_path
            )
        os.system(sql_cmd)

        #load data from pulled MySQL dump CSV
        with open(file_path, "rU") as file_data:
            reader = csv.reader(file_data, delimiter='\t')
            next(reader, None)
            # group the orderlines into orders
            orders = collections.defaultdict(list)
            for ol in reader:
                orders[ol[3]].append(ol)
            for o in orders:
                ols = orders[o]
                # build order and customer JSON and POST the JSON objects to AC server
                customer_json = build_customer_json(connection, ols)
                if customer_json:
                    crm_id = post_customer_to_crm(customers_url, auth_header, customer_json, venue_id, configs)
                    print(crm_id)
                    if crm_id:
                        order_json = build_order_json(connection, str(crm_id), ols)
                        post_order_to_crm(orders_url, auth_header, order_json, venue_id, configs)
                else:
                    print("BUILD CUSTOMER JSON FAILED!", str(ols))

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Sync Completed - " + configs['last_crm_sync'])


if __name__ == '__main__':
    active_campaign_sync()
