import os
import sys
import requests
import json
import collections
import _mysql
import smtplib

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
            "email": str(data[0]["orders_mv.email"])
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


def lookup_crm_id(email, url, auth_header, connection):
    r = requests.get(url+"?filters[email]=%s" % email,headers=auth_header)
    if r.status_code == 200:
        for c in r.json()["ecomCustomers"]:
            if c["connectionid"] == connection:
                return c["id"]
    return None


def post_object_to_crm(url, auth_header, data, venue_id, configs, connection, obj_type='ecomCustomer'):
    try:
        se_id = data[obj_type]["externalid"]
        r = requests.post(url, headers=auth_header, data=json.dumps(data))
        if r.status_code != 201:
            if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
                if obj_type == 'ecomCustomer':
                    return lookup_crm_id(data[obj_type]['email'], url, auth_header, connection)
                else:
                    return True
            else:
                print("ERROR: %s Not Created: %s" % (obj_type, se_id))
                return None
        else:
            if obj_type == 'ecomCustomer':
                return r.json()[obj_type]["id"]
            else:
                return True
    except UnicodeDecodeError:
        # skip over orders with Unicode Decode errors
        print("ERROR: UnicodeDecodeError while posting (%s): #%s" % (obj_type, data))
        return None


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_crm_sync = configs["last_crm_sync"]
    orders_url = configs['Api-Url'] + 'ecomOrders'
    customers_url = configs['Api-Url'] + 'ecomCustomers'
    venues = [(1, '3'), (5, '4'), (6, '5'), (7, '6'), (21, '7'), (23, '10'),
              (53, '11'), (63, '12'), (131, '9'), (133, '8'), (297, '2')]
    new_venues = []

    # setup a completion email notifying Kevin and Jason that a Month of Venue pushes has finished
    header = 'From: %s\n' % sender
    header += 'To: %s\n' % ", ".join(recipients)
    header += 'Subject: Completed DAILY Orders Sync - SeatEngine AWS\n'
    msg = header + "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the daily CRM sync updates have completed: SUCCESS"

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
        print("TOTAL ORDERS TO PUSH: %s" % len(orders))
        for o in orders:
            ols = orders[o]
            # build order and customer JSON and POST the JSON objects to AC server
            customer_json = build_customer_json(connection, ols)
            if customer_json:
                crm_id = post_object_to_crm(customers_url, auth_header, customer_json, venue_id, configs, connection)
                crm_postings.append([crm_id, ols])
            else:
                print("BUILD CUSTOMER JSON FAILED!", str(ols))

        print("~~ POSTING ORDERS PAYLOAD ~~")
        crm_postings = [i for i in crm_postings if i[0]]
        print("TOTAL ORDERS TO PUSH - LESS BAD CUST DATA: %s" % len(crm_postings))
        order_count = 0
        order_err = 0
        for i in crm_postings:
            try:
                crm_order = build_order_json(connection, str(i[0]), i[1])
                if post_object_to_crm(orders_url, auth_header, crm_order, venue_id, configs, connection, 'ecomOrder'):
                    order_count += 1
                else:
                    order_err += 1
            except:
                print("BUILD ORDER JSON FAILED!", str(i[1][0]["orders_mv.orderNumber"]))

        # add venue details for the month running to the final email msg
        msg += "~~~~~ VENUE #%s ~~~~~\nCustomer push (SUCCESS qty: %s, ERROR qty: %s)\nOrder push (SUCCESS qty: %s, ERROR qty: %s)\n" % (
                venue_id, len(crm_postings), len(orders)-len(crm_postings), order_count, order_err)


    # send a completion email notifying Kevin and Jason that daily updates have finished
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
    server.sendmail(sender,recipients, msg)
    server.quit()

    # WRITE NEW DATETIME FOR LAST CRM SYNC
    configs['last_crm_sync'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
    write_config(configs, dir_path)
    print("CRM Sync Completed - " + configs['last_crm_sync'])


if __name__ == '__main__':
    active_campaign_sync()
