import os
import sys
import requests
import json
import collections
import _mysql
import smtplib
import sleep

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
            "category": str(ol["orders_mv.orderproduct_name"]).replace('\x92','\'').replace('\xe2\x80\x99', '\'')
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


def lookup_customer_crm_id(email, url, auth_header, connection):
    # Looks up a customer's CRM Id from AC API. Returns ID as a string or None.
    r = requests.get(url+"?filters[email]=%s" % email,headers=auth_header)
    if r.status_code == 200:
        for c in r.json()["ecomCustomers"]:
            if c["connectionid"] == connection:
                return c["id"]
    return None


def get_se_id(data, obj_type):
    try:
        return data[obj_type]["externalid"]
    except KeyError:
        return None


def push_data_to_api(url, auth_header, data, method):
    if method == "update":
        return requests.put(url, headers=auth_header, data=json.dumps(data))
    elif method == "add":
        return requests.post(url, headers=auth_header, data=json.dumps(data))
    else:
        raise ValueError


def check_api_response(r, obj_type):
    if r.status_code != 201:
        # api complained about a duplicate
        if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
            return ("err", "duplicate",)
        # some other type of error occured
        else:
            return ("err", "other",)
    else:
        # successful add / update to API
        return ("success", None,)


def update_data(url, auth_header, data, configs, connection, obj_type):
    try:
        se_id = get_se_id(data, obj_type)
        # try to add the object first
        r = push_data_to_api(url, auth_header, data, 'add')
        status = check_api_response(r, obj_type)

        if status[0] == 'err':
            if status[1] == 'duplicate':
                if obj_type == 'ecomCustomer':
                    # try to get the CRM id for the customer and return
                    # that out for use elsewhere with orders.
                    return lookup_customer_crm_id(data[obj_type]['email'],
                                url, auth_header, connection)
                else:
                    # We have an order that's a duplicate in the system.
                    # We should try to PUT update its data.
                    r = push_data_to_api(url, auth_header, data, 'update')
                    status = check_api_response(r, obj_type)
                    if status[0] == 'success':
                        return "success"
                    else:
                        # We have an error with the PUT
                        return "err-update"
            else:
                # another kind of error occured :(
                # we know the data was not updated
                return "err-other"
        elif status[0] == 'success':
            if obj_type == 'ecomCustomer':
                try:
                    # return CRM Id for use elsewhere with orders
                    return r.json()[obj_type]["id"]
                except KeyError:
                    # catch any sneaky API / JSON errors that
                    # might manifest here and return None
                    return None
            else:
                return "success"
    except UnicodeDecodeError:
        # skip over orders with Unicode Decode errors
        print("ERROR: UnicodeDecodeError while posting (%s): #%s" % (obj_type, data))
        return 'err-unicode'


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_crm_sync = "00-00-00T00:00:01"
    orders_url = configs['Api-Url'] + 'ecomOrders'
    customers_url = configs['Api-Url'] + 'ecomCustomers'
    venues = [(5, '4'),  (6, '5'), (133, '8'), (7, '6'),
              (21, '7'), (23, '10'), (1, '3'), (297, '2')]

    for venue_id, connection in venues:
        # setup a completion email notifying Kevin and Jason that a Month of Venue pushes has finished
        sender = "kevin@matsongroup.com"
        recipients = ["flygeneticist@gmail.com", "jason@matsongroup.com"]
        header = 'From: %s\n' % sender
        header += 'To: %s\n' % ", ".join(recipients)
        header += 'Subject: Big Orders RESYNC - Venue %s - SeatEngine AWS\n' % venue_id
        msg = header + "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that BIG RESYNC Updates have completed for venue #%s.\n" % venue_id

        print("~~~~~ PROCESSING ORDERS FOR VENUE #%s ~~~~~" % venue_id )
        # download CSV file from MySQL DB
        db = _mysql.connect(user=configs['db_user'],
                            passwd=configs['db_password'],
                            port=configs['db_port'],
                            host=configs['db_host'],
                            db=configs['db_name'])

        sql = """SELECT * FROM orders_mv WHERE venue_id = %s AND(sys_entry_date = '0000-00-00 00:00:00' OR sys_entry_date < NOW()) AND email != '' AND customerid != 'None'""" % (
            str(venue_id)
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

        print("~~ POSTING CUSTOMERS ~~")
        print("TOTAL ORDERS TO PUSH: %s" % len(orders))
        crm_postings = []
        cust_err = {'build':0, 'push':0, 'unicode':0}
        for o in orders:
            ols = orders[o]
            # build order and customer JSON and POST the JSON objects to AC server
            customer_json = build_customer_json(connection, ols)
            if customer_json:
                crm_id = update_data(
                    customers_url, auth_header, customer_json, configs, connection, 'ecomCustomer')
                if crm_id == 'err-unicode':
                    cust_err["unicode"] += 1
                elif crm_id:
                    crm_postings.append([crm_id, ols])
                else:
                    cust_err['push'] += 1
            else:
                print("BUILD CUSTOMER JSON FAILED!", str(ols))
                cust_err['build'] += 1

        print("~~ POSTING ORDERS PAYLOAD ~~")
        print("TOTAL ORDERS TO PUSH: %s" % len(crm_postings))
        order_count = 0
        order_err = {'build':0, 'update':0, 'unicode':0, 'other':0}
        for i in crm_postings:
            try:
                crm_order = build_order_json(connection, str(i[0]), i[1])
                updated = update_data(orders_url, auth_header, crm_order, configs, connection, 'ecomOrder')
                if updated == "success":
                    order_count += 1
                elif updated == 'err-update':
                    order_err["update"] += 1
                elif updated == 'err-unicode':
                    order_err["unicode"] += 1
                elif updated == 'err-other':
                    order_err["other"] += 1
            except:
                print("BUILD ORDER JSON FAILED!", str(i[1][0]["orders_mv.orderNumber"]))
                order_err['build'] += 1
        # add venue details for the month running to the final email msg
        msg += "~~~~~ VENUE #%s ~~~~~\nCustomers pushed\nSUCCESS Qty: %s\nERROR Qty:\nBuild: %s\nPush: %s\nUnicode: %s\n\nOrders pushed\nSUCCESS Qty: %s\nERRORS Qty:\nBuild: %s\nUpdate: %s\nUnicode: %s\nOther: %s\n" % (
            venue_id, len(crm_postings), cust_err['build'], cust_err['push'], cust_err['unicode'], order_count, order_err['build'], order_err['update'], order_err['unicode'], order_err['other'])
        msg += "\nSleeping for 2 hours to avoid SSL issues..."
        # send a completion email notifying Kevin and Jason that daily updates have finished
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login(sender, "tie3Quoo!jaeneix2wah5chahchai%bi")
        server.sendmail(sender,recipients, msg)
        server.quit()
        print("Done! Sleeping for 2 hours to avoid SSL issues..."")
        sleep(7200)

if __name__ == '__main__':
    active_campaign_sync()
