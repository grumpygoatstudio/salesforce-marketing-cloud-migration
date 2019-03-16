import os
import sys
import requests
import json
import collections
import _mysql
import smtplib
# import sentry_sdk
from simplejson import JSONDecodeError
from datetime import datetime


reload(sys)
sys.setdefaultencoding('utf-8')
# sentry_sdk.init("https://8e6ee04ac6b14915a677ced85ab320f0@sentry.io/1286483")


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
        "email": data[0]["orders_mv.email"].strip(),
        "orderNumber": str(data[0]["orders_mv.orderNumber"]),
        "orderProducts": [
          {
            # name is a placeholder for the "<show number> - <event name> - <ticket type>"
            "name": str(ol["orders_mv.orderproduct_category"]) + " - " + str(unicode(ol["orders_mv.event_name"], errors='ignore')) + " - " + ol["orders_mv.orderproduct_name"],
            "price": int(ol["orders_mv.orderproduct_price"]),
            "quantity": 1,
            # category is a placeholder for ticket type
            "category": str(ol["orders_mv.orderproduct_name"]).replace('\x92', '\'').replace('\xe2\x80\x99', '\'')
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


def lookup_customer_crm_id(email, url, auth_header, connection, redo=False):
    try:
        # Looks up a customer's CRM Id from AC API. Returns ID as a string or None.
        r = requests.get(url+"?filters[email]=%s" % email,headers=auth_header)
        if r.status_code == 200:
            for c in r.json()["ecomCustomers"]:
                if c["connectionid"] == connection:
                    return c["id"]
        return None
    except Exception:
        if not redo:
            lookup_customer_crm_id(email, url, auth_header, connection, redo=True)
        else:
            return None


def link_order_crm_id(order_data, crm_id, db):
    try:
        query = '''INSERT INTO order_crm_link (order_number, connection, crm_id)
                    VALUES (\'%s\',\'%s\',\'%s\');''' % (
            order_data['orderNumber'], order_data['connectionid'], str(crm_id)
        )
        db.query(query)
    except Exception as err:
        try:
            query = '''UPDATE order_crm_link SET crm_id = \'%s\'
                        WHERE connection = \'%s\'
                        AND order_number = \'%s\';''' % (
                crm_id, order_data['connectionid'], order_data['orderNumber']
            )
            db.query(query)
        except Exception as err2:
            print("SQL INSERT & UPDATE FAILED - EVENT: ", order_data['orderNumber'], err, err2)
    db.close()


def lookup_order_crm_id(order_data, db):
    # Looks up an order's CRM Id from SE DB. Returns ID as a string or None.
    try:
        query = """SELECT crm_id FROM order_crm_link
            WHERE order_number = \'%s\'
            AND connection = \'%s\'
            LIMIT 1""" % (order_data['orderNumber'], order_data['connectionid'])
        db.query(query)
        r = db.store_result()
        order_link = r.fetch_row(how=2)[0]['crm_id']
    except Exception:
        order_link = None
    db.close()
    return order_link


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
        try:
            if r.status_code == 422 and r.json()['errors'][0]['code'] == 'duplicate':
                return ("err", "duplicate",)
            # some other type of error occured
            else:
                return ("err", "other",)
        except JSONDecodeError:
            return ("err", "other",)
    else:
        # successful add / update to API
        return ("success", None,)


def update_data(url, auth_header, data, db, connection, obj_type):
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
                    return lookup_customer_crm_id(data[obj_type]['email'], url, auth_header, connection)
                else:
                    try:
                        # We have an order that's already in the AC system.
                        # We should try to PUT update its data.
                        ac_id = lookup_order_crm_id(data[obj_type], db)
                        if ac_id:
                            r = push_data_to_api(url + '/%s' % ac_id, auth_header, data, 'update')
                            status = check_api_response(r, obj_type)
                            if status[0] == 'success':
                                return "success"
                            else:
                                # We have an error with the PUT
                                return "err-update"
                        else:
                            # no ac id was found can't update it...
                            return "success"
                    except Exception:
                        # still working out a better update method...
                        return "success"
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
                    return "err-other"
            else:
                try:
                    # update SQL linking table for AC and SE order IDs
                    link_order_crm_id(data[obj_type], r.json()[obj_type]["id"], db)
                except Exception:
                    pass
                return "success"
    except UnicodeDecodeError:
        # skip over orders with Unicode Decode errors
        print("ERROR: UnicodeDecodeError while posting (%s): #%s" % (obj_type, data))
        return 'err-unicode'
    except (Exception, JSONDecodeError):
        return "err-other"


def active_campaign_sync():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_crm_sync = configs["last_crm_sync"]
    orders_url = configs['Api-Url'] + 'ecomOrders'
    customers_url = configs['Api-Url'] + 'ecomCustomers'
    venues = [(1, '3'), (5, '4'), (6, '5'), (7, '6'), (21, '7'), (23, '10'),
              (53, '11'), (63, '12'), (131, '9'), (133, '8'), (297, '2'), (335, '16')]

    # setup a completion email notifying Jason that a Month of Venue pushes has finished
    sender = "kevin@matsongroup.com"
    recipients = ["jason@matsongroup.com", "flygeneticist@gmail.com"]
    header = 'From: %s\n' % sender
    header += 'To: %s\n' % ", ".join(recipients)
    header += 'Subject: Completed DAILY Orders Sync - SeatEngine AWS\n'
    msg = header + "\nThis is the AWS Server for Seatengine.\nThis is a friendly notice that the daily CRM sync updates have completed:\n\n"

    for venue_id, connection in venues:
        print("~~~~~ PROCESSING ORDERS FOR VENUE #%s ~~~~~" % venue_id )

        # connect to the AWS database
        db = _mysql.connect(user=configs['db_user'],
                            passwd=configs['db_password'],
                            port=configs['db_port'],
                            host=configs['db_host'],
                            db=configs['db_name'])
        db.autocommit(True)

        sql = """SELECT * FROM orders_mv WHERE venue_id = %s AND sys_entry_date > \'%s\' AND email != ''""" % (
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
                hash_id = str(hash(ol["orders_mv.email"].strip() + ol["orders_mv.orderNumber"]))
                orders[hash_id].append(ol)
            except IndexError:
                more_rows = False

        print("~~ POSTING CUSTOMERS ~~")
        print("TOTAL ORDERS TO PUSH: %s" % len(orders))
        crm_postings = []
        cust_err = {'build': [], 'push': [], 'unicode': [], 'ssl': []}
        for o in orders:
            ols = orders[o]
            # build order and customer JSON and POST the JSON objects to AC server
            customer_json = build_customer_json(connection, ols)
            try:
                if customer_json:
                    crm_id = update_data(
                        customers_url, auth_header, customer_json, db, connection, 'ecomCustomer')
                    if crm_id == 'err-unicode':
                        cust_err["unicode"].append(str(ols[0]["orders_mv.email"]))
                    elif crm_id:
                        crm_postings.append([crm_id, ols])
                    else:
                        cust_err['push'].append(str(ols[0]["orders_mv.email"]))
                else:
                    print("BUILD CUSTOMER JSON FAILED!", str(ols))
                    cust_err['build'].append(str(ols[0]["orders_mv.email"]))
            except requests.exceptions.SSLError:
                cust_err["ssl"].append(str(ols[0]["orders_mv.email"]))

        print("~~ POSTING ORDERS PAYLOAD ~~")
        print("TOTAL ORDERS TO PUSH: %s" % len(crm_postings))
        order_count = 0
        order_err = {'build': [], 'update': [], 'unicode': [], 'other': [], 'ssl': []}
        for i in crm_postings:
            try:
                crm_order = build_order_json(connection, str(i[0]), i[1])
                updated = update_data(orders_url, auth_header, crm_order, db, connection, 'ecomOrder')
                if updated == "success":
                    order_count += 1
                elif updated == 'err-update':
                    order_err["update"].append(
                        str(i[1][0]["orders_mv.externalid"]) + "(SE ID) | " +
                        str(i[1][0]["orders_mv.email"]) + "(EMAIL) | " +
                        str(i[1][0]["orders_mv.orderNumber"]) + "(ORDER ID)"
                    )
                elif updated == 'err-unicode':
                    order_err["unicode"].append(
                        str(i[1][0]["orders_mv.externalid"]) + "(SE ID) | " +
                        str(i[1][0]["orders_mv.email"]) + "(EMAIL) | " +
                        str(i[1][0]["orders_mv.orderNumber"]) + "(ORDER ID)"
                    )
                elif updated == 'err-other':
                    order_err["other"].append(
                        str(i[1][0]["orders_mv.externalid"]) + "(SE ID) | " +
                        str(i[1][0]["orders_mv.email"]) + "(EMAIL) | " +
                        str(i[1][0]["orders_mv.orderNumber"]) + "(ORDER ID)"
                    )
            except requests.exceptions.SSLError:
                order_err["ssl"].append(
                    str(i[1][0]["orders_mv.externalid"]) + "(SE ID) | " +
                    str(i[1][0]["orders_mv.email"]) + "(EMAIL) | " +
                    str(i[1][0]["orders_mv.orderNumber"]) + "(ORDER ID)"
                )
            except Exception:
                print("BUILD ORDER JSON FAILED!", str(i[1][0]["orders_mv.orderNumber"]))
                order_err['build'].append(
                    str(i[1][0]["orders_mv.externalid"]) + "(SE ID) | " +
                    str(i[1][0]["orders_mv.email"]) + "(EMAIL) | " +
                    str(i[1][0]["orders_mv.orderNumber"]) + "(ORDER ID)"
                )
        # add venue details for the month running to the final email msg
        msg += "\n\n----- VENUE #%s -----\nCustomers pushed\nSUCCESS Qty: %s\nERROR Qty:\nBuild: %s\nPush: %s\nUnicode: %s\nSSL: %s\n\nOrders pushed\nSUCCESS Qty: %s\nERRORS Qty:\nBuild: %s\nUpdate: %s\nUnicode: %s\nOther: %s\nSSL: %s\n\n" % (
            venue_id, len(crm_postings), len(cust_err['build']), len(cust_err['push']), len(cust_err['unicode']), len(cust_err['ssl']), order_count, len(order_err['build']), len(order_err['update']), len(order_err['unicode']), len(order_err['other']), len(order_err['ssl']))
        msg += "\n\n----- ERROR DETAILS FOR VENUE #%s -----\nCustomers\nBuild: %s\nPush: %s\nUnicode: %s\nSSL: %s\n\nOrders:\nBuild: %s\nUpdate: %s\nUnicode: %s\nOther: %s\nSSL: %s\n" % (
            venue_id, str(cust_err['build']), str(cust_err['push']), str(cust_err['unicode']), str(cust_err['ssl']), str(order_err['build']), str(order_err['update']), str(order_err['unicode']), str(order_err['other']), str(order_err['ssl']))
        msg += "\n\n----- END OF VENUE REPORT -----"

    # close the db connection
    db.close()

    # send a completion email notifying Jason that daily updates have finished
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
    # try:
    active_campaign_sync()
    # except Exception as e:
    #     sentry_sdk.capture_exception(e)
