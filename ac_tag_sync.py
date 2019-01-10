import os
import sys
import requests
import json
import collections
import _mysql
from simplejson import JSONDecodeError
from datetime import datetime
from optparse import OptionParser
from time import sleep


parser = OptionParser()
parser.add_option("-b", "--backlog", dest="backlog", type="string",
                  help="Run tag updates for backlog items", metavar="backlog")
reload(sys)
sys.setdefaultencoding('utf-8')


def load_config(dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'r') as f:
        return json.load(f)


def write_config(config, dir_path):
    with open(os.path.join(dir_path, 'config.json'), 'w') as f:
        json.dump(config, f)


def get_ac_tags(api_key):
    # uses the old v2 API due to bugs with v3 tag list retrieval
    import ipdb; ipdb.set_trace();
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    try:
        d = {
            "api_key": api_key,
            "api_action": "tag_list",
            "api_output": "json"
        }
        r = requests.post(url, headers=auth_header, data=d)
        try:
            if r.status_code == 200:
                return r.json()
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!")
            return None
    except Exception:
        return None

def create_ac_tag(url, auth_header, title):
    import ipdb; ipdb.set_trace();
    print("Creating a new tag\t", title)
    d = {"tag": {
        "tag": title,
        "tagType": "contact",
        "description": title
    }}
    try:
        r = requests.post(url + 'tags', headers=auth_header, data=d)
        try:
            if r.status_code == 201:
                return r.json()['tags']
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!")
            return None
    except Exception:
        return None


def lookup_crm_id_by_api(api_key, email):
    auth_header = {"Content-Type": "application/x-www-form-urlencoded"}
    url = "https://heliumcomedy.api-us1.com/admin/api.php"
    try:
        d = {
            "api_key": api_key,
            "api_action": "contact_view_email",
            "api_output": "json",
            "email": str(email).replace("\r", "").strip()
        }
        r = requests.post(url, headers=auth_header, data=d)
        try:
            if r.status_code == 200:
                return r.json()["id"]
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!", email)
            return None
    except Exception:
        return None


def add_tag_to_contact(url, auth_header, crm_id, tag_id):
    print("Adding tag(%s) to contact(%s)" % (crm_id, tag_id))
    d = {"contactTag": {
        "contact": crm_id,
        "tag": tag_id
    }}
    try:
        r = requests.post(url, headers=auth_header, data=d)
        try:
            if r.status_code == 200:
                return True
            else:
                return None
        except JSONDecodeError:
            print("JSON DECODE ERROR!", str(d["contact"]))
            return None
    except Exception:
        return None


def update_contact_tags(url, auth_header, email, contact_tags, ac_tags):
    crm_id = lookup_crm_id_by_api(auth_header["Api-Token"], email)
    if crm_id:
        for tag in contact_tags:
            add_tag_to_contact(url + 'contactTags', auth_header, crm_id, ac_tags[tag])
    else:
        print("ERROR: Missing CRM ID. Create contact via API failed.", email)
        return None
    return crm_id


def active_campaign_sync(backlog=False):
    dir_path = os.path.dirname(os.path.abspath(__file__))
    configs = load_config(dir_path)
    auth_header = {e: configs[e] for e in configs if "Api-Token" in e}
    auth_header["Content-Type"] = "application/json"
    last_ac_mobile_sync = configs["last_ac_mobile_sync"]
    url = configs['Api-Url']

    db = _mysql.connect(user=configs['db_user'],
                        passwd=configs['db_password'],
                        port=configs['db_port'],
                        host=configs['db_host'],
                        db=configs['db_name'])

    print("~~~~~ PROCESSING CONTACTS ~~~~~")
    contacts = collections.defaultdict(set)
    tags = set()
    contact_count = 0
    chunk_size = 5000
    chunk_num = 0
    more_rows = True

    if backlog:
        db.query(
            """SELECT
                    cm.email_address as email,
                    tt.tag as tag
                FROM contacts_mobile_mv cm
                LEFT JOIN ac_mobile_contacts ac ON (ac.email = cm.email_address)
                JOIN mobile_uploads mu ON (mu.mobile_number = cm.mobile_number)
                JOIN tag_temp tt ON (tt.campaign = mu.campaign)
                WHERE cm.email_address IS NOT NULL
                AND tt.tag NOT LIKE 'IGNORE%'
                ORDER BY cm.email_address;
            """
        )
        r = db.store_result()

        while more_rows:
            try:
                record = r.fetch_row(how=2)[0]
                contacts[record['cm.email']].add(record['tt.tag'])
                tags.add(record['tt.tag'])
            except IndexError:
                more_rows = False
        db.close()
    else:
        db.query(
            """SELECT
                cm.email_address as email,
                mu.campaign as campaign,
                tt.tag as tag
            FROM contacts_mobile_mv cm
            LEFT JOIN ac_mobile_contacts ac ON (ac.email = cm.email_address)
            JOIN mobile_uploads mu ON (mu.mobile_number = cm.mobile_number)
            LEFT JOIN tag_temp tt ON (tt.campaign = mu.campaign)
            WHERE cm.last_message_date BETWEEN \'%s\' AND NOW()
            AND cm.email_address IS NOT NULL
            ORDER BY cm.email_address, cm.last_message_date;
            """ % (last_ac_mobile_sync.replace('T', ' '))
        )
        r = db.store_result()

        while more_rows:
            try:
                record = r.fetch_row(how=2)[0]
                contacts[record['cm.email']].add(record['mu.campaign'])
                if record['tt.tag'] not in [None, "", "None"]:
                    tags.add(record['tt.tag'])
                else:
                    tags.add(record['mu.campaign'])
            except IndexError:
                more_rows = False
        db.close()

    # get full list of AC tags
    ac_tags = set([t['name'] for t in get_ac_tags(auth_header["Api-Token"])])
    import ipdb; ipdb.set_trace();
    # update AC with all new tags
    for tag in list(tags.difference(ac_tags)):
        create_ac_tag(url, auth_header, tag)
    # create dict lookup from newly updated list of AC tags
    ac_tags = {t['name']: t['id'] for t in get_ac_tags(auth_header["Api-Token"])}
    import ipdb; ipdb.set_trace();

    # assign contact's tags to them in AC
    for contact_info in contacts:
        try:
            update_contact_tags(url, auth_header, contact_info, contacts[contact_info], ac_tags)
            contact_count += 1
        except requests.exceptions.SSLError:
            contact_err["ssl"].append(str(contact_info['cm.email_address']))

    print("AC Tags Sync Completed - " + datetime.now().strftime("%Y-%m-%dT%H:%M:%S") + '\n')


if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.backlog:
        active_campaign_sync(backlog=True)
    else:
        active_campaign_sync()
