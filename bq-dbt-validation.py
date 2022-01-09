import re 
import os
from pathlib import Path
from postgres_bq_data_validation import send_slack_notification
import configparser

directory = f"/home/ubuntucontributor/gourmand-dwh"
path = Path(os.getcwd())
new_path = path.parent
dir_path = os.path.join(new_path, directory)

with open(f'{dir_path}/dbt-test-file.txt', 'r') as fh:
    num_list = [line for line in fh]


queries = [row for row in num_list if re.match(r'.*select \* from.*', row)]
cleaned_queries = [re.sub('\\n', '', row) for row in queries]

test_name = [row for row in num_list if re.match(r'.*FAIL .*', row)]

p = re.compile("FAIL (.*) \[FAIL")

cleaned_test_name_objects = [p.search(row) for row in test_name]

cleaned_test_names = [obj.group(1) for obj in cleaned_test_name_objects]

# still deciding on where to best store the queries but for now they will just be serialized in the local file system

# create functionality for sending slack notification
parser = configparser.ConfigParser()
parser.read("credentials.conf")
webhook_url = parser.get("slack", "webhook_url")
if len(cleaned_queries) > 0 and len(cleaned_test_names) > 0 :
    status = 1
    send_slack_notification(status, cleaned_test_names, webhook_url=webhook_url, msg_override= f'dbt test \n {cleaned_test_names}')
    exit(0)
else :
    print('all dbt tests passed')
    exit(0)