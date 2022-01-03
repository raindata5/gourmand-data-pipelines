import os
import sys
import configparser
import boto3
import csv
import datetime
import requests
import json
import psycopg2

webhook_url = 'https://hooks.slack.com/services/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'

#[]
#

def aws_bucket_client():
    
    # amazon s3 bucket info
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get("aws_boto_credentials", "access_key")
    secret_key = parser.get("aws_boto_credentials", "secret_key")
    bucket_name = parser.get("aws_boto_credentials", "bucket_name")
    account_id = parser.get("aws_boto_credentials", "account_id")
    s3_client = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)
    return s3_client, bucket_name

def db_conn():
    
    parser = configparser.ConfigParser()
    parser.read("/home/ubuntucontributor/gourmand-data-pipelines/pipeline.conf")
    dbname = parser.get("postgres_ubuntu_db", "database")
    user = parser.get("postgres_ubuntu_db", "username")
    password = parser.get("postgres_ubuntu_db", "password")
    host = parser.get("postgres_ubuntu_db", "host")
    port = parser.get("postgres_ubuntu_db", "port")

    ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)
    return ps_conn

def log_end_result(result, tbl, pg_tbl_cnt, bq_tbl_cnt, db_conn):
    load_query = """INSERT INTO validation_run_history(
                    result,
                    tbl,
                    pg_tbl_cnt,
                    bq_tbl_cnt,
                    test_run_at)
                VALUES(%s, %s, %s, %s,
                    current_timestamp);"""
    cursor = db_conn.cursor()
    cursor.execute(load_query, (result, tbl, pg_tbl_cnt, bq_tbl_cnt,))
    db_conn.commit()
    cursor.close()
    # db_conn.close()
    return "logged"

def commit_test(s3_client, bucket_name, db_conn=None):
    the_day = datetime.datetime.now().strftime('%Y-%m-%d') 
    raw_directory = f"raw_data/extract_{the_day}"
    curated_directory = f"curated_data/extract_{the_day}"

    raw_directory = f"raw_data/extract_2021-12-31"
    curated_directory = f"curated_data/extract_2021-12-31"

    s3_client.download_file(bucket_name,f'{raw_directory}/tbl_cnt_results.csv', f'{curated_directory}/tbl_cnt_results.csv')
    s3_client.download_file(bucket_name,f'{raw_directory}/ingest_logs.csv', f'{curated_directory}/ingest_logs.csv')

    with open(f'{curated_directory}/ingest_logs.csv') as read_obj:
        csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
        # list_of_tuples = list(map(tuple, csv_reader))
        # list_of_tuples = list_of_tuples[1:] # removing header
        ingested_data = [[row[-1],row] for row in csv_reader]
        
    sort_ingested_data = sorted(ingested_data)

    with open(f'{curated_directory}/tbl_cnt_results.csv') as read_obj:
        csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
        # list_of_tuples = list(map(tuple, csv_reader))
        # list_of_tuples = list_of_tuples[1:] # removing header
        pg_data = [[row[-1],row] for row in csv_reader]

    sort_pg_data = sorted(pg_data)

    # example structure
    # ['gourmanddwh.public2.user',
    #  ['LoadJob<project=gourmanddwh, location=US, id=ec803cf5-5c42-4cec-bcd1-2415a61c7179>',
    #   'LoadJob<project=gourmanddwh, location=US, id=ec803cf5-5c42-4cec-bcd1-2415a61c7179>',
    #   '5496',
    #   '6',
    #   'gourmanddwh.public2.user']]

    # ['business', ['63317', 'business']]

    failure_tbls = []
    logs = []
    status = 0
    for pg, bq in zip(sort_pg_data, sort_ingested_data):
        pg_tbl_cnt = pg[1][0]
        bq_tbl_cnt = bq[1][2]
        test_result = pg_tbl_cnt == bq_tbl_cnt
        tbl = bq[0]
        logs.append((test_result, tbl, pg_tbl_cnt, bq_tbl_cnt))
        if pg_tbl_cnt != bq_tbl_cnt:
            failure_tbls.append(tbl)
            status += 1
            # will add more functionality so that each test can be recorded in a db table in dwh or perhaps elasticsearch
        elif pg_tbl_cnt == bq_tbl_cnt:
            status += 0 
        else:
            print('something wrong happened here')
            exit(-1)
        #return the accruded data
        return logs, failure_tbls, status


#[]
#

# should be result, tbl, count in pg, count in bq, url
def send_slack_notification(status, tbls, webhook_url,pg_tbl_cnt=None, bq_tbl_cnt=None , msg_override = None):
    try:
        if status == 0:
            message = {"text": f"tested {tbls} and the result was a PASS"}
        else:
            message = {"text": f"tested {tbls} and the result was a FAILURE"}
        if msg_override:
            message = {"text":f'{msg_override}'}
        res = requests.request(method="POST",url=webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
        if res.status_code != 200 :
            print(res)
            print(res.status_code)
            return False
    except Exception as e:
        print("sending of slack message failed")
        print(e)


#[]
#
if __name__ == "__main__":
    if len(sys.argv) == 2 and (sys.argv[1] == '-h' or '--help'):
        print("Usage: python validator.py"
            + "severity_level")
        print("Valid severity values:")
        print("halt")
        print("warn")
        exit(0)

    if len(sys.argv) != 2 :
        print("Usage: python postgres-bq-data-validation.py"
            + "severity_level")
        exit(-1)
    sev = sys.argv[1]

    s3_client, bucket = aws_bucket_client()

    ps_conn = db_conn()

    logs, failure_tbls, status = commit_test(s3_client=s3_client, bucket_name=bucket)

    [log_end_result(row[0], row[1], row[2], row[3], ps_conn) for row in logs]

    ps_conn.close()

    if status > 0:
        #send the slack notification specify the tables that are bad
        if sev == 'halt':
            send_slack_notification(status = status, tbls=failure_tbls, webhook_url=webhook_url )
            exit(-1)
        elif sev == 'warn':
            send_slack_notification(status = status, tbls=failure_tbls, webhook_url=webhook_url )
            exit(0)
    elif status == 0 :
        # send the slack notification that all was well
        send_slack_notification(status = status, tbls=failure_tbls, webhook_url=webhook_url )














#[]
# 
# ingested_data = pd.read_csv(f'{curated_directory}/ingest_logs.csv')
# pg_data = pd.read_csv(f'{curated_directory}/tbl_cnt_results.csv')


# pull down tables counts
# pull down ingest logs
# where the table names match .... check the counts
# include option to halt pipeline on an error or warn
# on failure send slack notification with the table names on pass send nothing