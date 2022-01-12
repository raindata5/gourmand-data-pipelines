import psycopg2
import os
import sys
import requests
import json
import configparser
import boto3
import re 
import csv
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

os.listdir()

the_day = datetime.datetime.now().strftime('%Y-%m-%d') 
raw_directory = f"raw_data/extract_{the_day}"
curated_directory = f"curated_data/extract_{the_day}"

if not os.path.isdir(raw_directory):
    os.makedirs(raw_directory)
if not os.path.isdir(curated_directory):
    os.makedirs(curated_directory)

KEY_PATH = "gourmanddwh-f75384f95e86.json"

CREDS = service_account.Credentials.from_service_account_file(KEY_PATH)

client = bigquery.Client(credentials=CREDS, project=CREDS.project_id)


# change these to the actual production tables later in ...also add in support for event since it is an incremental model
fbh_date_query = "select coalesce(max(CloseDate), '1900-01-01') from `gourmanddwh.g_production.FactBusinessHolding`"

data = client.query(fbh_date_query).result()

data2 = [row for row in data]

fbh_date_res = data2[0][0]



cg_date_query = "select coalesce(max(LastEditedWhen), '1900-01-01') from `gourmanddwh.g_production.FactCountyGrowth`"

data = client.query(cg_date_query).result()

data2 = [row for row in data]

cg_date_res = data2[0][0]





#[]
# postgres extracts

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_ubuntu_db", "database")
user = parser.get("postgres_ubuntu_db", "username")
password = parser.get("postgres_ubuntu_db", "password")
host = parser.get("postgres_ubuntu_db", "host")
port = parser.get("postgres_ubuntu_db", "port")

parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
account_id = parser.get("aws_boto_credentials", "account_id")

ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)

ps_cursor = ps_conn.cursor()

sql_file = open('sql_scripts/all-oltp-tables-postgres.sql','r')
ps_cursor.execute(sql_file.read())
table_results = ps_cursor.fetchall()

cnt_results_tbl = []
for table in table_results:
    select_rows_query = f'select * from {table[0]}'
    select_cnt_query = f'select count(*) from {table[0]}'
    
    # getting the counts along with it's table and appending the tuple to a list which will be later used for data validation
    try:
        # get all the row data and then just serialized similar to the following (for cnt_results_tbl just do that all in one file at end)
        if table[2] == "businessholding":
            BH_QUERY = f'select\
                            businessholdingid,\
                            businessid,\
                            businessrating,\
                            reviewcount,\
                            closedate,\
                            concat(businessid, \'-\',closedate) as IncrementalCompKey from {table[0]}'
            final_BH_QUERY = BH_QUERY + " WHERE CloseDate >= %s"
            inc_select_rows_query = select_cnt_query + " WHERE CloseDate >= %s"
            ps_cursor.execute(inc_select_rows_query, (fbh_date_res,))
            cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)

            ps_cursor.execute(final_BH_QUERY, (fbh_date_res,))
            results = ps_cursor.fetchall()

        elif table[2] == "countygrowth":
            CG_QUERY = f'select \
                            countyid,\
                            estimationyear,\
                            estimatedpopulation,\
                            lasteditedwhen,\
                            concat(countyid, \'-\', estimationyear) as IncrementalCompKey from {table[0]}'
            final_CG_QUERY = CG_QUERY + " WHERE LastEditedWhen > %s"
            inc_select_rows_query = select_cnt_query + " WHERE LastEditedWhen > %s "
            ps_cursor.execute(inc_select_rows_query, (cg_date_res,))
            cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)

            ps_cursor.execute(final_CG_QUERY, (cg_date_res,))
            results = ps_cursor.fetchall()

        elif table[2] == 'businesstransactionbridge':
            ps_cursor.execute(select_cnt_query)
            cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            btb_query = f'select \
                            businessid,\
                            transactionid,\
                            lasteditedwhen,\
                            concat(businessid, \'-\', transactionid) as SnapshotCompKey from {table[0]}'
            ps_cursor.execute(btb_query)
            results = ps_cursor.fetchall()
        elif table[2] == 'businesscategorybridge':
            ps_cursor.execute(select_cnt_query)
            cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            bcb_query = f'select \
                            businessid,\
                            categoryid,\
                            lasteditedwhen,\
                            concat(businessid, \'-\', categoryid) as SnapshotCompKey from {table[0]}'
            ps_cursor.execute(bcb_query)
            results = ps_cursor.fetchall()
        else:
            ps_cursor.execute(select_cnt_query)
            cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            ps_cursor.execute(select_rows_query)
            results = ps_cursor.fetchall()
    except Exception as e:
        print(e)
        print(table)
        print(type(results))
        print(select_rows_query)
    with open(f'{raw_directory}/{table[1]}', 'w', encoding='UTF-8') as fp:
        csv_w = csv.writer(fp, delimiter='|', quotechar="'")
        csv_w.writerows(results)
    ps_conn.commit()
with open(f'{raw_directory}/tbl_cnt_results.csv', 'w', encoding='UTF-8') as fp:
    csv_w = csv.writer(fp, delimiter='|', quotechar="'")
    csv_w.writerows(cnt_results_tbl)



s3_client = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

for table_file in table_results:   
    try:
        s3_client.upload_file(f'{raw_directory}/{table_file[1]}', bucket_name, f'{raw_directory}/{table_file[1]}')
        print(f'{raw_directory}/{table_file[1]} to s3')
    except Exception as e:
        print(e)
        print(f'{raw_directory}/{table_file[1]}')
        
s3_client.upload_file(f'{raw_directory}/tbl_cnt_results.csv', bucket_name, f'{raw_directory}/tbl_cnt_results.csv')


ps_cursor.close()
ps_conn.close()