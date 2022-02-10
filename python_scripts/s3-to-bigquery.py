from google.oauth2 import service_account
from google.cloud import bigquery
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

#[]
#
the_day = datetime.datetime.now().strftime('%Y-%m-%d') 
raw_directory = f"raw_data/extract_{the_day}"
curated_directory = f"curated_data/extract_{the_day}"
if not os.path.isdir(curated_directory):
    os.makedirs(curated_directory)


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

#[] 
# amazon s3 bucket info
s3_client = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

# for loading into BigQuery

KEY_PATH = "gourmanddwh-f75384f95e86.json"
CREDS = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=CREDS, project=CREDS.project_id)


# ds = client.list_tables('Snapshots')
# ds2 = [table.table_id for table in ds]

# table_logs = []
# for table in ds2:
#     sql_query = f'select * from gourmanddwh.Snapshots.{table}'
#     new_table_id = f'gourmanddwh.Snapshots2.{table}'
#     # *** -------> review
#     # allow jagged rows should fix the issue
#     job_config = bigquery.QueryJobConfig(destination=new_table_id)
#     query_job = client.query(sql_query, job_config=job_config)
#     table_logs.append([query_job.result(), table[2], new_table_id])

#     print("Query results loaded to the following table: {}".format(new_table_id))

table_logs = []
for table in table_results:
    # downloading each file from s3    
    s3_client.download_file(bucket_name,f'{raw_directory}/{table[1]}', f'{curated_directory}/{table[1]}')
    print(f'{raw_directory}/{table[1]} brought down from s3')
    # params for big query
    file_path = f'{curated_directory}/{table[1]}'
    table_id = f'gourmanddwh.public2.{table[2]}'
    # config that describes our csv format and also makes sure to truncate the table before adding data
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, quote_character = "'", field_delimiter = "|"
        ,write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE, allow_quoted_newlines=True
    )
    # opening the file encoded as bytes and loading into big query tables
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    # job details
    job_result = job.result()
    # table details
    table = client.get_table(table_id)

    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    #storing the data describing each ingestion
    full_job = (job, job_result,table.num_rows,len(table.schema), table_id)
    table_logs.append(full_job)
    #considering reuploading as curated data into s3 but as there are no transformations at the moment perhaps better to not do so
    # s3_client.upload_file(f'{curated_directory}/{table[1]}', bucket_name, f'{curated_directory}/{table[1]}')

with open(f'{curated_directory}/ingest_logs.csv','w', encoding='utf-8') as fp:
        csv_w = csv.writer(fp, delimiter = '|', quotechar="'")   
        csv_w.writerows(table_logs)

s3_client.upload_file(f'{curated_directory}/ingest_logs.csv', bucket_name, f'{curated_directory}/ingest_logs.csv')
