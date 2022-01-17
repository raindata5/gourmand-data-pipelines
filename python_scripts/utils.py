import os
from pathlib import Path
import datetime
import configparser
import psycopg2
from google.oauth2 import service_account
from google.cloud import bigquery
import boto3

def execute_commit_sql_statement(sql_statement, db_conn):
        ps_cursor = db_conn.cursor()
        ps_cursor.execute(sql_statement)
        ps_cursor.close()
        db_conn.commit()


def create_data_directory(base_dir=None):
    try:
        the_day = datetime.datetime.now().strftime('%Y-%m-%d')
        directory = f"{base_dir}/extract_{the_day}"
        path = Path(os.getcwd())
        dir_path = os.path.join(path, directory)
        os.mkdir(dir_path)
    except Exception as e:
        print(f'{directory} already exists')
    return directory

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

def bq_client(keypath):
    CREDS = service_account.Credentials.from_service_account_file(keypath)
    client = bigquery.Client(credentials=CREDS, project=CREDS.project_id)
    return client


def s3_client_bucket(section= "aws_boto_credentials"):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get(section, "access_key")
    secret_key = parser.get(section, "secret_key")
    bucket_name = parser.get(section, "bucket_name")
    account_id = parser.get(section, "account_id")

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3_client, bucket_name