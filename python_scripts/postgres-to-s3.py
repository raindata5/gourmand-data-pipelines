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
from pipeline_ppackage.utils import create_data_directory, bq_client, db_conn, s3_client_bucket, PostgresConnection, execute_commit_sql_statement2

# os.listdir()

raw_directory = create_data_directory(base_dir='raw_data')
curated_directory = create_data_directory(base_dir='curated_data')

KEY_PATH = "gourmanddwh-f75384f95e86.json"
client = bq_client(keypath=KEY_PATH)

# change these to the actual production tables later in ...also add in support for event since it is an incremental model
fbh_date_query = "select coalesce(max(CloseDate), '1900-01-01') from `gourmanddwh.g_production.FactBusinessHolding`"

data = client.query(fbh_date_query).result()

data2 = [row for row in data]

fbh_date_res = data2[0][0]



cg_date_query = "select coalesce(max(LastEditedWhen), '1900-01-01') from `gourmanddwh.g_production.FactCountyGrowth`"

data = client.query(cg_date_query).result()

data2 = [row for row in data]

cg_date_res = data2[0][0]


event_date_query = "select coalesce(max(LastEditedWhen), '1900-01-01') from `gourmanddwh.g_production.DimEvent`"
data = client.query(event_date_query).result()

data2 = [row for row in data]

event_date_res = data2[0][0]



#[]
# postgres extracts
ps_obj = PostgresConnection()
ps_obj.start_connection()

# ps_conn = db_conn()

ps_obj.start_cursor()
# ps_cursor = ps_conn.cursor()

sql_file = open('sql_scripts/all-oltp-tables-postgres.sql','r')
table_results = execute_commit_sql_statement2(sql_statement=sql_file.read(), postgres_connection_obj=ps_obj, to_fetch='fetchall()')

# ps_cursor.execute(sql_file.read())
# table_results = ps_cursor.fetchall()

cnt_results_tbl = []
for table in table_results:
    select_rows_query = f'select * from {table[0]}'
    select_cnt_query = f'select count(*) from {table[0]}'
    ps_obj.commit_connection()
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
            cnt_res = execute_commit_sql_statement2(sql_statement=inc_select_rows_query, postgres_connection_obj=ps_obj, arguments=(fbh_date_res,), to_fetch='fetchone()')
            # ps_cursor.execute(inc_select_rows_query, (fbh_date_res,))
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            # results not defined
            results = execute_commit_sql_statement2(sql_statement=final_BH_QUERY, postgres_connection_obj=ps_obj, arguments=(fbh_date_res,), to_fetch='fetchall()')
            # ps_cursor.execute(final_BH_QUERY, (fbh_date_res,))
            # results = ps_cursor.fetchall()

        elif table[2] == "countygrowth":
            CG_QUERY = f'select \
                            countyid,\
                            estimationyear,\
                            estimatedpopulation,\
                            lasteditedwhen,\
                            concat(countyid, \'-\', estimationyear) as IncrementalCompKey from {table[0]}'
            final_CG_QUERY = CG_QUERY + " WHERE LastEditedWhen > %s"
            inc_select_rows_query = select_cnt_query + " WHERE LastEditedWhen > %s"

            cnt_res = execute_commit_sql_statement2(sql_statement=inc_select_rows_query, postgres_connection_obj=ps_obj, arguments=(cg_date_res,), to_fetch='fetchone()')
            # ps_cursor.execute(inc_select_rows_query, (cg_date_res,))
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)

            results = execute_commit_sql_statement2(sql_statement=final_CG_QUERY, postgres_connection_obj=ps_obj, arguments=(cg_date_res,), to_fetch='fetchall()')
            # ps_cursor.execute(final_CG_QUERY, (cg_date_res,))
            # results = ps_cursor.fetchall()

        elif table[2] == "event":
            final_event_query = select_rows_query + " WHERE LastEditedWhen > %s"
            inc_select_rows_query = select_cnt_query + " WHERE LastEditedWhen > %s"

            cnt_res = execute_commit_sql_statement2(sql_statement=inc_select_rows_query, postgres_connection_obj=ps_obj, arguments=(event_date_res,), to_fetch='fetchone()')
            # ps_cursor.execute(inc_select_rows_query, (event_date_res,))
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)

            results = execute_commit_sql_statement2(sql_statement=final_event_query, postgres_connection_obj=ps_obj, arguments=(event_date_res,), to_fetch='fetchall()')
            # ps_cursor.execute(final_event_query, (event_date_res,))
            # results = ps_cursor.fetchall()

        elif table[2] == 'businesstransactionbridge':

            cnt_res = execute_commit_sql_statement2(sql_statement=select_cnt_query, postgres_connection_obj=ps_obj, to_fetch='fetchone()')

            # ps_cursor.execute(select_cnt_query)
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            btb_query = f'select \
                            businessid,\
                            transactionid,\
                            lasteditedwhen,\
                            concat(businessid, \'-\', transactionid) as SnapshotCompKey,\
                            validto from {table[0]}'

            results = execute_commit_sql_statement2(sql_statement=btb_query, postgres_connection_obj=ps_obj, to_fetch='fetchall()')
            # ps_cursor.execute(btb_query)
            # results = ps_cursor.fetchall()
        elif table[2] == 'businesscategorybridge':
            cnt_res = execute_commit_sql_statement2(sql_statement=select_cnt_query, postgres_connection_obj=ps_obj, to_fetch='fetchone()')
            # ps_cursor.execute(select_cnt_query)
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)
            bcb_query = f'select \
                            businessid,\
                            categoryid,\
                            lasteditedwhen,\
                            concat(businessid, \'-\', categoryid) as SnapshotCompKey,\
                            validto from {table[0]}'

            results = execute_commit_sql_statement2(sql_statement=bcb_query, postgres_connection_obj=ps_obj, to_fetch='fetchall()')
            # ps_cursor.execute(bcb_query)
            # results = ps_cursor.fetchall()
        else:
            cnt_res = execute_commit_sql_statement2(sql_statement=select_cnt_query, postgres_connection_obj=ps_obj, to_fetch='fetchone()')
            # ps_cursor.execute(select_cnt_query)
            # cnt_res = ps_cursor.fetchone()[0]
            ix = (cnt_res,table[2],)
            cnt_results_tbl.append(ix)

            results = execute_commit_sql_statement2(sql_statement=select_rows_query, postgres_connection_obj=ps_obj, to_fetch='fetchall()')
            # ps_cursor.execute(select_rows_query)
            # results = ps_cursor.fetchall()
    except Exception as e:
        print(e)
        print(table)
        print(type(results))
        print(select_rows_query)
    with open(f'{raw_directory}/{table[1]}', 'w', encoding='UTF-8') as fp:
        csv_w = csv.writer(fp, delimiter='|', quotechar="'")
        csv_w.writerows(results)
    # will rather run at the top
    # ps_conn.commit()
with open(f'{raw_directory}/tbl_cnt_results.csv', 'w', encoding='UTF-8') as fp:
    csv_w = csv.writer(fp, delimiter='|', quotechar="'")
    csv_w.writerows(cnt_results_tbl)


if (len(sys.argv) == 2) and (sys.argv[1] == 'test'):
    ps_obj.close_cursor()
    ps_obj.close_connection()
    print('about to exit')
    exit(0)

s3_client, bucket_name = s3_client_bucket()


for table_file in table_results:   
    try:
        s3_client.upload_file(f'{raw_directory}/{table_file[1]}', bucket_name, f'{raw_directory}/{table_file[1]}')
        print(f'{raw_directory}/{table_file[1]} to s3')
    except Exception as e:
        print(e)
        print(f'{raw_directory}/{table_file[1]}')
        
s3_client.upload_file(f'{raw_directory}/tbl_cnt_results.csv', bucket_name, f'{raw_directory}/tbl_cnt_results.csv')


ps_obj.close_cursor()
ps_obj.close_connection()
# ps_cursor.close()
# ps_conn.close()