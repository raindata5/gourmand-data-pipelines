# run query to get every table 
# iterate over each table to select all the data, get all the counts (DV)
# then store in S3 bucket

import pyodbc
import pymssql
import configparser
import csv
import datetime
import os
import boto3




start_time = datetime.datetime.now()
print(start_time)


the_day = datetime.datetime.now().strftime('%Y-%m-%d') 
directory = f"raw_data/extract_{the_day}"

if not os.path.isdir(directory):
    os.makedirs(directory)

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")
account_id = parser.get("aws_boto_credentials", "account_id")

parser = configparser.ConfigParser()
parser.read("credentials.conf")
host = parser.get("mssqlLocal", "host")
user = parser.get("mssqlLocal", "user")
password = parser.get("mssqlLocal", "password")

#[]
# connecting to dwh and getting the latest close date for the business holdings
dwh_conn = pymssql.connect(host=host, database="GourmandDWH")
dwh_cursor = dwh_conn.cursor()
# dwh_cursor.execute("select COALESCE(MAX(fbh.CloseDate), '1900-00-00') from _Production.FactBusinessHolding fbh")
dwh_cursor.execute("select COALESCE(NULL, '2021-11-02')")
fbh_date_res = dwh_cursor.fetchone()[0]

# getting the latest lasteditedwhen date for the FactCountyGrowth table
# dwh_cursor.execute("select COALESCE(MAX(fcg.LastEditedWhen), '1900-00-00') from _Production.FactCountyGrowth fcg")
dwh_cursor.execute("select COALESCE(NULL, '2021-11-02')")
cg_date_res = dwh_cursor.fetchone()[0]
#[]
# connecting to OLTP db
server = 'localhost' 
database = 'GourmandOLTP' 
# username = 'myusername' 
# password = 'mypassword' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
oltp_cursor = cnxn.cursor()

# conn = pymssql.connect(host=host, user=user, password=password, database="GourmandOLTP")

# cursor = conn.cursor()

#[]
# running sql file to get all the tables
sql_file = open('all-oltp-tables.sql','r')
oltp_cursor.execute(sql_file.read())

table_results = oltp_cursor.fetchall()
# type(table_results)
# table_results[0:2]

# from datetime import datetime, timedelta, timezone
import struct
def handle_datetimeoffset(dto_value):
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime.datetime(tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6] // 1000,
                    datetime.timezone(datetime.timedelta(hours=tup[7], minutes=tup[8])))
                    
cnxn.add_output_converter(-155, handle_datetimeoffset)
#
cnt_results_tbl = []
for table in table_results:
    select_rows_query = f'select * from {table[0]}'
    select_cnt_query = f'select count(*) from {table[0]}'
    
    # getting the counts along with it's table and appending the tuple to a list which will be later used for data validation
    try:
        oltp_cursor.execute(select_cnt_query)
        cnt_res = oltp_cursor.fetchone()[0]
        ix = (cnt_res,table[2],)
        cnt_results_tbl.append(ix)
        # get all the row data and then just serialized similar to the following (for cnt_results_tbl just do that all in one file at end)
        if table[2] == "BusinessHolding":
            oltp_cursor.execute(select_rows_query + "WHERE CloseDate >= CAST(? as DATE)", (fbh_date_res,))
            results = oltp_cursor.fetchall()
        elif table[2] == "CountyGrowth":
            oltp_cursor.execute(select_rows_query + "WHERE LastEditedWhen > CAST(? as DATE)", (cg_date_res,))
            results = oltp_cursor.fetchall()
        else:
            oltp_cursor.execute(select_rows_query)
            results = oltp_cursor.fetchall()
    except Exception as e:
        print(e)
        print(table)
        print(type(results))
        print(select_rows_query)


    for row_ix, row in enumerate(results):
        row = list(row)
        for var_ix, variable in enumerate(row):
            # if type(variable) == datetime.datetime:
            #     variable = variable.strftime('%Y-%m-%d %H:%M:%S.%f')
                
            #     variable = variable[:-3]
            # if there is timezone info contained then .isoformat() will suffice
            if (type(variable) == datetime.datetime and variable.tzinfo) or type(variable) == datetime.date:
                variable = variable.isoformat()
            # otherwise we can still convert to isoformat but will still have the need to remove padding on the microseconds
            elif (type(variable) == datetime.datetime and variable.tzinfo == None) :
                variable = variable.isoformat(timespec='microseconds')
                variable = variable[:-3]
                
            row[var_ix] = variable
        row = tuple(row)
        results[row_ix] = row

    with open(f'{directory}/{table[1]}', 'w', encoding='UTF-8', newline='') as fp:
        csv_w = csv.writer(fp, delimiter='|', quotechar="'")
        csv_w.writerows(results)
    with open(f'{directory}/tbl_cnt_results.csv', 'w', encoding='UTF-8', newline='') as fp:
        csv_w = csv.writer(fp, delimiter='|', quotechar="'")
        csv_w.writerows(cnt_results_tbl)
# add a logger for these data extractions


# w/o transforms 0:00:42.457220
# 0:00:50.083163
#alternative is rather than iterating through each column 
# do
# [list comprehension returning the ixs of the columns with type datetime]
# now iterate through that list and change each value

#[]
# pushing data to s3
# import glob
s3_client = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)



# files = glob.glob(f"{directory}/*.csv")
# cut_files = [f for f in files if "log" not in f]
# cut_files_sorted = sorted(cut_files)

# ------>
# for table_file in table_results[:5]:
#     with open(f'{directory}/{table_file[1]}', 'w') as fp:
#         csv_w = csv.writer(fp, delimiter='|', quotechar="'")
#         csv_w.writerows(results)
#     print(f'{directory}/{table_file[1]} is the local filename')
#     print(f'{directory[9:]}/{table_file[1]} is the pushed filename')

# ------>


for table_file in table_results:   
    try:
        s3_client.upload_file(f'{directory}/{table_file[1]}', bucket_name, f'{directory}/{table_file[1]}')
        print(f'{directory}/{table_file[1]} to s3')
    except Exception as e:
        print(e)
        print(f'{directory}/{table_file[1]}')
        
s3_client.upload_file(f'{directory}/tbl_cnt_results.csv', bucket_name, f'{directory}/tbl_cnt_results.csv')
# add logging here as well
cnxn.close()


end_time = datetime.datetime.now()
elapsed = end_time - start_time
print(elapsed)



exit(0)