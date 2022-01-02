import pymssql
import configparser
import csv
import datetime
import os
import boto3
import re 

import pyodbc

the_day = datetime.datetime.now().strftime('%Y-%m-%d') 
raw_directory = f"raw_data/extract_{the_day}"
curated_directory = f"curated_data/extract_{the_day}"
# raw_directory = f"raw_data/extract_2021-12-26"
if not os.path.isdir(curated_directory):
    os.makedirs(curated_directory)

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
#OLTP db info to get table and file info and run sql file to get all the tables
conn = pymssql.connect(host=host, user=user, password=password, database="GourmandOLTP")

cursor = conn.cursor()

sql_file = open('all-oltp-tables.sql','r')
cursor.execute(sql_file.read())

table_results = cursor.fetchall()

#[] 
# amazon s3 bucket info
s3_client = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)

#[]
# DWH info to upload data

server = 'localhost' 
database = 'GourmandDWH' 
# username = 'myusername' 
# password = 'mypassword' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')
dwh_cursor = cnxn.cursor()
# +';UID='+user+';PWD='+ password

# dwh_conn = pymssql.connect(host=host, database="GourmandDWH", autocommit=False)
# dwh_cursor = dwh_conn.cursor()

for table in table_results:
    
    # truncating stagin tbl before proceeding
    trun_tbl_query = f'truncate table [{table[2]}]'
    dwh_cursor.execute(trun_tbl_query)

    # downloading each file from s3    
    s3_client.download_file(bucket_name,f'{raw_directory}/{table[1]}', f'{curated_directory}/{table[1]}')
    
    # uploading each file to the dwh
    with open(f'{curated_directory}/{table[1]}', newline='') as read_obj:
            csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
            # list_of_tuples = list(map(tuple, csv_reader))
            # list_of_tuples = list_of_tuples[1:] # removing header
            list_of_tuples = [tuple(row) for row in csv_reader]
            list_of_tuples
    rejected = []
     
# come back and attempt to change to execute many instead
    # if table[2] == "Business":
    #     the_len = len(list_of_tuples[0])

    #     the_query = f"INSERT INTO [dbo].[{table[2]}] VALUES({'?,' * (the_len - 1)}?)"
    #     first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
    #     list_of_tuples[0][4]
    #     second_list_of_tuples = [[re.sub('\\\\\'', '\'\'', value) for value in row] for row in first_list_of_tuples]
    #     dwh_cursor.executemany(the_query,second_list_of_tuples)
    #     print(table[2])
    #     print('continue')
    #     continue
    changed_tbl = re.sub('.csv', '', f'{curated_directory}/{table[1]}')
    # Could not allocate space for object 'dbo.BusinessTransactionBridge' in database 'GourmandDWH' because the 'PRIMARY' filegroup is full
    # is possible error
    if table[2] == "BusinessHolding":
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO [dbo].[{table[2]}] VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            dwh_cursor.executemany(the_query,first_list_of_tuples)
            continue          
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            trun_tbl_query = f'truncate table [{table[2]}]'
            dwh_cursor.execute(trun_tbl_query)
            with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
                csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
                csv_w.writerows(['issue on table'])
                print(e)
        
        
    elif table[2] in ('BusinessCategoryBridge','BusinessTransactionBridge'):
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO [dbo].[{table[2]}] VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            dwh_cursor.executemany(the_query,first_list_of_tuples)
            continue          
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            trun_tbl_query = f'truncate table [{table[2]}]'
            dwh_cursor.execute(trun_tbl_query)
            with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
                csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
                csv_w.writerows(['issue on table'])
            print(e)
        

    elif table[2] == "CountyGrowth":
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO [dbo].[{table[2]}] VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            dwh_cursor.executemany(the_query,first_list_of_tuples)
            continue     
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            trun_tbl_query = f'truncate table [{table[2]}]'
            dwh_cursor.execute(trun_tbl_query)
            with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
                csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
                csv_w.writerows(['issue on table'])
            print(e)
        
    for row in list_of_tuples:
        try:
            dwh_cursor.execute("SET QUOTED_IDENTIFIER OFF")
            insert_query=f"INSERT INTO [dbo].[{table[2]}] VALUES {row}"
            # insert_query_test=f"INSERT INTO [dbo].[{table[2]}] VALUES %s"
            mod_query = re.sub('\\\\\'', '\'\'', insert_query)
            mod_query = re.sub(r"(?<=, )''", 'NULL', mod_query)
            dwh_cursor.execute(mod_query)
            cnxn.commit()           
        except Exception as e:
            try:
                #try one more time just in case transaction log full
                dwh_cursor.execute(mod_query)
            except Exception as e:
                print(e)
            print(e)
            print(insert_query)
            print(len(row))
            error = [row, insert_query,e,table]
            rejected.append(error)
    
    changed_tbl = re.sub('.csv', '', f'{curated_directory}/{table[1]}')
  
    
    with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
        csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
        csv_w.writerows(rejected)
    print(f"finished {table}")


        


# cnxn.rollback()

try:
    cnxn.commit()
    conn.commit()
    # dwh_cursor.close()
    # cursor.close()
    
except Exception as e:
    print(e)

cnxn.close()
conn.close()


