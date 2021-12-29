import pyodbc
import pymssql
import configparser
import csv
import datetime
import os
import boto3
import psycopg2
from datetime import datetime
import os
import re

raw_directory = f"raw_data/extract_2021-12-26"

parser = configparser.ConfigParser()
parser.read("credentials.conf")
host = parser.get("mssqlLocal", "host")
user = parser.get("mssqlLocal", "user")
password = parser.get("mssqlLocal", "password")
server = 'localhost' 
database = 'GourmandOLTP' 

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
oltp_cursor = cnxn.cursor()

sql_file = open('all-oltp-tables.sql','r')
oltp_cursor.execute(sql_file.read())

table_results = oltp_cursor.fetchall()


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_ubuntu_db", "database")
user = parser.get("postgres_ubuntu_db", "username")
password = parser.get("postgres_ubuntu_db", "password")
host = parser.get("postgres_ubuntu_db", "host")
port = parser.get("postgres_ubuntu_db", "port")


ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)

ps_cursor = ps_conn.cursor()

# conn_str = (
    
#     f"DATABASE={dbname};"
#     f"UID={user};"
#     f"PWD={password};"
#     f"SERVER={host};"
#     f"PORT={port};"
#     )
# conn = pyodbc.connect(conn_str)


for table in table_results[1:]:
    
    # truncating stagin tbl before proceeding
    # trun_tbl_query = f'truncate public.\"{table[2]}\"'
    # ps_cursor.execute(trun_tbl_query)

    with open(f'{raw_directory}/{table[1]}', newline='') as read_obj:
            csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
            # list_of_tuples = list(map(tuple, csv_reader))
            # list_of_tuples = list_of_tuples[1:] # removing header
            list_of_tuples = [tuple(row) for row in csv_reader]
            list_of_tuples

    # Could not allocate space for object 'dbo.BusinessTransactionBridge' in database 'GourmandDWH' because the 'PRIMARY' filegroup is full
    # is possible error
    if table[2] == "BusinessHolding":
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO public.\"{table[2]}\" VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            ps_cursor.executemany(the_query,first_list_of_tuples)
            continue          
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            # trun_tbl_query = f'truncate public.\"{table[2]}\"'
            # ps_cursor.execute(trun_tbl_query)
            # with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
            #     csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
            #     csv_w.writerows(['issue on table'])
                print(e)
        
        
    elif table[2] in ('BusinessCategoryBridge','BusinessTransactionBridge'):
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO public.\"{table[2]}\" VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            ps_cursor.executemany(the_query,first_list_of_tuples)
            continue          
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            # trun_tbl_query = f'truncate public.\"{table[2]}\"'
            # ps_cursor.execute(trun_tbl_query)
            # with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
            #     csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
            #     csv_w.writerows(['issue on table'])
            print(e)
        

    elif table[2] == "CountyGrowth":
        print(table[2])
        try:
            the_len = len(list_of_tuples[0])
            the_query = f"INSERT INTO public.\"{table[2]}\" VALUES({'?,' * (the_len - 1)}?)"
            first_list_of_tuples = [[re.sub('', 'NULL', value) if value == '' else value for value in row ] for row in list_of_tuples]
            ps_cursor.executemany(the_query,first_list_of_tuples)
            continue     
        except Exception as e:
            # will just default to normal method after tuncating table for those that weren't able to be added 
            # due to a full transaction log and if not due to this the errors will be logged
            # trun_tbl_query = f'truncate public.\"{table[2]}\"'
            # ps_cursor.execute(trun_tbl_query)
            # with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
            #     csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
            #     csv_w.writerows(['issue on table'])
            print(e)
        rejected= []
    for row in list_of_tuples:
        try:
            # ps_cursor.execute("SET QUOTED_IDENTIFIER OFF")
            insert_query=f"INSERT INTO public.\"{table[2]}\" VALUES {row}"
            # insert_query_test=f"INSERT INTO [dbo].[{table[2]}] VALUES %s"
            # mod_query = re.sub('(?=[^,]) \'[^,]', '\'\'', insert_query)
            mod_query = re.sub('\\\\\'', '\'\'', mod_query)
            mod_query = re.sub(r"(?<=, )''", 'NULL', mod_query)
            mod_query = re.sub(r'(?<=, )"', '\'', mod_query)
            mod_query = re.sub(r'"(?=, \')', '\'', mod_query)
            # mod_query = re.sub(r'"(?=, \')', '\'', mod_query)
            ps_cursor.execute(mod_query)
            cnxn.commit()           
        except Exception as e:
            print(e)
            print(mod_query)
            print(len(row))
            error = [row, insert_query,e,table]
            rejected.append(error)
            try:
                #try one more time just in case transaction log full
                ps_cursor.execute(mod_query)
            except Exception as e:
                print(e)
            
            
    
  
    
    # with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
    #     csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
    #     csv_w.writerows(rejected)
    print(f"finished {table}")


        


ps_conn.rollback()

try:
    cnxn.commit()
    ps_conn.commit()
    # dwh_cursor.close()
    # cursor.close()
    
except Exception as e:
    print(e)

cnxn.close()
ps_conn.close()

# \copy public."BusinessHolding" FROM E'/mnt/c/Users/Ron/git-repos/yelp-data/raw_data/extract_2021-12-26/\'[_Production]_[BusinessHolding].csv\'' WITH (FORMAT CSV);
