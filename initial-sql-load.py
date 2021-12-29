import psycopg2
import pymssql
import csv
import glob
import os
import datetime, time
from operator import itemgetter 
import re
import configparser
# conn = pymssql.connect(host="DESKTOP-IJ2FP7P", user="raindata5", password="natalia", database="flask_notifications")
conn = pymssql.connect(host="localhost", database="GourmandOLTP")
# DB_USER="raindata5", DB_PASS="natalia", DB_ADDR=, DB_NAME="flask_notifications")
the_day = datetime.datetime.now().strftime('%Y-%m-%d') 

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_dwh", "database")
user = parser.get("postgres_dwh", "username")
password = parser.get("postgres_dwh", "password")
host = parser.get("postgres_dwh", "host")
port = parser.get("postgres_dwh", "port")

# ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)


# the_day = "2021-12-19"
directory = f"extract_{the_day}"
#[]
# organizing files I want to load in
file = glob.glob(f"{directory}/yelp*01.csv")
file = file + [f'{directory}/census_data_01.csv','state_abbreviations.csv']

# 'transactions',
tables = ['transactions','Business','categories','County','Event','Review','stateabbreviations']

ix = [4, 0, 1, 5, 2, 3, 6]

files = itemgetter(*ix)(file)


#[]
#
def delimiter_check(file):
    with open(file, 'r', encoding='utf-8') as f:
        line=f.readline()
        if '|' in line:
            return "|"
        else :
            return ","


#[]
# for inserting in rows of data
# just remove all single quote since that data is more important
# seems i may just have to remove places where the single quote is found at the end  then do the following method
#finished Event
# finished Review
# if error just send the row and output to different file 
#problem occurs when apostrophe at end or beginning of output
# cursor = ps_conn.cursor()




# with open(f'{directory}/yelp_business_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar='"', doublequote=True)
#     data = [tuple(row) for row in csv_reader]

# with open(f'{directory}/yelp_events_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar='"', doublequote=True)
#     data1 = [tuple(row) for row in csv_reader]

# with open(f'{directory}/yelp_reviews_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar='"', doublequote=True)
#     data2 = [tuple(row) for row in csv_reader]

# with open(f'{directory}/yelp_business_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'" )
#     data = [tuple(row) for row in csv_reader]

# with open(f'{directory}/yelp_events_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
#     data1 = [tuple(row) for row in csv_reader]

# with open(f'{directory}/yelp_reviews_01.csv', encoding='utf-8', ) as read_obj:
#     csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'")
#     data2 = [tuple(row) for row in csv_reader]


# rows = [r for r in data if "'" in r[2]]
# rows_ = [r for r in rows if "\"" in r[2]]
# rows_
# len(rows_)


# rows1 = [r for r in data1 if "'" in r[6]]
# rows1_0 = [r for r in rows1 if "\"" in r[6]]
# len(rows1_0)

# rows11 = [r for r in data1 if "'" in r[16]]
# rows1_0_0 = [r for r in rows11 if "\"" in r[16]]
# len(rows1_0_0)

# rows2 = [r for r in data2 if "'" in r[2]]
# rows2_0 = [r for r in rows2 if "\"" in r[2]]







# et = []
# for row in [data[1]]:    
#     insert_query=f"INSERT INTO Business VALUES {row}"
#     print(insert_query)
#     # print(re.sub('\\\\','()', insert_query))
#     new_query = re.sub('\\\\','\'', insert_query)
#     print(re.sub('\\\\\'','\'\'', insert_query))







cursor = conn.cursor()
for table, one_file in zip(tables,files):
    the_delimiter = delimiter_check(one_file) 
    
    if the_delimiter == "|":
        with open(one_file, encoding='utf-8', newline='') as read_obj:
            csv_reader = csv.reader(read_obj, delimiter='|', quotechar="'", doublequote=True)
            # list_of_tuples = list(map(tuple, csv_reader))
            # list_of_tuples = list_of_tuples[1:] # removing header
            list_of_tuples = [tuple(row) for row in csv_reader]
            list_of_tuples = list_of_tuples[1:]
    else :
        with open(one_file, encoding='utf-8', newline='') as read_obj:
            csv_reader = csv.reader(read_obj, delimiter=",", quotechar="'", doublequote=True ) # only problem is new line charcter being inserted by csv reader but this can easily be extracted out of data
            # list_of_tuples = list(map(tuple, csv_reader))
            # list_of_tuples = list_of_tuples[1:] # removing header
            list_of_tuples = [tuple(row) for row in csv_reader]
            list_of_tuples = list_of_tuples[1:]

    rejected = []
    for row in list_of_tuples:   
        try:
            cursor.execute("SET QUOTED_IDENTIFIER OFF")
            insert_query=f"INSERT INTO {table} VALUES {row}"
            mod_query = re.sub('\\\\\'','\'\'', insert_query)
            cursor.execute(mod_query)
            
        except Exception as e:
            print(e)
            print(insert_query)
            print(len(row))
            error = [list(row), insert_query,e,table]
            rejected.append(error)
    changed_tbl = re.sub('.csv', '', one_file)
    
    with open(f'{changed_tbl}_errors.csv','w', encoding='utf-8') as fp:
        csv_w = csv.writer(fp, delimiter = '|', quotechar="'") # , doublequote=True       
        csv_w.writerows(rejected)
    print(f"finished {table}")




conn.rollback()

# conn.commit()

# <----------->
#[]
# place these along with the other code I wrote for the other tables in the orig. yelp pull

# import pandas as pd
# bus = pd.read_csv("yelp_business_01.csv")

# bus_right  = bus.drop(['categories'],axis=1)

# bus.select_dtypes(include='object').columns.tolist()
# for row in bus.select_dtypes(include='object').columns.tolist():
#     bus[f'{row}'] = bus[f'{row}'].str.replace(r'^(\')', '',regex=True)
#     bus[f'{row}'] = bus[f'{row}'].str.replace(r'(\')$', '',regex=True)

# bus.to_csv("yelp_business_01.csv", sep="|", index=False)

# bus.loc[bus.name.str.contains(r'\'$'), 'name']
# bus.loc[bus.name.str.contains(r'^[(\')]'), '']

# bus.loc[bus['name'].str.contains(r'^[(\')]'),'name'].str.replace(r'^(\')', '',regex=True)
# bus.loc[bus['name'].str.contains("Dunkin"),'name'].str.replace(r'(\')$', '',regex=True)

# <----------->
#[]
# try reading in the columns
# taking the len of the different fields and splitting them up as such
# read in , transform and insert into tables
# for tbl in tables
# import pandas as pd
# eve_errors = pd.read_csv("yelp_events_01_errors.csv", sep="|", colum)


# bus.select_dtypes(include='object').columns.tolist()
# for row in bus.select_dtypes(include='object').columns.tolist():
#     bus[f'{row}'] = bus[f'{row}'].str.replace(r'^(\')', '',regex=True)
#     bus[f'{row}'] = bus[f'{row}'].str.replace(r'(\')$', '',regex=True)

# bus.to_csv("yelp_business_01.csv", sep="|", index=False)




conn.close()
print("donezo ^_^")