
import pandas as pd
import numpy as np
import json
import pprint
import requests
from bs4 import BeautifulSoup
import datetime
import os

pd.set_option('display.width', 80)
pd.set_option('display.max_columns',6)

states_url = "https://www.50states.com/abbreviations.htm"
webpage = requests.get(url=states_url)

bs = BeautifulSoup(webpage.text, 'html.parser')
#[]
# getting the columns first
tbl_headers = bs.find('table', {'class': 'table table-hover'}).thead.find_all('th')
columns = [h.get_text() for h in tbl_headers]

#[]
# getting the states
tbl_rows = bs.find('table', {'class': 'table table-hover'}).tbody.find_all('tr')

data = []
for row in tbl_rows:
    tdata = row.find_all('td')
    cell_val = [i.get_text() for i in tdata[:2]]
    data.append(cell_val)

abreviaturas = pd.DataFrame(data, columns=columns[:2])

#[]
# now going to get us territories/commonwealths separately since there are 2 tables
data2 = []
tbl_rows2 = bs.find('table', {'class': 'has-fixed-layout table table-hover'}).tbody.find_all('tr')
for row in tbl_rows2:
    tdata = row.find_all('td')
    cell_val = [i.get_text() for i in tdata[:2]]
    data2.append(cell_val)

df_data2 = pd.DataFrame(data2, columns=columns[:2])
abreviaturas = abreviaturas.append(df_data2)
#[]
#


# abreviaturas.head()
# abreviaturas.tail()
# abreviaturas.shape
abreviaturas.columns = abreviaturas.columns.str.title().str.replace(" ", "_")
# abreviaturas.head()


the_day = datetime.datetime.now().strftime('%Y-%m-%d')
directory = f"extract_{the_day}"

if not os.path.isdir(directory):
    os.makedirs(directory)

abreviaturas.to_csv(f"{directory}/state_abbreviations.csv", sep="|", index=False, quotechar="'", doublequote=True)

list_tuples = [tuple(r) for r in abreviaturas.values]



# for row in list_tuples:   
#     try:
#         cursor.execute("SET QUOTED_IDENTIFIER OFF")
#         insert_query=f"INSERT INTO {table} VALUES {row}"
#         cursor.execute(insert_query)
        
#     except Exception as e:
#         print(e)
#         print(insert_query)
#         error = [list(row), insert_query,e,table]
#         rejected.append(error)