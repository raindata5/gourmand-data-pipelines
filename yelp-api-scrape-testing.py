import requests
from requests import api
import urllib3
import json
import pprint
import configparser
from urllib.parse import urlparse, quote
# you still can only get up to 1000 businesses using multiple queries and
# combinations of the "limit" and "offset" parameters.

parser = configparser.ConfigParser()
parser.read('credentials.conf')
secret_key = parser.get("yelp_credentials", "secret_key")

api_domain = 'https://api.yelp.com'
multiple_business_path = '/v3/businesses/search'
individual_business_path = '/v3/businesses/' # the business id is passed in at the end
individual_business_review_path = '/reviews/'

url = '{0}{1}'.format(api_domain, quote(multiple_business_path.encode('utf8'))) # find out purpose
secondary_url = api_domain + multiple_business_path
headers = {"Authorization": "Bearer %s" % secret_key}
parameters = {"term": "food","location": "Miami","limit": 50, "offset": 0}
test_response = requests.request(method = "GET", url = url, params = parameters ,headers = headers)
test_response # <Response [200]>
test_data = test_response.json()
type(test_data)

test_data.keys() # ['businesses', 'total', 'region']

test_data['total'] # total is 240

test_data['region']

test_data['businesses'][0]



#[] 
# now we'll test the offset parameter
# offset clause seems to cause problems I tried different values and I keep getting 500

parameters = {"term": "food","location": "Fort Lauderdale", "limit": "30"}
test_response = requests.request(method = "GET", url = url, params = parameters ,headers = headers)
test_response # <Response [200]>
test_data2 = test_response.json()
test_data2['total']
test_data2['region']
test_data2['businesses'][0]
len(test_data2['businesses'])

#[]
# going to try and edit headers and see if I have more luck that way
headers = {"Authorization": "Bearer %s" % secret_key, "Connection": "keep-alive", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0"}
parameters_with_offset = {"term": "food","location": "Fort Lauderdale", "limit": 30, "offset": 240}
test_response = requests.request(method = "GET", url = url, params = parameters_with_offset ,headers = headers)
test_response # still gives 500 code
test_data3 = test_response.json()
test_data3['total']
test_data3['region']
test_data3['businesses'][0]
len(test_data3['businesses'])

#[]
#
full_url = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=20'
quoted_full_url = quote(full_url.encode('utf8'))
test_response4 = requests.request(method = "GET", url = full_url,headers = headers)
test_response4 # <Response [200]>
test_data4 = test_response4.json()


test_data4['total'] # total is 240

test_data4['region']

test_data4['businesses'][0]
len(test_data4['businesses'])

#[]
# let's try offset in the query string this time
full_url_with_offset = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=20&offset=40'
test_response5 = requests.request(method = "GET", url = full_url_with_offset, headers = headers)
test_response5 # <Response [200]>
test_data5 = test_response5.json()

test_data5['total'] # total says 240

test_data5['region']

test_data5['businesses'][0] # managed to get different starbucks
len(test_data5['businesses']) # limit is 20

#[]
# confirming offset works as expected
full_url_with_offset_by_1 = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=20&offset=1'

test_data5['businesses'][1] #this should move to zero after
test_response6 = requests.request(method = "GET", url = full_url_with_offset_by_1, headers = headers)
test_response6 # <Response [200]>
test_response6 = test_response6.json()
test_data6 = test_response6 # made a mistake in previous line

test_data6['total'] # total says 240

test_data6['region']

test_data6['businesses'][1] # managed to get different starbucks
len(test_data6['businesses'])
test_data6['businesses'][2]

first = [d for d in test_data6['businesses'] if d['id'] ==test_data4['businesses'][0]['id']]

test_data4['businesses'][0]['id']

second = [ [index,d] for index, d in enumerate(test_data6['businesses']) if d['id'] ==test_data4['businesses'][1]['id']] #got a match
test_data4['businesses'][1]['id']

test_data6['businesses'][0] #ok so this is now equal to test_data4['businesses'][1]['id']


# so offset must be in the query string and it seems all the other parameter could go in the parameters
# also concerning the offset it essentially goes down a list
# so if I offset by one then the first establishment disappears and it would append another new establishment to the end of the
# data so if I limited by 20 then my new establishment would be at 20
# if i got 240 then offset by 50 then what was at data[50] is now at zero and our new establishments began at 189

#[]
#
hp_1 = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=50'


test_response7 = requests.request(method = "GET", url = hp_1, headers = headers)
test_response7
test_data7 = test_response7.json()

test_data7['total'] # total says 240

test_data7['region']

test_data7['businesses'][49] 
len(test_data7['businesses'])

#[]
#
hp_2 = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=50&offset=49'


test_response8 = requests.request(method = "GET", url = hp_2, headers = headers)
test_response8
test_data8 = test_response8.json()

test_data8['total'] # total says 240

test_data8['region']


len(test_data8['businesses'])

test_data7['businesses'][49] # should be first spot now
test_data8['businesses'][0]  # it's correct

#[]
#

# from azure.storage.filedatalake import DataLakeServiceClient
parser = configparser.ConfigParser()
parser.read('credentials.conf')
credential = parser.get("ADLS", "access_key")
acc_url = parser.get("ADLS", "account_url")

# import azure.storage.filedatalake
# service = DataLakeServiceClient(
#     account_url="https://dbdatalakerd.dfs.core.windows.net/", credential=credential)
import pandas as pd

#[]
#
api_domain = 'https://api.yelp.com'
multiple_business_path = '/v3/businesses/search'
individual_business_path = '/v3/businesses/' # the business id is passed in at the end
individual_business_review_path = '/reviews/'

# 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=50&offset=49'
limit = 'limit=50'
offset = 'offset=50'
secret_key = parser.get("yelp_credentials", "secret_key")
headers = {"Authorization": "Bearer %s" % secret_key}
# getting all the counties present
counties = pd.read_csv("census_counties_01.csv")
counties_list = counties.iloc[:,-1].tolist()

sample_counties = counties_list[:5]

import time
from datetime import datetime
import csv


ok = [ix for ix, c in enumerate(counties_list) if c == "Bullitt County"]
# getting businesses
all_data = pd.DataFrame()
msg_store = []
category_data = pd.DataFrame()
transaction_data = pd.DataFrame()
for county in counties_list:
    counter = 0
    for ix in range(2):
        try:
            if ix == 0:
                f_url = f'{api_domain}{multiple_business_path}?location={county}&{limit}'
            elif ix > 0:
                f_url = f'{api_domain}{multiple_business_path}?location={county}&{limit}&{offset}'
            
            response = requests.request(method='get', url=f_url, headers=headers)

            if response.status_code != 200:
                print(f'location: {county} encountered an issue {response.status_code}')
                msg = [county,ix, 'issue']
                msg_store.append(msg)
                break

            data = response.json()   
            if data.get('businesses', None) is None:
                break

            pd_data = pd.json_normalize(data.get('businesses', None), max_level=2)
            cat_data = pd.json_normalize(data.get('businesses', None), 'categories' , ['alias','id'], 'business')
            tran_data = pd.json_normalize(data.get('businesses', None), 'transactions' , ['alias','id'], 'business')
            tran_data = tran_data.rename({0:'transaction'}, axis=1)

            pd_data['county'] = county
            pd_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')
            cat_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')
            tran_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')

            time.sleep(1)
            current_business_cnt = len(data.get('businesses', [])) 
            # if current_business_cnt < 50: 
            #     counter += 1
            

            # if current_business_cnt < 50 & counter == 2: #make sure its the 2nd pass
            #     break
            #     print(f'this {county} stops at {ix}')
            #     msg = [county,ix, 'broken']
            #     msg_store.append(msg)
            category_data = category_data.append(cat_data)
            all_data = all_data.append(pd_data)
            transaction_data = transaction_data.append(tran_data)

            print(f'this {county} finishes {ix}')
            msg = [county,ix, 'completed']
            msg_store.append(msg)
            if current_business_cnt < 50 and ix != 1:                 
                print(f'this {county} stops at {ix}')
                msg = [county,ix, 'broken']
                msg_store.append(msg) 
                break

        except Exception as e:
            print(e)
            f'location: {county} encountered an issue {response.status_code} also error{e}'
            msg = [county,ix, e]
            msg_store.append(msg)

with open('business_iterations_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|')        
    csv_w.writerows(msg_store)
all_data.to_csv("almost_complete_yelp_data_01.csv")

category_data.to_csv("yelp_categories_for_businesses_01.csv")
transaction_data.to_csv("yelp_transactions_for_businesses_01.csv")

category_data.drop_duplicates(inplace=True)


transaction_data.drop_duplicates(inplace=True)

all_data.duplicated('alias').sum()

all_data.shape
#[]
#
#only got go 43650 ...loop was borken due to lack of dict key 
# do have duplicates as well


#[]
# concatenating the 2 batches
p1 = pd.read_csv("almost_complete_yelp_data.csv")
p2 = pd.read_csv("almost_complete_yelp_data_01.csv")
p3 = p1.append(p2)
p3.shape
# (113843, 27)
p3.drop('Unnamed: 0',axis=1, inplace=True)
p3.duplicated('id').sum()
p3.drop_duplicates(['id'], inplace=True)
p3.shape
# (99188, 26)


#[]
# getting
first_and_last_businesses = p3.groupby('county', as_index=True).agg({"id":["first","last"]})
first_and_last_businesses_values = first_and_last_businesses.unstack()


review_bus_path = multiple_business_path[:-7]
review_path = "/reviews"
#[]
# getting reviews
all_review_data = pd.DataFrame()
review_msg_store = []
for bus_id in first_and_last_businesses_values.values[1642:]:
    try:        
        f_url_reviews = f'{api_domain}{review_bus_path}/{bus_id}{review_path}' # build the url
       
        response = requests.request(method='get', url=f_url_reviews, headers=headers) # make the request

        if response.status_code == 301: #if a different query must be carried out  then finish the process with the new id and go to next value
            print(f'id: {bus_id} encountered an issue {response.status_code}')
            msg = [bus_id,response.status_code, 'reroute']
            review_msg_store.append(msg)

            data = response.json()
            new_id = data.get('new_business_id',None)
            f_url_reviews_new = f'{api_domain}{review_bus_path}/{new_id}{review_path}'
            response = requests.request(method='get', url=f_url_reviews_new, headers=headers)
            data = response.json()
            if data.get('reviews', None) is None:
                continue
            pd_data = pd.json_normalize(data.get('reviews', None), max_level=2)
            pd_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')
            all_review_data = all_review_data.append(pd_data)
            print(f'business: {bus_id} was added despues de un retraso')   
            continue
            

        data = response.json()   
        if data.get('reviews', None) is None:
            continue
        pd_data = pd.json_normalize(data.get('reviews', None), max_level=2)
        pd_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')
        time.sleep(1)
        all_review_data = all_review_data.append(pd_data)
        msg = [bus_id,pd_data.shape[0], 'success']
        review_msg_store.append(msg)
        print(f'business: {bus_id} was added a la primera')
           

    except Exception as e:
        print(e)
        f'business: {bus_id} encountered an issue {response.status_code} also error{e}'
        msg = [bus_id,response.status_code, e]
        review_msg_store.append(msg)

all_review_data['url'] = all_review_data['url'].astype(str)
all_review_data['time_created'] = pd.to_datetime(all_review_data['time_created'])
all_review_data['time_extracted'] = pd.to_datetime(all_review_data['time_extracted'])

with open('review_iterations_02.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|')        
    csv_w.writerows(review_msg_store)

all_review_data.duplicated(['id']).sum() 
all_review_data.drop_duplicates(['id'], inplace=True)

all_review_data.to_csv("yelp_reviews_for_businesses_02.csv", sep='|', quoting=csv.QUOTE_MINIMAL)

#[]
# change some types to allow for quoting







# all_review_data.to_csv("test_01.csv", sep='|')

#[]
# when running the rest of the api requests make sure to extract the business alias from the url
all_review_data.iloc[[10], 1].str.extract(r'/([\w\-]+)\?')
all_review_data.iloc[10, 1]

all_review_data.iloc[:,3:].head()


#[]
# getting events

multiple_events_path = '/v3/events'

limit = 'limit=50'
offset = 'offset=50'
counties = pd.read_csv("census_counties_01.csv")
counties_list = counties.iloc[:,-1].tolist()

all_events_data = pd.DataFrame()
events_msg_store = []
for county in counties_list[10:]:
    counter = 0
    for ix in range(2):
        try:
            if ix == 0:
                f_url = f'{api_domain}{multiple_events_path}?location={county}&{limit}'
            elif ix > 0:
                f_url = f'{api_domain}{multiple_events_path}?location={county}&{limit}&{offset}'
            
            response = requests.request(method='get', url=f_url, headers=headers)

            if response.status_code != 200:
                print(f'location: {county} encountered an issue {response.status_code}')
                msg = [county ,ix ,0 , 'issue']
                events_msg_store.append(msg)
                break

            data = response.json()   
            if data.get('events', None) is None:
                print(f'no events for {county}')
                break

            pd_data = pd.json_normalize(data.get('events'), max_level=2)
            pd_data['county'] = county
            pd_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M')
            
            time.sleep(2)
            current_event_cnt = len(data.get('events', []))

            all_events_data = all_events_data.append(pd_data)
            print(f'this {county} finishes {ix+1} iteration with {current_event_cnt}')
            msg = [county, ix, current_event_cnt, 'completed']
            events_msg_store.append(msg)

            if current_event_cnt < 50 and ix != 1:                 
                print(f'this {county} stops at {ix+1} iteration')
                msg = [county,ix, current_event_cnt, 'broken']
                events_msg_store.append(msg) 
                break

        except Exception as e:
            print(e)
            f'location: {county} encountered an issue {response.status_code} also error{e}'
            msg = [county,ix,0, e]
            events_msg_store.append(msg)

with open('events_iterations_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|')        
    csv_w.writerows(events_msg_store)

all_events_data.duplicated(['id']).sum() 
all_events_data.drop_duplicates('id', inplace=True)
all_events_data.info()
all_events_data.head()
all_events_data.tail()
all_events_data.shape
all_events_data.to_csv("yelp_events_for_businesses_01.csv")


# sample data for initial test
sample_yelp_businesses = pd.read_csv("almost_complete_yelp_data.csv")
sample_yelp_events = pd.read_csv("yelp_events_for_businesses_01.csv")
sample_yelp_reviews = pd.read_csv("yelp_reviews_for_businesses_02.csv", sep='|', quoting=csv.QUOTE_MINIMAL)
counties = pd.read_csv("census_data_01.csv")

sample_yelp_businesses[:5].to_csv("sample_yelp_businesses.csv")
sample_yelp_events[:5].to_csv("sample_yelp_events.csv")
sample_yelp_reviews[:5].to_csv("sample_yelp_reviews.csv")
counties['county'] = counties['NAME'].str.split(',').str[0]
counties['state'] = counties['NAME'].str.split(',').str[1].str.strip()
counties.to_csv("sample_counties.csv")

json_test = pd.read_csv("sample_yelp_businesses.csv")
json_test.loc[:,['alias','categories']]
json_test.loc[:,'categories']
pd.json_normalize(data=)
json_test.loc[:,'categories'].to_json

json_test2

transaction_data[:5].to_csv("sample_yelp_tran.csv")
category_data[:5].to_csv("sample_yelp_cat.csv")



pd.json_normalize(json_test2['businesses'], 'categories' , ['alias','id'], 'businesses')







# import json
# fp = open('census_data_var.json', 'r')
# vars = json.load(fp)

# counties = pd.read_csv("census_data01.csv")
# counties = counties.loc[counties.DATE_CODE > 2, :]
# counties.loc[:, 'DATE_CODE'] = counties.loc[:,'DATE_CODE'].astype(str)
# counties.loc[:, 'DATE_CODE'] = counties.loc[:, 'DATE_CODE'].map(vars['variables']['DATE_CODE']['values']['item'])
# counties.loc[:, 'year'] = counties['DATE_CODE'].str.extract(r'(\d{4})')
# counties = counties.iloc[:,1:].reset_index(drop=True)
# counties.to_csv("census_data_01.csv")


# try blob service client next
# from azure.storage.blob import BlobServiceClient
# from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient

# service = DataLakeServiceClient(account_url="https://dbdatalakerd.dfs.core.windows.net/", credential=credential, 
# file_system_name="raindata", file_path="Rawdata")

# file = DataLakeFileClient(account_url="https://dbdatalakerd.dfs.core.windows.net/", credential=credential, 
# file_system_name="raindata", file_path="yelprawdata")
# sample_yelp_businesses[:5].values
# file.create_file()
# file.append_data(sample_yelp_businesses[:5].values, offset=0, length=len(sample_yelp_businesses[:5].values))
# file.flush_data(len(sample_yelp_businesses[:5]))
#[]
# create an archive folder perhaps
from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareFileClient

fileclient = ShareFileClient(account_url=acc_url, credential=credential, share_name="yelp-raw-data",file_path="yelp_data_01.csv")



import pandas as pd
service = ShareServiceClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="dXWnMJaRSfVMYVw4uOp3EqWE3OvMkhWOWtadrcAXBKwdGEo78dUuvr6MJXwBVor1QbJSWnfZ/LxiztbkvCrU0Q==")

service.account_name
b = [a for a in service.list_shares()]
b

share = ShareClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="dXWnMJaRSfVMYVw4uOp3EqWE3OvMkhWOWtadrcAXBKwdGEo78dUuvr6MJXwBVor1QbJSWnfZ/LxiztbkvCrU0Q==",
share_name="yelp-raw-data" )
share.create_share()

#[]
# going to try uploading a file
hp_1 = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=50'

headers = {"Authorization": "Bearer %s" % secret_key}
response = requests.request(method = "GET", url = hp_1, headers = headers)
data = response.json()
data['businesses'][1]
test = pd.json_normalize(data['businesses'], max_level=2)
test.to_csv("test_file.csv")

file_client = ShareFileClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="dXWnMJaRSfVMYVw4uOp3EqWE3OvMkhWOWtadrcAXBKwdGEo78dUuvr6MJXwBVor1QbJSWnfZ/LxiztbkvCrU0Q==",
share_name="yelp-raw-data",file_path="yelp-data-02.csv")

with open("test_file.csv", "rb") as file:
    file_client.upload_file(file)
