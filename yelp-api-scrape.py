import requests
from requests import api
import urllib3
import json
import pprint
import configparser
from urllib.parse import urlparse, quote
import time
from datetime import datetime
import csv
import pandas as pd
import os
import sys

if len(sys.argv) != 2 :
    print("add another arg")
    exit(-1)

daily_or_not = sys.argv[1]

if daily_or_not == "daily":
    iterator=1
    print(iterator)
    # exit(0)
elif daily_or_not == "not_daily":
    iterator=2
    print(iterator)
    # exit(0)
else:
    print("incorrect usage")
    exit(-1)

iterator=2

the_day = datetime.now().strftime('%Y-%m-%d')
directory = f"local_raw_data/extract_{the_day}"

if not os.path.isdir(directory):
    os.makedirs(directory)

parser = configparser.ConfigParser()
parser.read('credentials.conf')
secret_key = parser.get("yelp_credentials", "secret_key")

api_domain = 'https://api.yelp.com'
multiple_business_path = '/v3/businesses/search'
individual_business_path = '/v3/businesses/' # the business id is passed in at the end
individual_business_review_path = '/reviews/'

url = '{0}{1}'.format(api_domain, quote(multiple_business_path.encode('utf8'))) # find out purpose

# so offset must be in the query string and it seems all the other parameter could go in the parameters
# also concerning the offset it essentially goes down a list
# so if I offset by one then the first establishment disappears and it would append another new establishment to the end of the
# data so if I limited by 20 then my new establishment would be at 20
# if i got 240 then offset by 50 then what was at data[50] is now at zero and our new establishments began at 189

#[]
#
api_domain = 'https://api.yelp.com'
multiple_business_path = '/v3/businesses/search'
individual_business_path = '/v3/businesses/' # the business id is passed in at the end
individual_business_review_path = '/reviews/'

limit = 'limit=50'
offset = 'offset=50'
secret_key = parser.get("yelp_credentials", "secret_key")
headers = {"Authorization": "Bearer %s" % secret_key}

# getting all the counties present
counties = pd.read_csv(f"{directory}/census_counties_01.csv")
counties_list = counties.iloc[:,-1].tolist()


# getting businesses
bus_data = pd.DataFrame()
msg_store = [] # where the messages will be stored concerning any issues
category_data = pd.DataFrame()
transaction_data = pd.DataFrame()
for county_ix, county in enumerate(counties_list):
    counter = 0
    for ix in range(iterator): # due to the amount of request each day (5000) this is to iterate over each county and not go over limit
        try:
            if ix == 0: # on first iteration no offset parameter will be applied to query string
                f_url = f'{api_domain}{multiple_business_path}?location={county}&{limit}'
            elif ix > 0: # on second/subsequents iterations an offset parameter will be applied to query string
                f_url = f'{api_domain}{multiple_business_path}?location={county}&{limit}&{offset}'
            
            response = requests.request(method='get', url=f_url, headers=headers)

            if response.status_code != 200: # if there is a a reponse code other than 200 then will continue on to next county and log issue
                print(f'location: {county} encountered an issue {response.status_code}')
                msg = [county,ix+1, 'issue']
                msg_store.append(msg)
                break

            data = response.json() # converting response to a python object i.e dictionary
            if data.get('businesses', None) is None: # if there are no businesses contained in object then will continue on to next county and log issue
                msg = [county,ix+1, 'issue: missing key in dict']
                break
            # normalizing the returned json object and defining the structure of the data that I want to be returned back
            pd_data = pd.json_normalize(data.get('businesses', None), max_level=2)
            cat_data = pd.json_normalize(data.get('businesses', None), 'categories' , ['alias','id'], 'business')
            tran_data = pd.json_normalize(data.get('businesses', None), 'transactions' , ['alias','id'], 'business')
            tran_data = tran_data.rename({0:'transaction'}, axis=1)

            pd_data['county'] = county # assigning each batch of data with it's respective counties
            # pd_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S') # setting the time for when the data was extracted with a specific format
            # cat_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # tran_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pd_data['time_extracted'] = datetime.now().isoformat()
            cat_data['time_extracted'] = datetime.now().isoformat()
            tran_data['time_extracted'] = datetime.now().isoformat()
            # time.sleep(1)
            current_business_cnt = len(data.get('businesses', [])) # getting the current count of businesses in the json response body
            # if current_business_cnt < 50: 
            #     counter += 1
            

            # if current_business_cnt < 50 & counter == 2: #make sure its the 2nd pass
            #     break
            #     print(f'this {county} stops at {ix}')
            #     msg = [county,ix, 'broken']
            #     msg_store.append(msg)

            # appending the data to the the df created initially
            category_data = category_data.append(cat_data)
            bus_data = bus_data.append(pd_data)
            transaction_data = transaction_data.append(tran_data)


            print(f'{county_ix} out of {len(counties_list)}, {county} finishes {ix+1}')
            msg = [county,ix+1, 'completed'] # creating message that the county was completed at a specific iteration
            msg_store.append(msg) # appending the message to the message store
            if current_business_cnt < 50 and ix != 1: # if less than 50 businesses were returned on the first iteration then there is no need for a second since 50 is max                
                print(f'this {county} stops at {ix+1}')
                msg = [county,ix+1, 'broken']
                msg_store.append(msg) 
                break

        except Exception as e:
            print(e)
            f'location: {county} encountered an issue {response.status_code} also error{e}'
            msg = [county,ix+1, e]
            msg_store.append(msg)

bus_data_cut = bus_data.drop(['categories','transactions','location.display_address'], axis=1)


with open(f'{directory}/business_logs_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|', quotechar="'", doublequote=True)        
    csv_w.writerows(msg_store)



bus_data_cut_de_dup = bus_data_cut.drop_duplicates(['id'])
category_data_de_dup = category_data.drop_duplicates(['alias', 'title', 'businessalias', 'businessid']) # ['alias', 'title', 'businessalias', 'businessid']
transaction_data_de_dup = transaction_data.drop_duplicates(['transaction','businessalias','businessid']) # ['transaction','businessalias','businessid']
#---------->
# making ideal serialization
# made need to make newline='' then that may remove need to replace '\n' in subsequent transforms
bus_data_cut_de_dup.to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar="'") # , doublequote=True
category_data_de_dup.to_csv(f"{directory}/yelp_cats_01.csv", index=False, sep='|', quotechar="'")
transaction_data_de_dup.to_csv(f"{directory}/yelp_trans_01.csv", index=False, sep='|', quotechar="'")



# bus_data_cut_de_dup.to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar='"')
# category_data_de_dup.to_csv(f"{directory}/yelp_cats_01.csv", index=False, sep='|', quotechar='"')
# transaction_data_de_dup.to_csv(f"{directory}/yelp_trans_01.csv", index=False, sep='|', quotechar='"')

# (bus_data_cut_de_dup['name'].str.contains("'") & bus_data_cut_de_dup['name'].str.contains('"'))
#------------>





#check reviews still functions right
#[]
# getting the first and and last businesses to get their reviews
# using the first and last due to limit of api requests
first_and_last_businesses = bus_data_cut_de_dup.groupby('county', as_index=True).agg({"id":["first"], "alias":["first"]})
bus_stack = first_and_last_businesses.stack()



review_bus_path = multiple_business_path[:-7]
review_path = "/reviews"
#[]
# getting reviews
all_review_data = pd.DataFrame()
review_msg_store = []
for bus_id, bus_alias in zip(bus_stack['id'].values, bus_stack['alias'].values) :
    try:        
        f_url_reviews = f'{api_domain}{review_bus_path}/{bus_id}{review_path}' # build the url
       
        response = requests.request(method='get', url=f_url_reviews, headers=headers) # make the request
        # 301 indicates there was a duplicate and the response returns a new id that can be used to make the request
        if response.status_code == 301: #if a different query must be carried out then finish the process with the new id and go to next value
            print(f'id: {bus_id} encountered an issue {response.status_code}')
            msg = [bus_id,response.status_code, 'reroute']
            review_msg_store.append(msg)

            data = response.json()
            new_id = data.get('new_business_id',None)
            f_url_reviews_new = f'{api_domain}{review_bus_path}/{new_id}{review_path}'
            response = requests.request(method='get', url=f_url_reviews_new, headers=headers)
            new_data = response.json()
            if new_data.get('reviews', None) is None: # if the business contains no reviews then continue on to the next business
                continue

            pd_data = pd.json_normalize(new_data.get('reviews', None), max_level=2)
            # pd_data['time_extracted'] = datetime.now().strftime('%m-%d-%Y %H:%M')
            pd_data['time_extracted'] = datetime.now().isoformat()
            pd_data['business_id'] = bus_id
            pd_data['business_alias'] = bus_alias
            # can also extract the business alias using regex
            # pd_data['business_alias_extracted'] = pd_data['url'].str.extract(r'/([\w\-]+)\?')
            all_review_data = all_review_data.append(pd_data)
            print(f'business: {bus_id} was added despues de un retraso')   
            continue # continue on to the next business as the business was just added
            

        data = response.json()   
        if data.get('reviews', None) is None:
            continue # if the business contains no reviews then continue on to the next business
        pd_data = pd.json_normalize(data.get('reviews', None), max_level=2)
        # pd_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        pd_data['time_extracted'] = datetime.now().isoformat()
        pd_data['business_id'] = bus_id
        pd_data['business_alias'] = bus_alias
        # time.sleep(1)
        all_review_data = all_review_data.append(pd_data)
        msg = [bus_id,pd_data.shape[0], 'success']
        review_msg_store.append(msg)
        print(f'business: {bus_id} was added a la primera')
           

    except Exception as e:
        print(e)
        f'business: {bus_id} encountered an issue {response.status_code} also error{e}'
        msg = [bus_id,response.status_code, e]
        review_msg_store.append(msg)

# urls can also be parsed however this will most likely be done in an analysis after extracting data from db
# from urllib.parse import urlsplit, parse_qs
# all_review_data['url'].reset_index(drop=True).apply(urlsplit)
# parse_qs(all_review_data['url'].reset_index(drop=True).apply(urlsplit)[0].query)

with open(f'{directory}/review_logs_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|', quotechar="'", doublequote=True)       
    csv_w.writerows(review_msg_store)

all_review_data_de_dup = all_review_data.drop_duplicates(['id'])

# all_review_data_de_dup.to_csv(f"{directory}/yelp_reviews_01.csv", sep='|', index=False, quotechar='"')
all_review_data_de_dup.to_csv(f"{directory}/yelp_reviews_01.csv", sep='|', index=False, quotechar="'")



#[]
# getting events

multiple_events_path = '/v3/events'

limit = 'limit=50'
offset = 'offset=50'
# counties = pd.read_csv("census_counties_01.csv")
# counties_list = counties.iloc[:,-1].tolist()

all_events_data = pd.DataFrame()
events_msg_store = []
# for county in counties_list[:1090]:
for county in counties_list:
    counter = 0
    for ix in range(iterator): # due to the amount of request each day (5000) this is to iterate over each county and not go over limit
        try:
            if ix == 0: # on first iteration no offset parameter will be applied to query string
                f_url = f'{api_domain}{multiple_events_path}?location={county}&{limit}'
            elif ix > 0: # on second/subsequents iterations an offset parameter will be applied to query string
                f_url = f'{api_domain}{multiple_events_path}?location={county}&{limit}&{offset}'
            
            response = requests.request(method='get', url=f_url, headers=headers)

            if response.status_code != 200: # if there is a a reponse code other than 200 then will continue on to next county and log issue
                print(f'location: {county} encountered an issue {response.status_code}')
                msg = [county ,ix+1 ,0 , 'issue']
                events_msg_store.append(msg)
                break

            data = response.json() # converting response to a python object i.e dictionary
            if data.get('events', None) is None:
                print(f'no events for {county}')
                break
            # normalizing the returned json object and defining the structure of the data that I want to be returned back
            pd_data = pd.json_normalize(data.get('events'), max_level=2)
            pd_data['county'] = county
            # pd_data['time_extracted'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pd_data['time_extracted'] = datetime.now().isoformat()
            # time.sleep(1)
            current_event_cnt = len(data.get('events', [])) # getting the current count of the events in the json response body

            all_events_data = all_events_data.append(pd_data)
            print(f'this {county} finishes {ix+1} iteration with {current_event_cnt}')
            msg = [county, ix+1, current_event_cnt, 'completed']
            events_msg_store.append(msg)

            if current_event_cnt < 50 and ix != 1: # if less than 50 events were returned on the first iteration then there is no need for a second since 50 is max                  
                print(f'this {county} stops at {ix+1} iteration')
                msg = [county,ix+1, current_event_cnt, 'broken']
                events_msg_store.append(msg) 
                break

        except Exception as e:
            print(e)
            f'location: {county} encountered an issue {response.status_code} also error{e}'
            msg = [county,ix+1,0, e]
            events_msg_store.append(msg)

with open(f'{directory}/events_logs_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|')        
    csv_w.writerows(events_msg_store)


events_data_de_dup = all_events_data.drop_duplicates('id')

events_data_de_dup_cut = events_data_de_dup.drop(['location.display_address'], axis=1)

# events_data_de_dup_cut.to_csv(f"{directory}/yelp_events_01.csv", index=False, sep='|', quotechar='"')
events_data_de_dup_cut.to_csv(f"{directory}/yelp_events_01.csv", index=False, sep='|', quotechar="'")







# -------> this can be leveraged to create sample data for any initial tests
# sample_yelp_businesses = pd.read_csv("yelp_business_01.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_events = pd.read_csv("yelp_events_01.csv",sep='|', quotechar="'", doublequote=True)
# sample_yelp_reviews = pd.read_csv("yelp_reviews_01.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_cats =  pd.read_csv("yelp_cats_01.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_trans =  pd.read_csv("yelp_trans_01.csv", sep='|', quotechar="'", doublequote=True)


# sample_yelp_businesses[:5].to_csv("sample_yelp_businesses.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_events[:5].to_csv("sample_yelp_events.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_reviews[:5].to_csv("sample_yelp_reviews.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_trans[:5].to_csv("sample_yelp_tran.csv", sep='|', quotechar="'", doublequote=True)
# sample_yelp_cats[:5].to_csv("sample_yelp_cat.csv", sep='|', quotechar="'", doublequote=True)



# -----> this code just involves some testing


#[]
#

# from azure.storage.filedatalake import DataLakeServiceClient
# parser = configparser.ConfigParser()
# parser.read('credentials.conf')
# credential = parser.get("ADLS", "access_key")
# acc_url = parser.get("ADLS", "account_url")

# import azure.storage.filedatalake
# service = DataLakeServiceClient(
#     account_url="https://dbdatalakerd.dfs.core.windows.net/", credential=credential)






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
# from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareFileClient

# fileclient = ShareFileClient(account_url=acc_url, credential=credential, share_name="yelp-raw-data",file_path="yelp_data_01.csv")


# service = ShareServiceClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="/LxiztbkvCrU0Q==")

# service.account_name
# b = [a for a in service.list_shares()]
# b

# share = ShareClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="/LxiztbkvCrU0Q==",
# share_name="yelp-raw-data" )
# share.create_share()

#[]
# going to try uploading a file
# hp_1 = 'https://api.yelp.com/v3/businesses/search?term=Starbucks&location=NYC&limit=50'

# headers = {"Authorization": "Bearer %s" % secret_key}
# response = requests.request(method = "GET", url = hp_1, headers = headers)
# data = response.json()
# data['businesses'][1]
# test = pd.json_normalize(data['businesses'], max_level=2)
# test.to_csv("test_file.csv")

# file_client = ShareFileClient(account_url="https://dbdatalakerd.file.core.windows.net/", credential="/LxiztbkvCrU0Q==",
# share_name="yelp-raw-data",file_path="yelp-data-02.csv")

# with open("test_file.csv", "rb") as file:
#     file_client.upload_file(file)
