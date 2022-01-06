import time
from datetime import datetime
import csv
import pandas as pd
import os
import sys
import json
import requests
import configparser

the_day = datetime.now().strftime('%Y-%m-%d')
directory = f"local_raw_data/extract_{the_day}"


if (len(sys.argv) == 2) and (sys.argv[1] == 'test'):
    test_var = 500
else:
    test_var=None

if not os.path.isdir(directory):
    os.makedirs(directory)

iterator=2
parser = configparser.ConfigParser()
parser.read('credentials.conf')
secret_key = parser.get("yelp_credentials", "secret_key")
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


bus_data_df_template = pd.DataFrame(columns=['id',
                            'alias',
                            'name',
                            'image_url',
                            'is_closed',
                            'url',
                            'review_count',
                            'categories',
                            'rating',
                            'transactions',
                            'price',
                            'phone',
                            'display_phone',
                            'distance',
                            'coordinates.latitude',
                            'coordinates.longitude',
                            'location.address1',
                            'location.address2',
                            'location.address3',
                            'location.city',
                            'location.zip_code',
                            'location.country',
                            'location.state',
                            'location.display_address',
                            'county',
                            'time_extracted']) 
msg_store = []
for county_ix, county in enumerate(counties_list[:test_var]):
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
                msg = [county_ix, county, ix+1, 'status code not 200', response.status_code]
                msg_store.append(msg)
                break

            data = response.json() # converting response to a python object i.e dictionary
            if data.get('businesses', None) is None: # if there are no businesses contained in object then will continue on to next county and log issue
                msg = [county_ix, county, ix+1, 'issue: missing key in dict', response.status_code]
                break
            # normalizing the returned json object and defining the structure of the data that I want to be returned back
            pd_data = pd.json_normalize(data.get('businesses', None), max_level=2)

            pd_data['county'] = county # assigning each batch of data with it's respective counties


            pd_data['time_extracted'] = datetime.now().isoformat()

            
            current_business_cnt = len(data.get('businesses', [])) # getting the current count of businesses in the json response body

            # appending the data to the df created initially
            # bus_data = bus_data.append(pd_data)
            bus_data = pd_data
            bus_data_cut = bus_data.drop(['categories','transactions','location.display_address'], axis=1)
            bus_data_cut_de_dup = bus_data_cut.drop_duplicates(['id'])
            if 'yelp_business_01.csv' not in os.listdir(f"{directory}"):
                bus_data_df_template.append(bus_data_cut_de_dup).to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar="'")
            else :
                bus_data_df_template.append(bus_data_cut_de_dup).to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar="'", header=False, mode='a')
        
            print(f'{county}')
            msg = [county_ix,county,ix+1, 'completed', response.status_code] # creating message that the county was completed at a specific iteration
            msg_store.append(msg) # appending the message to the message store
            if current_business_cnt < 50 and ix != 1: # if less than 50 businesses were returned on the first iteration then there is no need for a second since 50 is max                
                print(f'this {county} stops at {ix+1}')
                msg = [county_ix,county,ix+1, 'broken', response.status_code]
                msg_store.append(msg) 
                break

        except Exception as e:
            print(e)
            f'location: {county} encountered an issue {response.status_code} also error{e}'
            msg = [county_ix,county,ix+1, e, '']
            msg_store.append(msg)

# bus_data_cut = bus_data.drop(['categories','transactions','location.display_address'], axis=1)


with open(f'{directory}/business_logs_01.csv','w') as fp:
    csv_w = csv.writer(fp, delimiter = '|', quotechar="'", doublequote=True)        
    csv_w.writerows(msg_store)

# bus_data_cut_de_dup = bus_data_cut.drop_duplicates(['id'])

# bus_data_cut_de_dup.to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar="'")