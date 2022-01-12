import requests
import urllib3
import json
import pprint
import configparser
from urllib.parse import urlparse, quote
import pandas as pd

api_base = "https://api.census.gov"
population_path = "/data/2019/pep/population"

parser = configparser.ConfigParser()
parser.read("credentials.conf")
census_key = parser.get("census_credentials", "census_key")

# info needs to be sent as query string
get_var = "?get=NAME,POP,DATE_CODE"
county_var = "&for=county:*"
state_var = "&in=state:*"
key_var = "&key=%s" % census_key 

#[]
# function for requesting census data

def census_api_request(api_base, pep_path, variables_req_path="?get=NAME,POP,DATE_CODE", 
county_path="&for=county:*", state_path="&in=state:*", variables_map_path="/variables.json", census_key=None, return_df = False, transform = False):
    """For sending a GET request to the Census API.
    Args:
        api_base (str): The domain host of the API.
        pep_path (str): The path of the API after the domain for the population estimates program.
        variables_path (str): The variables that you want returned in the request
        county_path (str): The county or counties you want returned in the request
        state_path (str): The state/s you want returned in the request
        variables_map_path (str): endpoint to get the variable mappings
        census_key (str): Your census key.

    Returns:
        dict: The JSON response from the request.
        pandas dataframe: with option to transform or not

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url= f"{api_base}{pep_path}{variables_req_path}{county_path}{state_path}&key={census_key}"
    var_url = f"{api_base}{pep_path}{variables_map_path}"
    if not return_df and transform:
        return "not a valid combination"
    
    response = requests.request(method="GET", url=url)
    data = response.json()

    response2 = requests.request(method="GET", url=var_url)
    variables = response2.json()
    '''just sending json dictionary'''
    if not return_df:
        return data
    '''sending transformed pandas df'''
    if return_df and transform:

        df_data = pd.DataFrame(data=data[1:], columns=data[0]) #getting rid of first entry which are the headers
        df_data['DATE_CODE'] = df_data.DATE_CODE.astype(int)
        df_data = df_data.loc[df_data.DATE_CODE > 2, :] # these values have the wrong grain (year)
        df_data.loc[:, 'DATE_CODE'] = df_data.loc[:,'DATE_CODE'].astype(str)
        df_data.loc[:, 'DATE_CODE'] = df_data.loc[:, 'DATE_CODE'].map(variables['variables']['DATE_CODE']['values']['item'])
        df_data.loc[:, 'year'] = df_data['DATE_CODE'].str.extract(r'(\d{4})')
        df_data.loc[:, 'county'] = df_data.loc[:,'NAME'].str.split(', ').str[0]
        df_data.loc[:, 'state'] = df_data.loc[:,'NAME'].str.split(', ').str[1]
        
        data_finished = df_data.iloc[:,1:].reset_index(drop=True)

        data_finished_counties = pd.Series(data = data_finished['county'].unique(), name='counties')
        print("made it")
        return data_finished, data_finished_counties
    '''just sending pandas df'''
    if return_df :
        df_data = pd.DataFrame(data=data[1:], columns=data[0])
        return df_data
#[]
# making the directory to store data
import os
import datetime
from pathlib import Path

# Directory

# Parent Directory path

# Path


try:
    the_day = datetime.datetime.now().strftime('%Y-%m-%d')
    directory = f"local_raw_data/extract_{the_day}"
    path = Path(os.getcwd())
    dir_path = os.path.join(path, directory)
    os.mkdir(dir_path)
except Exception as e:
    print('already exists')


the_data, the_data_counties = census_api_request(api_base=api_base, pep_path=population_path, census_key=census_key, return_df=True, transform=True)


the_data.to_csv(f"{directory}/census_data_01.csv", index=False, sep='|', quotechar="'", doublequote=True)
the_data_counties.to_csv(f"{directory}/census_counties_01.csv", index=False, sep='|', quotechar="'", doublequote=True)





#[]
#sending http request
# url = api_base + population_path + get_var + county_var + state_var + key_var
# response = requests.request(method="GET", url=url)

#[] getting the variables for the years
# variables_path = "/variables.json"
# var_url = api_base + population_path + variables_path
# response2 = requests.request(method="GET", url=var_url)
# variables = response2.json()
# f=open('census_data_var.json','w')
# json.dump(variables,f)
# f.close()

