from pipeline_ppackage.utils import create_data_directory, census_api_request
import requests
import urllib3
import json
import pprint
import configparser
from urllib.parse import urlparse, quote
import pandas as pd
import os
import datetime
from pathlib import Path

#[]
# function for requesting census data




#[]
# making the directory to store data


# Directory

# Parent Directory path

# Path

if __name__ == "__main__":
    # try:
    #     the_day = datetime.datetime.now().strftime('%Y-%m-%d')
    #     directory = f"local_raw_data/extract_{the_day}"
    #     path = Path(os.getcwd())
    #     dir_path = os.path.join(path, directory)
    #     os.mkdir(dir_path)
    # except Exception as e:
    #     print('already exists')
    
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
    base_dir='local_raw_data'

    directory = create_data_directory(base_dir=base_dir)

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

