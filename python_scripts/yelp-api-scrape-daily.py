from pipeline_ppackage.utils import create_data_directory, yelp_daily_pull
from datetime import datetime
import os
import sys
import configparser
import pandas as pd





if __name__ == "__main__":
    # this allows a test amount of data to be pulled from the api
    if (len(sys.argv) == 3) and (sys.argv[1] == 'test'):
        test_var = int(sys.argv[2])

    elif (len(sys.argv) == 2) and (sys.argv[1] == 'test') and (sys.argv[-1] == sys.argv[1]):
        print('example usage: python python_scripts/yelp-api-scrape-daily.py test 10')
        exit(0)
    # if test is not specified then we will pull the maximum amount of records
    elif (len(sys.argv) == 1): 
        test_var=None
    else:
        print('example usage: python python_scripts/yelp-api-scrape-daily.py test 10')
        exit(0)

        
    directory = create_data_directory(base_dir='local_raw_data')
    # since the csv file is being appended to (if it already is present) any residual file 
    # needs to be removed first
    if os.path.exists(f"{directory}/yelp_business_01.csv"):
        os.remove(f"{directory}/yelp_business_01.csv")
    else:
        print("The file does not exist")

    # setting up some parameters for the pull function
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
    headers = {"Authorization": "Bearer %s" % secret_key}

    # getting all the counties present
    counties = pd.read_csv(f"{directory}/census_counties_01.csv")
    counties_list = counties.iloc[:,-1].tolist()

    yelp_daily_pull(api_domain=api_domain, multiple_business_path=multiple_business_path, counties_list=counties_list, 
    limit=limit, offset=offset, headers=headers, directory=directory, iterator=iterator, test_var=test_var)



# bus_data_cut = bus_data.drop(['categories','transactions','location.display_address'], axis=1)
# bus_data_cut_de_dup = bus_data_cut.drop_duplicates(['id'])
# bus_data_cut_de_dup.to_csv(f"{directory}/yelp_business_01.csv", index=False, sep='|', quotechar="'")