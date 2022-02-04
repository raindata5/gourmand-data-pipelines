from optparse import Option
import os
from pathlib import Path
import datetime
import configparser
from typing import Optional
import psycopg2
from google.oauth2 import service_account
from google.cloud import bigquery
import boto3
import pandas as pd
import csv
import requests
from typing import Set


class PostgresConnection():
    """ This is an object for managing the connection to the Postgres database"""
    parser = configparser.ConfigParser()
    if "pipeline.conf" in os.listdir():
        parser.read('pipeline.conf')
    else: 
        parser.read("/home/ubuntucontributor/gourmand-data-pipelines/pipeline.conf")

    def __init__(self) -> None:
        self.conn: Optional[psycopg2.connection] = None
        self.cursor: Optional[psycopg2.cursor] = None
        self.dbname: str = self.parser.get("postgres_ubuntu_db", "database")
        self.user: str = self.parser.get("postgres_ubuntu_db", "username")
        self.password: str = self.parser.get("postgres_ubuntu_db", "password")
        self.host: str = self.parser.get("postgres_ubuntu_db", "host")
        self.port: str = self.parser.get("postgres_ubuntu_db", "port")
    
    def start_connection(self) -> None:
        try:
            ps_conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        except (psycopg2.OperationalError, psycopg2.InternalError):
            print('Database unreachable on first attempt going to try a second time')
            ps_conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port)
        self.conn = ps_conn

    def start_cursor(self) -> None:
        if self.conn:
            cursor = self.conn.cursor()
            self.cursor = cursor
        elif not self.conn:
            print('connection not initialized yet')

    def close_cursor(self) -> None:
        if self.cursor:
            self.cursor.close()
        # self.cursor=None

    def commit_connection(self) -> None:
        """We'll commit the transaction only if there is a connection object yet to be closed however 
        if it is closed then we'll start a new connection assuming it closed by itself or was broken"""
        if self.conn and self.conn.closed == 0:
            self.conn.commit()
        elif self.conn and self.conn.closed != 0:
            self.start_connection()
            self.start_cursor()
            print('connection was closed prematurely')
        elif not self.conn:
            print('no valid connection')
    
    def close_connection(self) -> None:
        if self.conn:
            self.conn.close()       
        # self.conn = None

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.conn},"
            f"{self.cursor},"
            f"xxx,"
            f"{self.user},"
            f"xxx,"
            f"xxx,"
            f"{self.port})"
            )

def db_conn():
    parser = configparser.ConfigParser()
    parser.read("/home/ubuntucontributor/gourmand-data-pipelines/pipeline.conf")
    dbname = parser.get("postgres_ubuntu_db", "database")
    user = parser.get("postgres_ubuntu_db", "username")
    password = parser.get("postgres_ubuntu_db", "password")
    host = parser.get("postgres_ubuntu_db", "host")
    port = parser.get("postgres_ubuntu_db", "port")

    ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)
    return ps_conn


class SqlReturn(Set[str]):
    def verificar(self, choice: str) -> None:
        if choice not in self:
            raise ValueError(f'no se permite éste {choice!r} :D')
        print(choice)
        
return_options = SqlReturn({"fetchone()", "fetchall()", None})

def execute_commit_sql_statement2(sql_statement, postgres_connection_obj: PostgresConnection, arguments: tuple = None, to_fetch: str = None, rollback_transaction = False) -> Optional[None]:
    """
    A utility function to quickly run an sql statement
    Params:
        :param sql_statement:
        :param db_cursor: This must be an already initialized db cursor
        :param arguments: tuple a tuple that specifies arguments that you want to be passed into an sql statement containing '?'
        :param to_fetch: string accepts either 'fetch_one()' or 'fetch_all()' depending on the results you expect
        :param rollback_transaction:
    Returns:
    Raises:
    >>> results = execute_commit_sql_statement2(sql_statement = 'select * from table where date = ?', db_conn=conn
    , arguments = ('2022-01-23',), to_fetch='fetch_all()')
    >>> results
    """
    results = None
    # and there are arguments to be passed in and they are of type tuple
    if arguments and type(arguments) == tuple:
        postgres_connection_obj.cursor.execute(sql_statement, arguments)
    elif not arguments:
        postgres_connection_obj.cursor.execute(sql_statement)
    
    return_options.verificar(to_fetch)

    # if only one result is to be fetched
    if to_fetch == 'fetchone()':
        results = postgres_connection_obj.cursor.fetchone()[0]
    # if  multiple results are to be fetched
    elif to_fetch == 'fetchall()':
        results = postgres_connection_obj.cursor.fetchall()
        
    # postgres_connection_obj.close_cursor()
    postgres_connection_obj.commit_connection()
    # db_conn.commit()
    return results

def execute_commit_sql_statement(sql_statement, db_conn):
        ps_cursor = db_conn.cursor()
        ps_cursor.execute(sql_statement)
        ps_cursor.close()
        db_conn.commit()


def create_data_directory(base_dir=None):
    try:
        the_day = datetime.datetime.now().strftime('%Y-%m-%d')
        directory = f"{base_dir}/extract_{the_day}"
        path = Path(os.getcwd())
        dir_path = os.path.join(path, directory)
        os.mkdir(dir_path)
    except Exception as e:
        print(f'{directory} or {base_dir} already exists')
    return directory

# def create_data_directory(base_dir=None):
#     try:
#         the_day = datetime.datetime.now().strftime('%Y-%m-%d')
#         directory = f"{base_dir}/extract_{the_day}"
#         path = Path(os.getcwd())
#         dir_path = os.path.join(path, directory)
#         os.mkdir(dir_path)
#     except Exception as e:
#         print(f'{base_dir} already exists')
#         print(e)
#     return directory

def bq_client(keypath):
    CREDS = service_account.Credentials.from_service_account_file(keypath)
    client = bigquery.Client(credentials=CREDS, project=CREDS.project_id)
    return client


def s3_client_bucket(section= "aws_boto_credentials"):
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")
    access_key = parser.get(section, "access_key")
    secret_key = parser.get(section, "secret_key")
    bucket_name = parser.get(section, "bucket_name")
    account_id = parser.get(section, "account_id")

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return s3_client, bucket_name

def yelp_daily_pull(api_domain, multiple_business_path, counties_list, limit, offset, headers, directory, iterator=2, test_var=None):
    #This creates a sort of schema for our data to deal with fields that are missing data
    bus_data_df_template = pd.DataFrame(columns=['id',
                                'alias',
                                'name',
                                'image_url',
                                'is_closed',
                                'url',
                                'review_count',
                                'rating',
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


                pd_data['time_extracted'] = datetime.datetime.now().isoformat()

                
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

    with open(f'{directory}/business_logs_01.csv','w') as fp:
        csv_w = csv.writer(fp, delimiter = '|', quotechar="'", doublequote=True)        
        csv_w.writerows(msg_store)

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


conn_obj: Optional[DbConn] = None

def start_database_conn_obj():
    global conn_obj
    conn_obj = DbConn()