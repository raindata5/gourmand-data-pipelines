from datetime import datetime
import psycopg2
import configparser
from postgres_bq_data_validation import db_conn
from census_api import create_data_directory

def execute_commit_sql_statement(sql_statement, db_conn):
        ps_cursor = db_conn.cursor()
        ps_cursor.execute(sql_statement)
        ps_cursor.close()
        db_conn.commit()



if __name__ == "__main__":
    directory = create_data_directory(base_dir='local_raw_data')
    # truncates the table where the data is initially ingested into
    truncate_query = "TRUNCATE public.\"Business\""

    # this ingests the data into the db
    sql = f"COPY public.\"Business\" FROM \
    \'/home/ubuntucontributor/gourmand-data-pipelines/{directory}/yelp_business_01.csv\' \
    WITH CSV HEADER DELIMITER \'|\' NULL \'\' QUOTE \'\'\'\';"

    # establishes connection to the database
    try:
        ps_conn = db_conn()
    except Exception as e:
        print(e)
    
    try:
        execute_commit_sql_statement(sql_statement=truncate_query, db_conn=ps_conn)
        print('table truncated')
    except Exception as e:
        print(e)

    try:
        execute_commit_sql_statement(sql_statement=sql, db_conn=ps_conn)
        print('data inserted fine into base/stage table')
    except Exception as e:
        print(e)

    try:
        sql_file = open('sql_scripts/postgres-removing-duplicates.sql','r')
        execute_commit_sql_statement(sql_statement=sql_file.read(), db_conn=ps_conn)
        print('data inserted fine into main table')
    except Exception as e:
        print(e)

    ps_conn.close()