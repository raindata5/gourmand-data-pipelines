from datetime import datetime
import psycopg2
import configparser
from pipeline_ppackage.utils import create_data_directory, execute_commit_sql_statement2, db_conn, PostgresConnection




if __name__ == "__main__":
    directory = create_data_directory(base_dir='local_raw_data')
    # truncates the table where the data is initially ingested into
    truncate_query = "TRUNCATE public.\"Business\""

    # this ingests the data into the db
    sql = f"COPY public.\"Business\" FROM \
    \'/home/ubuntucontributor/gourmand-data-pipelines/{directory}/yelp_business_01.csv\' \
    WITH CSV HEADER DELIMITER \'|\' NULL \'\' QUOTE \'\'\'\';"
    ps = PostgresConnection()
    # establishes connection to the database
    # try:
    #     ps.start_connection()
    # except Exception as e:
    #     print(e)
    
    # try:

    # except Exception as e:
    #     print(e)

    # try:

    # except Exception as e:
    #     print(e)
    #     exit(1)

    try:
        ps.start_connection()
        execute_commit_sql_statement2(sql_statement=truncate_query, postgres_connection_obj=ps)
        print('table truncated')
        execute_commit_sql_statement2(sql_statement=sql, postgres_connection_obj=ps)
        print('data inserted fine into base/stage table')
        sql_file = open('sql_scripts/postgres-removing-duplicates.sql','r')
        execute_commit_sql_statement2(sql_statement=sql_file.read(), postgres_connection_obj=ps)
        print('data inserted fine into main table')
    except Exception as e:
        print(e)
        exit(1)
    ps.close_cursor()
    ps.close_connection()