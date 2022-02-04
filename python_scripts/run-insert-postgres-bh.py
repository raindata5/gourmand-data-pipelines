import configparser
import psycopg2
from pipeline_ppackage.utils import execute_commit_sql_statement2, create_data_directory, db_conn, PostgresConnection


if __name__ == "__main__":
    ps = PostgresConnection()
    try:
        ps.start_connection()
        sql_file = open('sql_scripts/insert-postgres-bh.sql','r')
        execute_commit_sql_statement2(sql_statement=sql_file.read(), postgres_connection_obj=ps)
        ps.close_cursor()
        ps.close_connection()
        exit(0)
    except Exception as e:
        print(e)
        exit(-1)