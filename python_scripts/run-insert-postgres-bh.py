import configparser
import psycopg2
from python_scripts.utils import execute_commit_sql_statement, create_data_directory, db_conn


if __name__ == "__main__":

    try:
        ps_conn = db_conn()
        sql_file = open('sql_scripts/insert-postgres-bh.sql','r')
        execute_commit_sql_statement(sql_statement=sql_file.read(), db_conn=ps_conn)
        ps_conn.close()
        exit(0)
    except Exception as e:
        print(e)
        exit(-1)