import configparser
import psycopg2
from postgres_bq_data_validation import db_conn

ps_conn = db_conn()

cursor = ps_conn.cursor()

sql_file = open('insert-postgres-bh.sql','r')
cursor.execute(sql_file.read())
record = cursor.fetchone()
res_1 = record[0]
print(res_1)
exit(0)