from datetime import datetime
import psycopg2
import configparser


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_ubuntu_db", "database")
user = parser.get("postgres_ubuntu_db", "username")
password = parser.get("postgres_ubuntu_db", "password")
host = parser.get("postgres_ubuntu_db", "host")
port = parser.get("postgres_ubuntu_db", "port")

the_day = datetime.now().strftime('%Y-%m-%d')
directory = f"local_raw_data/extract_{the_day}"

truncate_query = "TRUNCATE public.\"Business\""

sql = f"COPY public.\"Business\" FROM \
\'/home/ubuntucontributor/gourmand-data-pipelines/{directory}/yelp_business_01.csv\' \
WITH CSV DELIMITER \'|\' NULL \'\' QUOTE \'\'\'\';"



try:
    ps_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host= host, port=port)
    ps_cursor = ps_conn.cursor()
    ps_cursor.execute(truncate_query)
    ps_cursor.execute(sql)
    ps_cursor.close()
    ps_conn.commit()
except Exception as e:
    print(e)



ps_conn.close()