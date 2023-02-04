import configparser
import psycopg2
from sql_queries import *


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    aws_db_paremeters = ("""
    host = {}
    dbname = {}
    user = {}
    password = {}
    port = {}
    """).format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)

    conn = psycopg2.connect(aws_db_paremeters)
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()