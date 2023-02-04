import configparser
import psycopg2
from sql_queries import *


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()