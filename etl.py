import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''This function iterates through the copy_table_queries list and executes each query. It loads the data from the S3 bucket to the staging tables.'''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''This function iterates through the insert_table_queries list and executes each query. It inserts the data from the staging tables to the dimensional tables.'''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''This main function reads through the dwh.cfg file with the configparser object and connects to the Redshift cluster using psycopg2 connect object. Then it runs the load_staging_tables function and insert_tables function.'''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()