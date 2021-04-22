import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Description: Load data from S3 to staging tables in Redshift using the queries in
        'copy_table_queries' list
    """
    print('Loading data process')
    for query in copy_table_queries:
        print('Loading data into the following staging tables ' + query + ' ')
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Description: Insert data from staging tables to fact and dimension tables using
        the queries in 'insert_table_queries' list
    """
    print('Inserting data process')
    for query in insert_table_queries:
        print('Inserting data into the following table: ' + query + ' ')
        cur.execute(query)
        conn.commit()


def main():
    """
        Description: Establishes a connection with the Redshift cluster and initiate functions to load data
        from S3 to staging tables then extract data from from staging tables and insert them into fact and
        dimension tables for analytic purposes
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()