import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Description: This function is responsible to delete all tables in Redshift cluster using
        the queries in 'drop_table_queries' list
    """

    print ('Dropping table process')
    for query in drop_table_queries:
        print("Dropping the following table: " + query + " ")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: This function is responsible to create staging, fact and dimension tables in Redshift cluster
        using the queries in 'create_table_queries' list
    """
    print('Creating table process')
    for query in create_table_queries:
        print('Creating the following table ' + query + ' ')
        cur.execute(query)
        conn.commit()


def main():
    """
        Description: Establishes a connection with the Redshift cluster and initiate functions to setup a database
        for ETL process. The procedures are as follow:
            - Drops all tables
            - Create all required tables
            - Close a connection
        
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Connected to the Redshift Cluster')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()