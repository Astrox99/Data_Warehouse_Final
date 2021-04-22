import configparser
import psycopg2
from sql_queries import del_queries, insert_table_queries


 
# def del_all_rows(cur, conn):
#     for query in del_queries:
#         print('Running ' +  query + ' ')
#         cur.execute(query)
#         conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Running ' +  query + ' ')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    insert_tables(cur, conn)
    # del_queries(cur,conn)

    conn.close()


if __name__ == "__main__":
    main()

# import configparser
# import psycopg2
# from sql_queries import drop_star_schema, create_star_schema


# def drop_tables(cur, conn):
#     print ('Dropping tables')
#     for query in drop_star_schema:
#         cur.execute(query)
#         conn.commit()


# def create_tables(cur, conn):
#     for query in create_star_schema:
#         print('Running ' + query + ' ')
#         cur.execute(query)
#         conn.commit()


# def main():
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')

#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     cur = conn.cursor()
    
#     print('Connected to the Redshift Cluster')

#     drop_star_schema(cur, conn)
#     create_star_schema(cur, conn)

#     conn.close()


# if __name__ == "__main__":
#     main()