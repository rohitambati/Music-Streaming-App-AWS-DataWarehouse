import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """Drop existing tables in Redshift.

    Args:
        cur: psycopg2 cursor object.
        conn: psycopg2 connection object.

    Executes each query in the `drop_table_queries` list to drop
    existing tables in the Redshift database and commits the transaction.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """Create tables in Redshift.

    Args:
        cur: psycopg2 cursor object.
        conn: psycopg2 connection object.

    Executes each query in the `create_table_queries` list to create
    tables in the Redshift database and commits the transaction.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """Connect to Redshift, drop existing tables, and create new tables.

    Reads configuration from `dwh.cfg`, connects to the Redshift cluster,
    drops existing tables, creates new tables, and closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
