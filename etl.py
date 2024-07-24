import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data from S3 into Redshift staging tables.

    Args:
        cur: psycopg2 cursor object.
        conn: psycopg2 connection object.

    Executes each query in the `copy_table_queries` list to load data
    from S3 to Redshift staging tables and commits the transaction.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into final analytics tables in Redshift.

    Args:
        cur: psycopg2 cursor object.
        conn: psycopg2 connection object.

    Executes each query in the `insert_table_queries` list to transform and
    insert data from staging tables to analytics tables and commits the transaction.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to Redshift, load staging tables, and insert data into analytics tables.

    Reads configuration from `dwh.cfg`, connects to the Redshift cluster,
    loads data into staging tables, transforms and inserts data into analytics tables,
    and closes the connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
