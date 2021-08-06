import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 into the staging_* tables
    defined in sql_queries.py (copy_table_queries).

    Arguments:
    cur -- The cursor instance used to execute on the database.
    conn -- The database connection instance to commit the transaction.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Loads data from S3 into the analytical tables
    defined in sql_queries.py (insert_table_queries).

    Arguments:
    cur -- The cursor instance used to execute on the database.
    conn -- The database connection instance to commit the transaction.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Runs the entire ETL process.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    db_conn_vals = [
        config.get('CLUSTER', 'HOST'),
        config.get('CLUSTER', 'DB_NAME'),
        config.get('CLUSTER', 'DB_USER'),
        config.get('CLUSTER', 'DB_PASSWORD'),
        config.get('CLUSTER', 'DB_PORT'),
    ]

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *db_conn_vals
        )
    )

    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print("ETL process finished")


if __name__ == "__main__":
    main()
