import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drops all tables as defined in sql_queries.py (drop_table_queries)

    Arguments:
    cur -- The cursor instance used to execute on the database.
    conn -- The database connection instance to commit the transaction.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables as defined in sql_queries.py (create_table_queries)

    Arguments:
    cur -- The cursor instance used to execute on the database.
    conn -- The database connection instance to commit the transaction.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Drops and recreates all tables on the Redshift cluster.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("Tables successfully created")


if __name__ == "__main__":
    main()
