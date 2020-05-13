import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Create database function create connection to database, checks and drop if DB 
    already exits if not creates the DB, connects to DB and returns the connection
    
    Returns:
    * cur the cursor variable
    * conn the connection to database
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Reads the DROP table query from sql_queries.py and execute it to drop Tables if it already Exists
    
    Parameter:
    * cur the cursor variable
    * conn the connection to database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Reads the CREATE table queries from sql_queries.py and execute it to create Table in DB
    
    Parameter:
    * cur the cursor variable
    * conn the connection to database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establish connection to DB, drop tables if already exists if not create tables
    Close the connection to database at the end
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()