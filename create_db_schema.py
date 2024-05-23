import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT #không cần chờ cho tới khi giao dịch hoàn thành, mới thực hiện các thao tác tiếp theo.

def create_database():
    conn=psycopg2.connect("host=localhost port=5433 dbname=postgres user=postgres password=postgres")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname='sales'")
    exits = cur.fetchone()
    if not exits:
        cur.execute("CREATE DATABASE sales WITH ENCODING 'utf8' TEMPLATE template0 ")
    conn.close()
    conn=psycopg2.connect("host=localhost port=5433 dbname=sales user=postgres password=postgres")
    cur = conn.cursor()
    return cur,conn
table_name = ['sales','stocks']

def drop_tables(cur,conn):
    """
    Drop table
    """
    for i in table_name:
        cur.execute(f"DROP TABLE IF EXISTS {i}")
        conn.commit()
def create_tables(cur,conn):
    """
    Create Table
    """
    sales_tables = """
        CREATE TABLE IF NOT EXISTS sales(
        sale_id INT,
        product VARCHAR(40),
        quantity_sold INT,
        each_price FLOAT(2),
        sales FLOAT(2),
        Date DATE,
        day int,
        month int,
        year int)
    """
    stock_table ="""
        CREATE TABLE IF NOT EXISTS stocks(
        product VARCHAR(40),
        total_quantity_sold INT,
        each_price FLOAT(2),
        total_in_stock INT,
        total_sales FLOAT(2)
        )
        """
    tables = [sales_tables,stock_table]
    for i in tables:
        cur.execute(i)
        conn.commit()

def sales_db_schema():
    cur,conn = create_database()
    drop_tables(cur,conn)
    create_tables(cur,conn)
    conn.close()


if __name__ == "__main__":
    sales_db_schema()