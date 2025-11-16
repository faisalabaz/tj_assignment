import os
import csv
import psycopg2

script_dir = os.path.dirname(__file__)

def insert_into_postgres(
        pg_conn,
        sql_insert,
        dest_schema,
        dest_table
        ):
    
    try:
        conn = psycopg2.connect(
            host=pg_conn["host"],
            database=pg_conn["database"],
            user=pg_conn["user"],
            password=pg_conn["password"],
            port=pg_conn["port"]
        )
    except Exception as e_conn:
        print(f"An unexpected error on connecting to database: {e_conn}")

    cur = conn.cursor()

    # try:
    #     cur.execute(f"TRUNCATE TABLE {dest_schema}.{dest_table};")
    #     print("Table truncated.")
    # except Exception as e_insert:
    #     print(f"Error truncating: {e_insert}")
    #     return

    try:
        cur.execute(sql_insert)
    except Exception as e:
        print(f"An unexpected when inserting data: {e}")
    
    conn.commit()
    print("All data inserted and committed.")

    cur.close()
    conn.close()