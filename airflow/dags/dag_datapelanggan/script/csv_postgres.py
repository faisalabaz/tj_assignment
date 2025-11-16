import os
import csv
import psycopg2

script_dir = os.path.dirname(__file__)

def csv_to_postgres(
        script_dir,
        item,
        pg_conn,
        data_mapping,
        dest_schema,
        dest_table
        ):
    with open(f'{script_dir}/{item}.csv', "r") as f:
        reader = csv.reader(f)
        keys = next(reader)
        data_list = []

        for row in reader:
            row_dict = dict(zip(keys, row))
            data_list.append(row_dict)
    
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

    try:
        cur.execute(f"TRUNCATE TABLE {dest_schema}.{dest_table};")
        print("Table truncated.")
    except Exception as e_insert:
        print(f"Error truncating: {e_insert}")
        return
    
    for row in data_list:
        pg_columns = []
        pg_values = []

        for key, value in row.items():
            if key in data_mapping:
                pg_columns.append(data_mapping[key])
                pg_values.append(value)
        
        sql = f"""
            INSERT INTO {dest_schema}.{dest_table} ({", ".join(pg_columns)})
            VALUES ({", ".join(["%s"] * len(pg_values))});
        """

        try:
            cur.execute(sql, pg_values)
        except Exception as e:
            print(f"An unexpected when inserting data: {e}")
    
    conn.commit()
    print("All data inserted and committed.")

    cur.close()
    conn.close()