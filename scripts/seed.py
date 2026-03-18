import pandas as pd
import time
from scripts.db import get_connection

def run_seed():
    """
    Template for database seeding.
    You can read your CSV files here and bulk insert them into Oracle.
    """
    print("--- Starting Database Seeder ---")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # csv_file = 'data/example_data.csv'
        # print(f"Reading data from {csv_file}...")
        # df = pd.read_csv(csv_file)
        #
        # data_to_insert = [tuple(x) for x in df.values]
        #
        # insert_sql = """
        #    INSERT INTO LISTINGS (name, host_name, price)
        #    VALUES (:1, :2, :3)
        # """
        #
        # start_time = time.time()
        # cursor.executemany(insert_sql, data_to_insert)
        # conn.commit()
        #
        # print(f"Inserted {len(data_to_insert)} rows in {time.time() - start_time:.4f} seconds.")

        print("Seeding logic finished successfully.")

    except Exception as e:
        print(f"[!] Error during seeding: {e}")
    finally:
        if "conn" in locals() and conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    run_seed()
