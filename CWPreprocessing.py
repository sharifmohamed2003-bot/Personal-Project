import sqlite3
import pandas as pd
import os
import gc
import time
"""
This script converts all CSV files in a specified folder into individual SQLite database files.
Each CSV file is read into a pandas DataFrame, and then saved into a SQLite database
with the same name as the CSV file (but with a .db extension).
"""

def convert_all_csv_in_folder(folder_path):
    csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the folder.")
        return

    for csv_file in csv_files:
        csv_path = os.path.join(folder_path, csv_file)

        try:
            df = pd.read_csv(csv_path)
            df.columns = df.columns.str.strip()

            base_name = os.path.splitext(csv_file)[0]
            db_name = f"{base_name}.db"
            db_path = os.path.join(folder_path, db_name)
            table_name = base_name

            conn = sqlite3.connect(db_path)

            try:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
            finally:
                conn.close()

            # Remove all references
            del df
            del conn

            # Force garbage cleanup
            gc.collect()

            # Wait for Windows to release the lock
            time.sleep(0.2)

            print(f"Converted '{csv_file}' → '{db_name}' (table: '{table_name}')")

        except Exception as e:
            print(f"Error converting {csv_file}: {e}")

convert_all_csv_in_folder("E:\\Python\\Personal Project\\CWdatafiles")  # Specify the folder path here. It will add all DB files in the same folder.

