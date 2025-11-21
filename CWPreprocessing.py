import sqlite3
import pandas as pd
import os

def convert_all_csv_in_folder(folder_path):
    # List all CSV files in the folder
    csv_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.csv')]

    if not csv_files:
        print("⚠️ No CSV files found in the specified folder.")
        return

    for csv_file in csv_files:
        csv_path = os.path.join(folder_path, csv_file)

        try:
            # Load CSV
            df = pd.read_csv(csv_path)

            # Strip whitespace from column names
            df.columns = df.columns.str.strip()

            # Generate DB and table names based on CSV filename
            base_name = os.path.splitext(csv_file)[0]
            db_name = f"{base_name}.db"
            table_name = base_name

            # Save to SQLite
            with sqlite3.connect(os.path.join(folder_path, db_name)) as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)

            print(f" Converted '{csv_file}' → '{db_name}' (table: '{table_name}')")

        except Exception as e:
            print(f" Error converting {csv_file}: {e}")