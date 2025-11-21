import os
import sqlite3
import pandas as pd
import tempfile
import shutil
from CWPreprocessing import convert_all_csv_in_folder
import time

def wait_for_file_release(path, timeout=5):
    """Wait until Windows releases the SQLite file lock."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with open(path, "rb"):
                return True
        except PermissionError:
            time.sleep(0.1)
    raise PermissionError(f"File still locked after {timeout} seconds: {path}")


def test_convert_all_csv_in_folder():
    # --- Create a temp directory that DOES NOT auto-delete ---
    temp_dir = tempfile.mkdtemp()

    try:
        # Create CSV files
        csv1_path = os.path.join(temp_dir, "testfile1.csv")
        csv2_path = os.path.join(temp_dir, "second_file.csv")

        df1 = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        df2 = pd.DataFrame({"x": [10, 20, 30], "y": ["cat", "dog", "mouse"]})

        df1.to_csv(csv1_path, index=False)
        df2.to_csv(csv2_path, index=False)

        # Run your converter
        convert_all_csv_in_folder(temp_dir)

        # Expected DB paths
        db1 = os.path.join(temp_dir, "testfile1.db")
        db2 = os.path.join(temp_dir, "second_file.db")

        # --- Open and test DB1 ---
        conn1 = sqlite3.connect(db1)
        df_loaded1 = pd.read_sql_query("SELECT * FROM testfile1", conn1)
        conn1.close()

        pd.testing.assert_frame_equal(df_loaded1, df1)

        # --- Open and test DB2 ---
        conn2 = sqlite3.connect(db2)
        df_loaded2 = pd.read_sql_query("SELECT * FROM second_file", conn2)
        conn2.close()

        pd.testing.assert_frame_equal(df_loaded2, df2)

    finally:
        # Give Windows time to release ALL SQLite locks
        time.sleep(1)

        # Force GC cleanup of any remaining SQLite handles
        import gc
        gc.collect()

        # Now safely delete the directory
        shutil.rmtree(temp_dir, ignore_errors=True)