import os
import sqlite3
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.path.dirname(os.path.abspath(__file__)) + '/../databases/' + os.getenv('DATABASE_NAME')

def extract_data_from_source(db_table):
    """
    Extract data from a SQLite database table.

    Parameters
    ----------
    db_table : str
        Name of the database table to extract data from.

    Returns
    -------
    data : pandas.DataFrame
        DataFrame containing the extracted data.
    """
    conn = sqlite3.connect(DB_NAME)

    data = pd.read_sql_query("SELECT * FROM " + db_table, conn)

    conn.close()

    return data

