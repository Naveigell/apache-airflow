import os
import sqlite3

RAW_FOLDER     = os.path.dirname(os.path.abspath(__file__)) + '/../data/' + os.getenv('RAW_FOLDER')
STAGING_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/../data/' + os.getenv('STAGING_FOLDER')

def save_data_into_csv(df, file_name, folder = None):

    """
    Save data into a CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to save.
    file_name : str
        Name of the CSV file to save the data into.

    Returns
    -------
    None
    """

    folder = folder if folder is not None else STAGING_FOLDER

    if not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Created directory: {folder}")

    df.to_csv(os.path.join(folder, file_name), index=False)

    print(f"Data saved to {file_name} in {folder}")


def save_dataframe_into_sqlite(df, db_name, table_name):
    """
    Save data into a SQLite database.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to save.
    table_name : str
        Name of the table to save the data into.

    Returns
    -------
    None
    """
    connection = sqlite3.connect(db_name)

    df.to_sql(table_name, connection, if_exists='replace', index=False)

    connection.close()

    print(f"Data saved to {table_name} in {db_name}")

