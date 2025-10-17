# scripts/load.py
import os

import pandas as pd

from scripts.utils import save_dataframe_into_sqlite
from scripts.transform import transform_data

RAW_FOLDER       = os.path.dirname(os.path.abspath(__file__)) + '/../DATA/' + os.getenv('RAW_FOLDER')
OUTPUT_FILE      = os.path.join(RAW_FOLDER, 'library_transformed_data.csv')
FOLDER           = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_FOLDER   = FOLDER + os.getenv('STAGING_FOLDER')
STAGING_DATABASE = FOLDER + '/staging/' + os.getenv('STAGING_DATABASE')

DB_NAME = os.path.dirname(os.path.abspath(__file__)) + '/../databases/' + os.getenv('DATABASE_NAME')


def load_data(df, output_file):
    """
    Load transformed data into a CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the transformed data.
    output_file : str
        Path to the CSV file to save the data into.

    Returns
    -------
    None

    Notes
    -----
    This function creates the directory path if it does not exist.
    """
    if not os.path.exists(RAW_FOLDER):
        os.makedirs(RAW_FOLDER)

    df.to_csv(output_file, index=False)


def load_into_staging():
    """
    Load transformed data into the staging database.

    This function reads the CSV files from the staging folder, transforms the data using the transform_data function,
    and saves the result into the staging database.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    # Read the CSV files from the staging folder
    df_books = pd.read_csv(STAGING_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(STAGING_FOLDER + '/patrons.csv')
    df_loans = pd.read_csv(STAGING_FOLDER + '/loans.csv')

    transformed = transform_data(df_books, df_patrons, df_loans)

    save_dataframe_into_sqlite(transformed, STAGING_DATABASE, 'staging_data')
