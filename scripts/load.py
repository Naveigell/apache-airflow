# scripts/load.py
import os

import extract as extract
import transform as transform

RAW_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/../DATA/' + os.getenv('RAW_FOLDER')
OUTPUT_FILE = os.path.join(RAW_FOLDER, 'library_transformed_data.csv')

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
