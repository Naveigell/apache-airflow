import os
import sys

import pandas as pd
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# append path
sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from scripts.extract   import extract_data_from_source
from scripts.utils     import save_data_into_csv, save_dataframe_into_sqlite
from scripts.transform import transform_data

def notify_failure(context):
    """
    Write log.txt when task failed.

    Parameters
    ----------
    context : str
        Task context where failure occurred.

    Returns
    -------
    None
    """
    with open('log.txt', 'w') as f:
        f.write('Task failed : ' + str(context['reason']))


def extract_data_into(table, file_name):
    """
    Extract data from a SQLite database table into a pandas DataFrame.

    Parameters
    ----------
    table : str
        Name of the database table to extract data from.

    Returns
    -------
    data : pandas.DataFrame
        DataFrame containing the extracted data.
    """
    extracted = extract_data_from_source(table)

    save_data_into_csv(extracted, file_name)

def save_into_staging():
    """
    Save transformed data into a CSV file in the staging folder.

    This function reads three CSV files from the raw folder (books.csv, patrons.csv, loans.csv),
    transforms the data using transform_data, and saves the result into a CSV file named
    staging_data.csv in the staging folder.

    Returns
    -------
    None
    """
    FOLDER           = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
    RAW_FOLDER       = FOLDER + os.getenv('RAW_FOLDER')
    STAGING_DATABASE = FOLDER + '/staging/' + os.getenv('STAGING_DATABASE')

    df_books = pd.read_csv(RAW_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(RAW_FOLDER + '/patrons.csv')
    df_loans = pd.read_csv(RAW_FOLDER + '/loans.csv')

    transformed = transform_data(df_books, df_patrons, df_loans)

    save_dataframe_into_sqlite(transformed, STAGING_DATABASE, 'staging_data')

with DAG(
    dag_id='etl_to_datamart_552518',
    # schedule=None,
    schedule=timedelta(seconds=30),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': notify_failure,
    },
) as dag:

    save_raw_books = PythonOperator(
        task_id='save_raw_books',
        python_callable=extract_data_into,
        op_kwargs={'table': 'books', 'file_name': 'books.csv'}
    )

    save_raw_patrons = PythonOperator(
        task_id='save_raw_patrons',
        python_callable=extract_data_into,
        op_kwargs={'table': 'patrons', 'file_name': 'patrons.csv'}
    )

    save_raw_loans = PythonOperator(
        task_id='save_raw_loans',
        python_callable=extract_data_into,
        op_kwargs={'table': 'loans', 'file_name': 'loans.csv'}
    )

    save_into_staging = PythonOperator(
        task_id='save_into_staging',
        python_callable=save_into_staging,
    )

    [save_raw_books, save_raw_patrons, save_raw_loans] >> save_into_staging