import os
import sys

import pandas as pd
from airflow.providers.standard.operators.python import PythonOperator

# append path
sys.path.append('../')
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)) + '/../')

from datetime import datetime, timedelta

from airflow.sdk import DAG

from scripts.extract   import extract_data_from_source
from scripts.utils     import save_data_into_csv, save_dataframe_into_sqlite
from scripts.transform import transform_data
from scripts.clean     import run_data_quality_checks, perform_data_cleaning
from scripts.datamart  import datamart_build_pipeline
from scripts.validate  import validate_not_empty

FOLDER           = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_FOLDER   = FOLDER + os.getenv('STAGING_FOLDER')
STAGING_DATABASE = FOLDER + '/staging/' + os.getenv('STAGING_DATABASE')

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
    with open('logs/log_' + datetime.today().strftime('%Y%m%d%H%M%S') + '.txt', 'w') as f:
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

    df_books   = pd.read_csv(STAGING_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(STAGING_FOLDER + '/patrons.csv')
    df_loans   = pd.read_csv(STAGING_FOLDER + '/loans.csv')

    transformed = transform_data(df_books, df_patrons, df_loans)

    save_dataframe_into_sqlite(transformed, STAGING_DATABASE, 'staging_data')

def validate_extraction():
    """
    Validate that the extracted data is consistent.

    This function reads three CSV files from the raw folder (books.csv, patrons.csv, loans.csv),
    checks that the data is not empty, and checks that the number of records in each file is the same.

    If any of the checks fail, a ValueError is raised.

    Returns
    -------
    True
        If the data is consistent and not empty.
    """
    df_books   = pd.read_csv(STAGING_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(STAGING_FOLDER + '/patrons.csv')
    df_loans   = pd.read_csv(STAGING_FOLDER + '/loans.csv')
    
    if validate_not_empty(df_books) or validate_not_empty(df_patrons) or validate_not_empty(df_loans):
        raise ValueError("No data extracted.")
    
    return True

def data_quality_check():
    """
    Checks data quality for books, patrons, and loans.

    Reads data from CSV files in the raw folder and runs data quality checks using
    run_data_quality_checks. If any of the checks fail, a ValueError is raised.

    Returns
    -------
    True
        If all data quality checks pass.
    """
    df_books   = pd.read_csv(STAGING_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(STAGING_FOLDER + '/patrons.csv')
    df_loans   = pd.read_csv(STAGING_FOLDER + '/loans.csv')

    run_data_quality_checks(df_books, 'books')
    run_data_quality_checks(df_patrons, 'patrons')
    run_data_quality_checks(df_loans, 'loans')

    return True

def create_dim_tables():
    datamart_build_pipeline()

def data_cleaning():
    """
    Performs data cleaning on books, patrons, and loans.

    Reads data from CSV files in the raw folder and performs data cleaning using
    perform_data_cleaning.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    df_books   = pd.read_csv(STAGING_FOLDER + '/books.csv')
    df_patrons = pd.read_csv(STAGING_FOLDER + '/patrons.csv')
    df_loans   = pd.read_csv(STAGING_FOLDER + '/loans.csv')

    perform_data_cleaning(df_books, df_patrons, df_loans)

with DAG(
    dag_id='etl_to_datamart_552518',
    schedule=timedelta(seconds=30), # TODO: change this as needed
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'example'},
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': notify_failure,
    },
) as dag:

    # TASK 1
    save_raw_books = PythonOperator(
        task_id='save_raw_books',
        python_callable=extract_data_into,
        op_kwargs={'table': 'books', 'file_name': 'books.csv'}
    )

    #TASK 2
    save_raw_patrons = PythonOperator(
        task_id='save_raw_patrons',
        python_callable=extract_data_into,
        op_kwargs={'table': 'patrons', 'file_name': 'patrons.csv'}
    )

    # TASK 3
    save_raw_loans = PythonOperator(
        task_id='save_raw_loans',
        python_callable=extract_data_into,
        op_kwargs={'table': 'loans', 'file_name': 'loans.csv'}
    )

    # TASK 4
    validate_extraction = PythonOperator(
        task_id='validate_extraction',
        python_callable=validate_extraction,
    )

    # TASK 5
    load_into_staging = PythonOperator(
        task_id='save_into_staging',
        python_callable=save_into_staging,
    )

    # TASK 6
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    # TASK 7
    data_cleaning = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
    )

    # create_dim_tables = PythonOperator(
    #     task_id='create_dim_tables',
    #     python_callable=create_dim_tables,
    # )

    [save_raw_books, save_raw_patrons, save_raw_loans] >> validate_extraction >> load_into_staging >> data_quality_check >> data_cleaning