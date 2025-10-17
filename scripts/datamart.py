import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv

from scripts.utils import save_data_into_csv

load_dotenv()

FOLDER              = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_DATABASE    = FOLDER + 'staging/' + os.getenv('STAGING_DATABASE')
DATAMART_DATABASE   = FOLDER + 'datamart/' + os.getenv('DATAMART_DATABASE')
DATAMART_TABLE_NAME = 'staging_data'

DATAMART_TEMPORARY_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/../data/' + os.getenv('DATAMART_FOLDER') + '/temp'


def get_db_connection(db_path):
    """
    Returns a connection to a SQLite database at the given path.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.

    Returns
    -------
    sqlite3.Connection
        A connection to the SQLite database.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    return sqlite3.connect(db_path)


def save_dataframe_into_sqlite(df, db_path, table_name, if_exists = 'replace'):
    """
    Save a pandas DataFrame into a SQLite database.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to save.
    db_path : str
        Path to the SQLite database file.
    table_name : str
        Name of the table to save the data into.
    if_exists : str, optional
        How to behave if the table already exists. Default is 'replace'.

    Notes
    -----
    - This function will create the database file if it does not exist.
    - This function will raise an exception if there is an error while saving the data.
    """
    conn = None
    try:
        conn = get_db_connection(db_path)

        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    except Exception as e:
        raise ValueError (f"SQlite error in ({table_name}): {e}")
    finally:
        if conn:
            conn.close()


def create_dim_date(conn, df_staging):

    """
    Create a Dim_Date table in the data mart database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the SQLite database file.
    df_staging : pandas.DataFrame
        DataFrame containing the staging data

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the Dim_Date table data
    """
    all_dates = pd.concat([ # create date from staging data and make it unique
        pd.to_datetime(df_staging['loan_timestamp'], errors='coerce').dt.normalize(),
        pd.to_datetime(df_staging['due_date'], errors='coerce'),
        pd.to_datetime(df_staging['return_date'], errors='coerce'),
        pd.to_datetime(df_staging['membership_date'], errors='coerce'),
    ]).dropna().unique()

    # create date ranges from year now to year now + 1
    min_date = pd.to_datetime(all_dates).min() if len(all_dates) > 0 else pd.Timestamp('2025-01-01')
    max_date = pd.Timestamp.today() + pd.DateOffset(years=1)
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')

    # and then create dataframe by the date range
    df_date = pd.DataFrame({'date': date_range})
    df_date['date_key'] = (df_date['date'].dt.strftime('%Y%m%d')).astype(int)
    df_date['full_date'] = df_date['date'].dt.date
    df_date['year'] = df_date['date'].dt.year
    df_date['month'] = df_date['date'].dt.month
    df_date['day_name'] = df_date['date'].dt.day_name()
    df_date['is_weekend'] = (df_date['date'].dt.dayofweek >= 5).astype(int)

    # create default unknown date
    unknown_date = pd.DataFrame({
        'date_key': [-1],
        'full_date': [None],
        'year': [None],
        'month': [None],
        'day_name': ['Unknown'],
        'is_weekend': [None]
    })

    # and then concat unknown date to the dataframe
    df_date = pd.concat([unknown_date, df_date], ignore_index=True)

    # save dataframe into datamart
    save_dataframe_into_sqlite(df_date, DATAMART_DATABASE, 'Dim_Date', if_exists='replace')

    return df_date[['date_key', 'full_date', 'year', 'month']]


def create_dim_book_patron(conn, df_staging):
    """
    Create Dim_Book and Dim_Patron tables in the data mart database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the SQLite database file.
    df_staging : pandas.DataFrame
        DataFrame containing the staging data

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the Dim_Book and Dim_Patron table data
    """
    # drop duplicate book_id, book_title and genre, and then increment book key
    df_book = df_staging[['book_id', 'book_title', 'genre']].drop_duplicates(subset=['book_id']).reset_index(drop=True)
    df_book['book_key'] = df_book.index + 1
    # now take only the 'book_key', 'book_id', 'book_title' and 'genre'
    df_book = df_book[['book_key', 'book_id', 'book_title', 'genre']]

    # and then save into sqlite datamart with replace if exists
    save_dataframe_into_sqlite(df_book, DATAMART_DATABASE, 'Dim_Book', if_exists='replace')

    # do the same for patron
    df_patron = df_staging[['patron_id', 'patron_name', 'membership_date']].drop_duplicates(subset=['patron_id']).reset_index(drop=True)
    df_patron['patron_key'] = df_patron.index + 1
    df_patron = df_patron[['patron_key', 'patron_id', 'patron_name', 'membership_date']]

    # and then save into sqlite datamart with replace if exists
    save_dataframe_into_sqlite(df_patron, DATAMART_DATABASE, 'Dim_Patron', if_exists='replace')

    # return only the 'book_key' and 'patron_key' with 'book_id' and 'patron_id'
    return df_book[['book_key', 'book_id']], df_patron[['patron_key', 'patron_id']]


def create_fact_loan_transactions(conn, df_staging, df_dim_book, df_dim_patron, df_dim_date):
    """
    Create the Fact_Loan_Transactions table in the data mart database.

    This function takes the staging data, Dim_Book, Dim_Patron, and Dim_Date tables, and
    creates the Fact_Loan_Transactions table in the data mart database.

    It first creates copies of the staging data, and then performs the following operations:

    * Converts the loan_timestamp, due_date, and return_date columns to datetime objects
    * Creates a loan_count column with a value of 1
    * Creates a return_date_filled column that fills NaN values with the current date
    * Creates a days_loaned column that calculates the difference between the return_date_filled and loan_timestamp columns
    * Creates an is_overdue column that marks a row as overdue if the return_date is greater than the due_date and the status is 'Returned' or 'Overdue'
    * Creates a loan_date_key, due_date_key, and return_date_key column by applying the get_date_key function to the loan_timestamp, due_date, and return_date columns, respectively
    * Merges the Dim_Book and Dim_Patron tables into the staging data
    * Selects the required columns and saves the result into the Fact_Loan_Transactions table in the data mart database

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the SQLite database file.
    df_staging : pandas.DataFrame
        DataFrame containing the staging data.
    df_dim_book : pandas.DataFrame
        DataFrame containing the Dim_Book table data.
    df_dim_patron : pandas.DataFrame
        DataFrame containing the Dim_Patron table data.
    df_dim_date : pandas.DataFrame
        DataFrame containing the Dim_Date table data.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the Fact_Loan_Transactions table data
    """
    df_loans_fact = df_staging[
        ['loan_id', 'book_id', 'patron_id', 'loan_timestamp', 'due_date', 'return_date', 'status']
    ].copy()

    df_loans_fact['loan_timestamp'] = pd.to_datetime(df_loans_fact['loan_timestamp'], errors='coerce')
    df_loans_fact['due_date'] = pd.to_datetime(df_loans_fact['due_date'], errors='coerce')
    df_loans_fact['return_date'] = pd.to_datetime(df_loans_fact['return_date'], errors='coerce')

    df_loans_fact['loan_count'] = 1
    df_loans_fact['return_date_filled'] = df_loans_fact['return_date'].fillna(pd.Timestamp.now().normalize())
    df_loans_fact['days_loaned'] = (df_loans_fact['return_date_filled'] - df_loans_fact['loan_timestamp']).dt.days

    returned_overdue = (df_loans_fact['return_date'] > df_loans_fact['due_date']) & (df_loans_fact['status'] == 'Returned')
    active_overdue = (df_loans_fact['status'] == 'Overdue')
    df_loans_fact['is_overdue'] = (returned_overdue | active_overdue).astype(int)

    df_loans_fact = df_loans_fact.merge(df_dim_book, on='book_id', how='left')
    df_loans_fact = df_loans_fact.merge(df_dim_patron, on='patron_id', how='left')

    df_loans_fact['loan_date_key'] = df_loans_fact['loan_timestamp'].apply(get_date_key)
    df_loans_fact['due_date_key'] = df_loans_fact['due_date'].apply(get_date_key)
    df_loans_fact['return_date_key'] = df_loans_fact['return_date'].apply(get_date_key)

    fact_columns = [
        'loan_id', 'book_key', 'patron_key',
        'loan_date_key', 'due_date_key', 'return_date_key',
        'loan_count', 'days_loaned', 'is_overdue', 'status'
    ]

    df_fact = df_loans_fact[fact_columns].copy()

    save_dataframe_into_sqlite(df_fact, DATAMART_DATABASE, 'Fact_Loan_Transactions', if_exists='replace')

    return df_fact


def get_date_key(x):
    """
    Return an integer representing the date key (YYYYMMDD) of the given datetime object x.

    If x is null, return -1.

    Parameters
    ----------
    x : datetime
        The datetime object to get the date key from.

    Returns
    -------
    int
        The date key of the given datetime object x.
    """
    return int(x.strftime('%Y%m%d')) if pd.notnull(x) else -1


def create_loan_performance_mart(conn, df_fact, df_dim_date):
    """
    Create the Loan_Performance_Mart table in the data mart database.

    This function takes the fact table and Dim_Date table, and creates the Loan_Performance_Mart table in the data mart database.
    It first merges the fact table with the Dim_Date table on the loan_date_key column, and then groups the result by book_key, year, and month.
    It then calculates the total_loans, avg_days_loaned, total_overdue, and monthly_overdue_rate columns for each group.
    Finally, it saves the result into the Loan_Performance_Mart table in the data mart database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the SQLite database file.
    df_fact : pandas.DataFrame
        DataFrame containing the fact table data.
    df_dim_date : pandas.DataFrame
        DataFrame containing the Dim_Date table data.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the Loan_Performance_Mart table data.
    """
    df_fact = df_fact.merge(
        df_dim_date[['date_key', 'year', 'month']],
        left_on='loan_date_key', right_on='date_key', how='left', suffixes=('', '_loan_date')
    )

    df_loan_mart = df_fact.groupby(['book_key', 'year', 'month']).agg(
        total_loans=('loan_count', 'sum'),
        avg_days_loaned=('days_loaned', 'mean'),
        total_overdue=('is_overdue', 'sum')
    ).reset_index()

    df_loan_mart['monthly_overdue_rate'] = (df_loan_mart['total_overdue'] / df_loan_mart['total_loans']).fillna(0)

    save_dataframe_into_sqlite(df_loan_mart, DATAMART_DATABASE, 'Loan_Performance_Mart', if_exists='replace')


def create_monthly_kpi_summary(conn, df_fact, df_dim_date):
    """
    Create the Monthly_KPI_Summary table in the data mart database.

    This function takes the fact table and Dim_Date table, and creates the Monthly_KPI_Summary table in the data mart database.
    It first merges the fact table with the Dim_Date table on the loan_date_key column, and then groups the result by year and month.
    It then calculates the monthly_loan_volume, avg_loan_duration_system, total_overdue_volume, and overdue_rate_kpi columns for each group.
    Finally, it saves the result into the Monthly_KPI_Summary table in the data mart database.

    Parameters
    ----------
    conn : sqlite3.Connection
        Connection to the SQLite database file.
    df_fact : pandas.DataFrame
        DataFrame containing the fact table data.
    df_dim_date : pandas.DataFrame
        DataFrame containing the Dim_Date table data.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the Monthly_KPI_Summary table data.
    """
    df_fact = df_fact.merge(
        df_dim_date[['date_key', 'year', 'month']],
        left_on='loan_date_key', right_on='date_key', how='left', suffixes=('', '_loan_date')
    )

    df_kpi = df_fact.groupby(['year', 'month']).agg(
        monthly_loan_volume=('loan_count', 'sum'),
        avg_loan_duration_system=('days_loaned', 'mean'),
        total_overdue_volume=('is_overdue', 'sum')
    ).reset_index()

    df_kpi['overdue_rate_kpi'] = (df_kpi['total_overdue_volume'] / df_kpi['monthly_loan_volume']).fillna(0)

    save_dataframe_into_sqlite(df_kpi, DATAMART_DATABASE, 'Monthly_KPI_Summary', if_exists='replace')


def create_fact_tables():
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    df_staging = get_df_staging()

    df_dim_book   = pd.read_csv(DATAMART_TEMPORARY_FOLDER + '/dim_book.csv')
    df_dim_patron = pd.read_csv(DATAMART_TEMPORARY_FOLDER + '/dim_patron.csv')
    df_dim_date   = pd.read_csv(DATAMART_TEMPORARY_FOLDER + '/dim_date.csv')

    create_fact_loan_transactions(conn_datamart, df_staging, df_dim_book, df_dim_patron, df_dim_date)

    conn_datamart.close()

    remove_temporary_file('dim_book.csv')
    remove_temporary_file('dim_patron.csv')
    remove_temporary_file('dim_date.csv')

    os.removedirs(DATAMART_TEMPORARY_FOLDER)

def create_dim_tables():
    """
    Creates the dimension tables for the data mart.

    Reads the data from the staging database, creates the dimension tables (Dim_Date, Dim_Book, Dim_Patron) and saves them into CSV files in the data mart folder.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    df_staging = get_df_staging()

    conn_datamart = get_db_connection(DATAMART_DATABASE)

    # create dimension date table, book and patron
    df_dim_date                = create_dim_date(conn_datamart, df_staging)
    df_dim_book, df_dim_patron = create_dim_book_patron(conn_datamart, df_staging)

    conn_datamart.close()

    save_data_into_csv(df_dim_date, 'dim_date.csv', DATAMART_TEMPORARY_FOLDER)
    save_data_into_csv(df_dim_book, 'dim_book.csv', DATAMART_TEMPORARY_FOLDER)
    save_data_into_csv(df_dim_patron, 'dim_patron.csv', DATAMART_TEMPORARY_FOLDER)

def get_df_staging():
    """
    Gets the staging data from the staging database.

    Reads the data from the staging database, checks if the data is not empty,
    and then returns the staging data as a pandas DataFrame.

    Raises
    ------
    ValueError
        If the staging data is empty.

    Returns
    -------
    pandas.DataFrame
        The staging data as a pandas DataFrame.
    """
    conn_staging = get_db_connection(STAGING_DATABASE)
    df_staging = pd.read_sql(f"SELECT * FROM {DATAMART_TABLE_NAME}", conn_staging)

    if df_staging.empty:
        raise ValueError("Staging data is empty. Skipping Data Mart build.")

    conn_staging.close()

    return df_staging


def create_datamart():
    """
    Creates the data mart by reading data from the fact and dimension tables, creating the loan performance and monthly kpi summary tables, and saving the result into the data mart database.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Raises
    ------
    None

    """
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    df_fact = pd.read_sql(f"SELECT * FROM Fact_Loan_Transactions", conn_datamart)
    df_dim_date = pd.read_sql(f"SELECT * FROM Dim_Date", conn_datamart)

    # create loan performance mart
    create_loan_performance_mart(conn_datamart, df_fact, df_dim_date)

    conn_datamart.close()

def create_summary_stats():
    """
    Creates the monthly KPI summary table in the data mart database.

    Reads the data from the Fact_Loan_Transactions and Dim_Date tables, creates the monthly KPI summary table by calling the create_monthly_kpi_summary function, and saves the result into the data mart database.

    Parameters
    ----------
    None

    Returns
    -------
    None

    Raises
    ------
    None
    """
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    df_fact = pd.read_sql(f"SELECT * FROM Fact_Loan_Transactions", conn_datamart)
    df_dim_date = pd.read_sql(f"SELECT * FROM Dim_Date", conn_datamart)

    # create kpi summary
    create_monthly_kpi_summary(conn_datamart, df_fact, df_dim_date)

    conn_datamart.close()

def remove_temporary_file(file_name):
    """
    Removes a temporary file from the data mart temporary folder.

    Parameters
    ----------
    file_name : str
        The name of the file to be removed.

    Returns
    -------
    None
    """
    if os.path.exists(DATAMART_TEMPORARY_FOLDER + '/' + file_name):
        os.remove(DATAMART_TEMPORARY_FOLDER + '/' + file_name)