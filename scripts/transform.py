import os
from datetime import datetime

import pandas as pd

from dotenv import load_dotenv

load_dotenv()

def transform_data(df_books, df_patrons, df_loans):
    """
    Transforms the staging data into a format suitable for the data mart.

    This function takes the staging data, merges the books and patrons data into the loans
    data, converts the timestamp columns into datetime objects, fills the return_date
    column with the current date if it is null, calculates the duration days, and
    creates a new boolean column to mark late returns.

    Parameters
    ----------
    df_books : pandas.DataFrame
        DataFrame containing the staging data for books.
    df_patrons : pandas.DataFrame
        DataFrame containing the staging data for patrons.
    df_loans : pandas.DataFrame
        DataFrame containing the staging data for loans.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the transformed data.
    """
    # merge df_loans and df_books by its book_id
    df_transformed = pd.merge(df_loans, df_books[['book_id', 'title', 'genre', 'author']],
                              on='book_id', how='left')

    # and then the db_transformed merge with df_patrons by its patron_id
    df_transformed = pd.merge(df_transformed, df_patrons[['patron_id', 'name', 'membership_date']],
                              on='patron_id', how='left')

    # now we need to convert the timestamp into datetime
    date_cols = ['loan_timestamp', 'due_date', 'return_date']
    for col in date_cols:

        if col == 'loan_timestamp':
            df_transformed[col] = pd.to_datetime(df_transformed[col])
        else:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')

    now = datetime.now()

    # fill the return_date with current date if it is null
    df_transformed['actual_return_date'] = df_transformed['return_date'].fillna(now)

    # calculate the duration days
    df_transformed['duration_days'] = (df_transformed['actual_return_date'] - df_transformed['loan_timestamp']).dt.days

    # fill return_check_date into noew if empty
    df_transformed['return_check_date'] = df_transformed['return_date'].fillna(now)

    # add new boolean column, if the return date is greater than due date, then it is late
    df_transformed['is_late'] = (df_transformed['return_check_date'] > df_transformed['due_date'])

    # drop the actual_return_date and return_check_date (no needed for datamart in the future)
    df_transformed = df_transformed.drop(columns=['actual_return_date', 'return_check_date'])

    # now we rename the column
    df_transformed.rename(columns={
        'name': 'patron_name',
        'title': 'book_title'
    }, inplace=True)

    # take the needed column
    final_cols = [
        'loan_id', 'book_id', 'patron_id',
        'patron_name', 'membership_date',
        'book_title', 'genre', 'author',
        'loan_timestamp', 'due_date', 'return_date',
        'status', 'duration_days', 'is_late'
    ]
    df_transformed = df_transformed[final_cols]

    return df_transformed


