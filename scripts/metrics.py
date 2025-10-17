import os
import time
from datetime import datetime

from dotenv import load_dotenv

import pandas as pd

from scripts.datamart import get_db_connection

load_dotenv()

ROOT_FOLDER         = os.path.dirname(os.path.abspath(__file__))
REPORT_FOLDER       = ROOT_FOLDER + '/../reports/'
FOLDER              = ROOT_FOLDER + '/../data/'
STAGING_DATABASE    = FOLDER + 'staging/' + os.getenv('STAGING_DATABASE')
DATAMART_DATABASE   = FOLDER + 'datamart/' + os.getenv('DATAMART_DATABASE')

def generate_report():
    """
    Generate a report containing summary statistics of the data mart.

    This function generates a report containing various summary statistics
    of the data mart, including the total number of records in the
    fact table, the number of unique books and patrons, the average
    loan duration, the total number of overdue loans, and the
    execution time in seconds.

    The report is saved to a CSV file in the "datamart/reports/"
    directory.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    # make sure directory report exists
    os.makedirs(os.path.join(REPORT_FOLDER), exist_ok=True)

    report_path = os.path.join(REPORT_FOLDER, "summary_" + datetime.now().strftime("%Y%m%d") + ".csv")

    start_time = time.time()

    df_fact = pd.read_sql(f"SELECT * FROM Fact_Loan_Transactions", conn_datamart)
    df_dim_date = pd.read_sql(f"SELECT * FROM Dim_Date", conn_datamart)

    # create metrics for reporting
    metrics = {
        "total_records_fact": len(df_fact),
        "unique_books": df_fact["book_key"].nunique(),
        "unique_patrons": df_fact["patron_key"].nunique(),
        "avg_loan_duration": df_fact["days_loaned"].mean(),
        "total_overdue": df_fact["is_overdue"].sum(),
        "execution_time_sec": round(time.time() - start_time, 2),
    }

    pd.DataFrame([metrics]).to_csv(report_path, index=False)

    conn_datamart.close()
