import os
import sqlite3
import time
from datetime import datetime

from dotenv import load_dotenv

import pandas as pd

from scripts.datamart import get_db_connection

load_dotenv()

FOLDER              = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_DATABASE    = FOLDER + 'staging/' + os.getenv('STAGING_DATABASE')
DATAMART_DATABASE   = FOLDER + 'datamart/' + os.getenv('DATAMART_DATABASE')

def generate_report():
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    os.makedirs(os.path.join(FOLDER, "datamart/reports/"), exist_ok=True)
    report_path = os.path.join(FOLDER, "datamart/reports/datamart_summary_report_" + datetime.now().strftime("%Y%m%d") + ".csv")

    start_time = time.time()

    df_fact = pd.read_sql(f"SELECT * FROM Fact_Loan_Transactions", conn_datamart)
    df_dim_date = pd.read_sql(f"SELECT * FROM Dim_Date", conn_datamart)

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
