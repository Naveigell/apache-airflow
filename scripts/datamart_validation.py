import os
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd

from scripts.datamart import get_db_connection

load_dotenv()

FOLDER              = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_DATABASE    = FOLDER + 'staging/' + os.getenv('STAGING_DATABASE')
DATAMART_DATABASE   = FOLDER + 'datamart/' + os.getenv('DATAMART_DATABASE')

STAGING_DATABASE_TABLE_NAME = 'staging_data'

def validate_datamart():
    """
    Validates data mart integrity by checking row counts and consistency
    between staging and datamart tables.

    Returns
    -------
    dict
        Dictionary containing validation summary results.
    """
    conn_staging  = get_db_connection(STAGING_DATABASE)
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    counts = {}
    for table in ["Dim_Book", "Dim_Patron", "Dim_Date", "Fact_Loan_Transactions"]:
        counts[table] = pd.read_sql(f"SELECT COUNT(*) AS count FROM {table}", conn_datamart).iloc[0, 0]

    staging_count = pd.read_sql(f"SELECT COUNT(*) AS count FROM {STAGING_DATABASE_TABLE_NAME}", conn_staging).iloc[0, 0]

    conn_staging.close()
    conn_datamart.close()

    validations = {tbl: ("OK" if cnt > 0 else "EMPTY") for tbl, cnt in counts.items()}

    if counts["Fact_Loan_Transactions"] <= staging_count:
        validations["Fact_Loan_Transactions"] = "Row count is valid"
    else:
        validations["Fact_Loan_Transactions"] = "Unexpectedly larger than staging"

    summary = {
        "row_counts": counts,
        "staging_count": staging_count,
        "validation_status": validations
    }

    os.makedirs("logs/validation", exist_ok=True)

    with open("logs/validation/datamart_validation_" + datetime.now().strftime("%Y%m%d") + ".log", "w") as f:
        f.write(str(summary))

    return summary