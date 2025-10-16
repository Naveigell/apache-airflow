import os
from datetime import datetime

import pandas as pd

FOLDER  = os.path.dirname(os.path.abspath(__file__)) + '/../data/'

def send_notification():
    """
    Sends notification (currently logs to file) when ETL finishes successfully.
    Includes summary metrics from latest report and validation.
    """
    validation_log = "logs/datamart_validation.log"
    report_path = os.path.join(FOLDER, "datamart/reports/notification_log.csv")

    if os.path.exists(validation_log):
        with open(validation_log, "r") as f:
            validation_summary = f.read()
    else:
        validation_summary = "No validation summary found."

    if os.path.exists(report_path):
        df_report = pd.read_csv(report_path)
        report_summary = df_report.to_dict(orient="records")[0]
    else:
        report_summary = {}

    message = f"""
    📢 Data Mart ETL Notification - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    ----------------------------------------------------
    ✅ ETL Completed Successfully!

    📊 Validation Summary:
    {validation_summary}

    📈 Report Summary:
    {report_summary}
    """

    os.makedirs("logs/", exist_ok=True)
    with open("logs/notification.log", "a") as f:
        f.write(message + "\n")

    print(message)

send_notification()