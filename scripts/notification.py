import os
from datetime import datetime

import pandas as pd

ROOT_FOLDER = os.path.dirname(os.path.abspath(__file__))
FOLDER      = ROOT_FOLDER + '/../data/'

def send_notification():
    """
    Sends notification (currently logs to file) when ETL finishes successfully.
    Includes summary metrics from latest report and validation.
    """
    validation_log = ROOT_FOLDER + "logs/validation/datamart_" + datetime.now().strftime('%Y%m%d') + ".log"

    report_path = os.path.join(ROOT_FOLDER, "reports/summary_" + datetime.now().strftime("%Y%m%d") + ".csv")

    os.makedirs(os.path.dirname(report_path), exist_ok=True)

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
    with open("logs/notification_" + datetime.now().strftime('%Y%m%d') + ".log", "a") as f:
        f.write(message + "\n")