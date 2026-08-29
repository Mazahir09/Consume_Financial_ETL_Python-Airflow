
import json
import os

import pandas as pd
import pygsheets

from airflow.providers.mysql.hooks.mysql import MySqlHook


MYSQL_CONN_ID = os.getenv(
    "MYSQL_CONN_ID",
    "mysql_default",
)

GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
)

GOOGLE_SHEET_NAME = os.getenv(
    "GOOGLE_SHEET_NAME",
    "Consumer_Complaints",
)

TRANSFORMED_DATA_PATH = os.getenv(
    "TRANSFORMED_DATA_PATH",
    "/opt/airflow/dags/complaints_transformed.csv",
)

MYSQL_COLUMNS = [
    "product",
    "complaint_what_happened",
    "date_sent_to_company",
    "issue",
    "sub_product",
    "zip_code",
    "tags",
    "has_narrative",
    "complaint_id",
    "timely",
    "consumer_consent_provided",
    "company_response",
    "submitted_via",
    "company",
    "date_received",
    "state",
    "consumer_disputed",
    "company_public_response",
    "sub_issue",
]


def load_data(**context):
    """
    Load extracted JSON data into MySQL.
    """

    file_path = context["ti"].xcom_pull(
        task_ids="extract_data_from_source"
    )

    if not file_path:
        raise ValueError(
            "No file received from extract task."
        )

    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:
        data = json.load(file)

    if not data:
        raise ValueError(
            "Extracted data is empty."
        )

    df = pd.DataFrame(data)

    rows = df.reindex(
        columns=MYSQL_COLUMNS
    ).values.tolist()

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    hook.insert_rows(
        table="complaints",
        rows=rows,
        target_fields=MYSQL_COLUMNS,
        commit_every=1000,
    )

    print(
        f"Loaded {len(rows)} records into MySQL."
    )


def csv_to_googlesheet():
    """
    Upload the transformed CSV file to Google Sheets.
    Google service-account credentials are supplied
    through an environment variable.
    """

    if not GOOGLE_SERVICE_ACCOUNT_FILE:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_FILE environment variable "
            "is not configured."
        )

    if not os.path.exists(
        GOOGLE_SERVICE_ACCOUNT_FILE
    ):
        raise FileNotFoundError(
            "Google service-account file was not found."
        )

    client = pygsheets.authorize(
        service_account_file=GOOGLE_SERVICE_ACCOUNT_FILE
    )

    spreadsheet = client.open(
        GOOGLE_SHEET_NAME
    )

    try:
        worksheet = spreadsheet.worksheet_by_title(
            "Transformed Data"
        )

    except pygsheets.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            "Transformed Data"
        )

    df = pd.read_csv(
        TRANSFORMED_DATA_PATH
    )

    required_rows = len(df) + 1
    required_columns = len(df.columns)

    if required_rows > worksheet.rows:
        worksheet.add_rows(
            required_rows - worksheet.rows
        )

    if required_columns > worksheet.cols:
        worksheet.add_cols(
            required_columns - worksheet.cols
        )

    worksheet.set_dataframe(
        df,
        (1, 1),
        copy_head=True,
    )

    print(
        "Transformed data successfully uploaded "
        "to Google Sheets."
    )

    return spreadsheet.url

