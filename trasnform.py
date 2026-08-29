```python
import os

import numpy as np
import pandas as pd

from airflow.providers.mysql.hooks.mysql import MySqlHook


MYSQL_CONN_ID = os.getenv(
    "MYSQL_CONN_ID",
    "mysql_default",
)

OUTPUT_PATH = os.getenv(
    "TRANSFORMED_DATA_PATH",
    "/opt/airflow/dags/complaints_transformed.csv",
)


def transform_data():
    """
    Transform complaint data from MySQL into
    an aggregated dataset for reporting.
    """

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    df = hook.get_pandas_df(
        "SELECT * FROM complaints;"
    )

    if df.empty:
        raise ValueError(
            "No data found in complaints table."
        )

    columns_to_remove = [
        "id",
        "complaint_what_happened",
        "date_sent_to_company",
        "zip_code",
        "tags",
        "has_narrative",
        "consumer_consent_provided",
        "consumer_disputed",
        "company_public_response",
    ]

    df = df.drop(
        columns=columns_to_remove,
        errors="ignore",
    )

    df["sub_issue"] = df["sub_issue"].replace(
        {
            None: np.nan,
            "": np.nan,
            " ": np.nan,
        }
    )

    df["date_received"] = pd.to_datetime(
        df["date_received"]
    )

    df["date_received"] = (
        df["date_received"]
        .dt.strftime("%B %Y")
    )

    df = df.rename(
        columns={
            "date_received": "Month_Year"
        }
    )

    df = df.replace(
        "",
        "Nan",
    )

    df = df.replace(
        {None: "Nan"}
    )

    df = df.replace(
        r"^\s*$",
        "Nan",
        regex=True,
    )

    group_columns = [
        "product",
        "sub_product",
        "issue",
        "sub_issue",
        "submitted_via",
        "company",
        "state",
        "timely",
        "company_response",
        "Month_Year",
    ]

    df = (
        df.groupby(group_columns)["complaint_id"]
        .nunique()
        .reset_index(
            name="Count_Of_Complaint_ids"
        )
    )

    output_columns = [
        "product",
        "sub_product",
        "issue",
        "sub_issue",
        "submitted_via",
        "company",
        "state",
        "timely",
        "company_response",
        "Month_Year",
        "Count_Of_Complaint_ids",
    ]

    df = df[output_columns]

    df.to_csv(
        OUTPUT_PATH,
        index=False,
    )

    print(
        f"Transformation completed. "
        f"Output rows: {len(df)}"
    )

    return OUTPUT_PATH
```
