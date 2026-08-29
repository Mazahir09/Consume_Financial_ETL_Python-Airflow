```python
import json
import os
import time
from datetime import date, timedelta

import requests


API_URL = (
    "https://www.consumerfinance.gov/"
    "data-research/consumer-complaints/search/api/v1/"
)

STATES_URL = (
    "https://gist.githubusercontent.com/mshafrir/2646763/raw/"
    "8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"
)

OUTPUT_PATH = os.getenv(
    "RAW_DATA_PATH",
    "/opt/airflow/dags/data.json",
)

REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1"))

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
}


def get_data():
    """
    Extract consumer complaint data for all states
    within the configured date range.
    """

    states_response = requests.get(
        STATES_URL,
        timeout=10,
    )
    states_response.raise_for_status()

    list_of_states = list(states_response.json().keys())

    data = []

    session = requests.Session()
    session.headers.update(HEADERS)

    max_date = date(2024, 8, 31)
    min_date = max_date - timedelta(days=30)

    for state in list_of_states:

        params = {
            "field": "complaint_what_happened",
            "size": 10,
            "date_received_max": max_date.strftime("%Y-%m-%d"),
            "date_received_min": min_date.strftime("%Y-%m-%d"),
            "state": state,
        }

        try:
            response = session.get(
                API_URL,
                params=params,
                timeout=20,
            )

            response.raise_for_status()

            hits = response.json().get(
                "hits",
                {},
            ).get(
                "hits",
                [],
            )

            print(
                f"Extracting {len(hits)} records from {state}"
            )

            for hit in hits:
                source = hit.get("_source")

                if source:
                    data.append(source)

        except requests.RequestException as error:
            print(
                f"Error extracting data for {state}: {error}"
            )

        time.sleep(REQUEST_DELAY)

    with open(
        OUTPUT_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            indent=4,
            ensure_ascii=False,
        )

    print(
        f"Extraction completed. Total records: {len(data)}"
    )

    return OUTPUT_PATH
```
