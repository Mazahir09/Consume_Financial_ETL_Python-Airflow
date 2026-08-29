# Consumer Financial Complaints ETL Pipeline

An end-to-end **Data Engineering ETL pipeline** built with Python and Apache Airflow to acquire, store, transform, and publish Consumer Financial Protection Bureau (CFPB) complaint data.

The project demonstrates the complete data engineering workflow:

**Acquisition → Storage → Transformation → Orchestration → Reporting → Notification**

---

## Project Overview

The Consumer Financial Protection Bureau (CFPB) provides publicly available consumer complaint data through an API.

The objective of this project is to build an automated pipeline that:

1. Extracts consumer complaint data from the CFPB API.
2. Iterates through multiple U.S. states while respecting API rate limits.
3. Stores the raw extracted data as JSON.
4. Loads the raw data into a MySQL database.
5. Transforms and aggregates the data using Pandas.
6. Exports the transformed dataset to CSV.
7. Uploads the transformed data to Google Sheets.
8. Sends an automated email containing the Google Sheets URL after successful completion.

---

## Architecture

```text
                    ┌──────────────────────┐
                    │      CFPB API        │
                    │ Consumer Complaints  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │       Extract        │
                    │      extract.py      │
                    │                      │
                    │  State-by-state API  │
                    │      extraction      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │     Raw JSON File    │
                    │      data.json       │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │        MySQL         │
                    │   Raw Complaints     │
                    │      complaints      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      Transform       │
                    │     transform.py     │
                    │                      │
                    │       Pandas         │
                    │   Cleaning & Grouping│
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Transformed CSV    │
                    │ complaints_transformed│
                    │        .csv          │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Google Sheets     │
                    │   Transformed Data  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   Email Notification │
                    │   Google Sheet URL   │
                    └──────────────────────┘
```

---

## Airflow DAG

The entire workflow is orchestrated using **Apache Airflow**.

The DAG is named:

```text
consumer_financial_etl
```

### DAG Workflow

```text
extract_data_from_source
          ↓
dump_data_to_mysql
          ↓
transform_data
          ↓
check_consumer_financial_csv
          ↓
dump_to_googlesheet
          ↓
send_googlesheet_url_via_email
```

Airflow ensures that each stage runs only after the previous stage has completed successfully.

---

## Project Structure

```text
consumer-financial-etl/
│
├── consumer_financial_etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── load.py
│   └── transform.py
│
├── etl_dag.py
├── consumer_complaints_transformed.csv
├── requirements.txt
├── .gitignore
└── README.md
```

### File Description

| File                                  | Description                                                             |
| ------------------------------------- | ----------------------------------------------------------------------- |
| `etl_dag.py`                          | Defines and orchestrates the complete Airflow workflow                  |
| `extract.py`                          | Extracts complaint data from the CFPB API                               |
| `load.py`                             | Loads raw data into MySQL and uploads transformed data to Google Sheets |
| `transform.py`                        | Reads data from MySQL and performs Pandas-based transformation          |
| `requirements.txt`                    | Python dependencies required for the project                            |
| `consumer_complaints_transformed.csv` | Sample/output transformed dataset                                       |

---

# 1. Data Extraction

The extraction process uses the publicly available **CFPB Consumer Complaint API**.

The API endpoint used by the project is:

```text
https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/
```

The pipeline first retrieves the list of U.S. states and then performs an API request for each state.

### Extraction Parameters

The API request uses parameters including:

```text
field
size
date_received_max
date_received_min
state
```

The extraction currently processes a 30-day date range ending on:

```text
2024-08-31
```

For every state, the script:

1. Sends a request to the CFPB API.
2. Checks the HTTP response.
3. Extracts complaint records from the JSON response.
4. Adds the records to the overall dataset.
5. Waits between requests to reduce the risk of hitting API rate limits.

The extracted data is stored as:

```text
data.json
```

---

# 2. Loading Data into MySQL

After extraction, the JSON file path is passed from the extraction task to the MySQL loading task using **Airflow XCom**.

The loader:

1. Reads the extracted JSON file.
2. Converts the data into a Pandas DataFrame.
3. Converts the DataFrame into rows.
4. Loads the records into the MySQL `complaints` table.
5. Inserts records in batches using `commit_every=1000`.

The MySQL connection is managed through an **Airflow Connection** rather than storing database credentials in the source code.

Example Airflow connection ID:

```text
mysql_default
```

The connection credentials should be configured directly in Airflow.

---

# 3. Data Transformation

The transformation stage reads the complaint data from MySQL using `MySqlHook` and processes it with Pandas.

### Transformation Steps

The pipeline removes fields that are not required for the final reporting dataset, including:

```text
id
complaint_what_happened
date_sent_to_company
zip_code
tags
has_narrative
consumer_consent_provided
consumer_disputed
company_public_response
```

The `date_received` field is converted into a month-year format:

```text
January 2024
February 2024
March 2024
...
```

The column is renamed to:

```text
Month_Year
```

Missing and empty values are standardized.

The data is then grouped using:

```text
product
sub_product
issue
sub_issue
submitted_via
company
state
timely
company_response
Month_Year
```

The number of unique complaints is calculated using:

```text
complaint_id
```

The resulting metric is stored as:

```text
Count_Of_Complaint_ids
```

---

# 4. Transformed Dataset

The final transformed dataset contains the following columns:

```text
product
sub_product
issue
sub_issue
submitted_via
company
state
timely
company_response
Month_Year
Count_Of_Complaint_ids
```

The transformed data is written to:

```text
complaints_transformed.csv
```

---

# 5. File Sensor

Before uploading the transformed dataset to Google Sheets, the Airflow DAG uses a `FileSensor`.

The sensor checks whether:

```text
/opt/airflow/dags/complaints_transformed.csv
```

exists.

The sensor checks every 10 seconds and waits for a maximum of 600 seconds.

This ensures that the Google Sheets loading task only starts after the transformation output has been successfully generated.

---

# 6. Google Sheets Integration

The transformed CSV is uploaded to Google Sheets using `pygsheets`.

The pipeline:

1. Authenticates using a Google service account.
2. Opens the configured spreadsheet.
3. Looks for the `Transformed Data` worksheet.
4. Creates the worksheet if it does not already exist.
5. Reads the transformed CSV.
6. Adjusts the worksheet dimensions if required.
7. Uploads the DataFrame to the worksheet.
8. Returns the Google Sheets URL through Airflow XCom.

### Security

The Google service-account credential file is **not included in this repository**.

The credential path is provided through an environment variable:

```text
GOOGLE_SERVICE_ACCOUNT_FILE
```

The Google Sheet name can also be configured through:

```text
GOOGLE_SHEET_NAME
```

---

# 7. Email Notification

As a bonus component, the pipeline sends an automated email after the Google Sheets upload completes.

The email contains the URL of the generated Google Sheet.

The email task is executed only after:

```text
Google Sheets Upload
```

has completed successfully.

Email credentials are managed through an Airflow SMTP connection:

```text
smtp_default
```

No email passwords or SMTP credentials are stored in the repository.

The recipient email can be configured through an Airflow Variable:

```text
completion_email
```

---

# 8. Airflow Connections and Variables

The project uses Airflow's built-in connection and variable management instead of hardcoding sensitive information.

### MySQL Connection

Configure an Airflow connection such as:

```text
Connection ID: mysql_default
Connection Type: MySQL
```

Add the appropriate:

```text
Host
Port
Username
Password
Database
```

inside Airflow.

### SMTP Connection

Configure:

```text
Connection ID: smtp_default
```

with the required SMTP settings.

### Airflow Variable

Create:

```text
completion_email
```

and set it to the desired email recipient.

---

# 9. Environment Variables

The following environment variables can be used to configure file paths and external credentials:

```text
MYSQL_CONN_ID
GOOGLE_SERVICE_ACCOUNT_FILE
GOOGLE_SHEET_NAME
RAW_DATA_PATH
TRANSFORMED_DATA_PATH
REQUEST_DELAY
```

Example:

```bash
export MYSQL_CONN_ID=mysql_default
export GOOGLE_SERVICE_ACCOUNT_FILE=/opt/airflow/dags/keys/service-account.json
export GOOGLE_SHEET_NAME=Consumer_Complaints
export RAW_DATA_PATH=/opt/airflow/dags/data.json
export TRANSFORMED_DATA_PATH=/opt/airflow/dags/complaints_transformed.csv
export REQUEST_DELAY=1
```

These values are environment-specific and should not contain secrets inside the Git repository.

---

# 10. Installation

## Prerequisites

Before running the project, install/configure:

* Python 3.x
* Apache Airflow
* MySQL
* Google Cloud service account with Google Sheets access
* SMTP connection for email notification

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

---

# 11. Airflow Setup

Copy the DAG and package into the Airflow DAGs directory.

Example:

```text
/opt/airflow/dags/
│
├── etl_dag.py
│
├── consumer_financial_etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── load.py
│   └── transform.py
```

Make sure the Airflow scheduler and webserver are running.

The DAG should then appear in the Airflow UI as:

```text
consumer_financial_etl
```

---

# 12. Database Setup

Create a MySQL database and a `complaints` table containing the fields expected by the loading task.

The table should include fields corresponding to:

```text
product
complaint_what_happened
date_sent_to_company
issue
sub_product
zip_code
tags
has_narrative
complaint_id
timely
consumer_consent_provided
company_response
submitted_via
company
date_received
state
consumer_disputed
company_public_response
sub_issue
```

The MySQL credentials should be configured through an Airflow Connection.

---

# 13. Running the Pipeline

Once Airflow, MySQL, Google Sheets authentication, and SMTP have been configured:

1. Start Airflow.
2. Open the Airflow web interface.
3. Locate `consumer_financial_etl`.
4. Trigger the DAG manually.
5. Monitor the task execution in the Grid or Graph view.

The pipeline executes in the following order:

```text
Extract
  ↓
MySQL Load
  ↓
Transform
  ↓
File Sensor
  ↓
Google Sheets Load
  ↓
Email Notification
```

---

# 14. Error Handling

The extraction process includes handling for:

* HTTP request failures
* API errors
* Request timeouts
* Individual state extraction failures

The MySQL loading stage validates that extracted data is available before loading.

The transformation stage validates that the MySQL table contains data.

The Google Sheets stage validates that the credential configuration and transformed CSV are available.

Airflow task dependencies ensure that downstream tasks are not executed when an upstream task fails.

---

# 15. Rate Limiting

The extraction process runs requests state-by-state.

A configurable delay is included between API requests:

```text
REQUEST_DELAY
```

The default value is:

```text
1 second
```

This helps reduce the risk of excessive API requests and demonstrates consideration for API rate limits.

---

# 16. Security

No sensitive credentials should be committed to this repository.

The following should remain outside GitHub:

```text
.env
Google service-account JSON
Database passwords
SMTP passwords
API credentials
Airflow metadata/database credentials
```

The project uses:

* Airflow Connections for database and SMTP credentials.
* Environment variables for configurable paths and external credential locations.
* Airflow Variables for configurable email recipients.

---

# 17. Assignment Requirements Coverage

| Requirement               | Implementation  |
| ------------------------- | --------------- |
| API data acquisition      | ✅ CFPB API      |
| State-wise extraction     | ✅               |
| Date range filtering      | ✅               |
| Rate-limit handling       | ✅ Request delay |
| Raw data storage          | ✅ JSON          |
| MySQL loading             | ✅               |
| Pandas transformation     | ✅               |
| Aggregation               | ✅               |
| CSV output                | ✅               |
| Google Sheets upload      | ✅               |
| Airflow orchestration     | ✅               |
| FileSensor                | ✅               |
| Completion email          | ✅ Bonus         |
| DAG dependency management | ✅               |
| Error handling            | ✅               |

---

# 18. Technologies

* **Python**
* **Apache Airflow**
* **Pandas**
* **NumPy**
* **Requests**
* **MySQL**
* **Google Sheets**
* **pygsheets**
* **SMTP**
* **JSON**
* **CSV**

---

# 19. Learning Outcomes

This project demonstrates practical experience with:

* REST API consumption
* Data acquisition
* API pagination/state iteration
* Rate-limit handling
* JSON processing
* Relational database loading
* MySQL integration with Airflow
* Pandas-based data transformation
* Data aggregation
* CSV generation
* Google Sheets integration
* Airflow DAG design
* Task dependencies
* Sensors
* XCom communication
* Automated email notifications
* Managing credentials outside source code

---

## Author

**Mazahir Hussain**

Data Engineering | Python | SQL | Apache Airflow | ETL
