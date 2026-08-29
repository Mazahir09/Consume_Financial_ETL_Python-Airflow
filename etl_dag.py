
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from consumer_financial_etl.extract import get_data
from consumer_financial_etl.load import load_data, csv_to_googlesheet
from consumer_financial_etl.transform import transform_data


with DAG(
    dag_id="consumer_financial_etl",
    default_args={
        "depends_on_past": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=20),
    },
    description="ETL pipeline for Consumer Financial Protection Bureau complaints",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-engineering", "etl", "consumer-complaints"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data_from_source",
        python_callable=get_data,
    )

    dump_mysql_task = PythonOperator(
        task_id="dump_data_to_mysql",
        python_callable=load_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    check_file_task = FileSensor(
        task_id="check_consumer_financial_csv",
        filepath="/opt/airflow/dags/complaints_transformed.csv",
        poke_interval=10,
        timeout=600,
        mode="poke",
        fs_conn_id="fs_default",
    )

    load_task = PythonOperator(
        task_id="dump_to_googlesheet",
        python_callable=csv_to_googlesheet,
    )

    send_email_task = EmailOperator(
        task_id="send_googlesheet_url_via_email",
        to="{{ var.value.completion_email }}",
        subject="Consumer Complaints ETL Completed",
        html_content="""
        <h3>Consumer Complaints ETL Completed</h3>
        <p>
            The transformed Consumer Complaints data has been successfully
            uploaded to Google Sheets.
        </p>
        <p>
            Google Sheet:
            {{ ti.xcom_pull(task_ids='dump_to_googlesheet') }}
        </p>
        """,
        conn_id="smtp_default",
    )

    (
        extract_task
        >> dump_mysql_task
        >> transform_task
        >> check_file_task
        >> load_task
        >> send_email_task
    )

