from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add DAG folder to import helper scripts
sys.path.append(os.path.dirname(__file__))

from load_raw_to_bq import main as load_raw_to_bq_main
from transform_raw_to_clean import transform_raw_to_clean
from truncate_raw_table import truncate_raw_table
from archive_loaded_file import move_to_archive

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='all_vax_ingestion_dag',
    default_args=default_args,
    description='Ingest, transform, and archive ALL_VAX data from GCS to BigQuery',
    schedule_interval=None,  # manual or cron e.g. '0 8 * * 1'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['all_vax', 'gcp', 'bq'],
) as dag:

    load_raw_task = PythonOperator(
        task_id='load_raw_csv_to_bigquery',
        python_callable=load_raw_to_bq_main
    )

    transform_task = PythonOperator(
        task_id='transform_raw_to_clean',
        python_callable=transform_raw_to_clean
    )

    truncate_task = PythonOperator(
        task_id='truncate_raw_table',
        python_callable=truncate_raw_table
    )

    archive_task = PythonOperator(
        task_id='archive_loaded_file',
        python_callable=move_to_archive
    )

    # Task dependencies
    load_raw_task >> transform_task >> truncate_task >> archive_task
