"""
DAG for Boston Gov events pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Task functions
from data_load.boston_gov.scrape_events import scrape_boston_gov, WEBSITE_NAME
from data_load.boston_gov.process_images import process_images
from data_load.boston_gov.load_to_staging import load_to_staging
from data_load.boston_gov.load_to_edw import load_to_edw
from data_load.connectors.s3_connection import create_s3_logging_connection

# Metrics
from data_load.helpers.metrics import start_task_metrics, end_task_metrics

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    f'{WEBSITE_NAME}_events_pipeline',
    default_args=default_args,
    description=f'Pipeline for scraping {WEBSITE_NAME} events',
    schedule_interval=None,
    catchup=False,
)

create_s3_logging_connection()

# Task 1: Scrape events
scrape_task = PythonOperator(
    task_id=f'scrape_{WEBSITE_NAME}',
    python_callable=scrape_boston_gov,
    provide_context=True,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    dag=dag,
)

# Task 2: Process images and upload to S3
process_images_task = PythonOperator(
    task_id='process_images',
    python_callable=process_images,
    provide_context=True,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    dag=dag,
)

# Task 3: Load to Snowflake staging
load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    provide_context=True,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    dag=dag,
)

# Task 4: Load to Snowflake EDW
load_edw_task = PythonOperator(
    task_id='load_to_edw',
    python_callable=load_to_edw,
    provide_context=True,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    dag=dag,
)

# Set task dependencies
scrape_task >> process_images_task >> load_staging_task >> load_edw_task
