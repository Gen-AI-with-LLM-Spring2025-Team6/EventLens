""" DAG for Boston Calendar events pipeline """

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from data_load.boston_calendar.scrape_events import scrape_boston_calendar, WEBSITE_NAME
from data_load.boston_calendar.process_images import process_images
from data_load.boston_calendar.load_to_staging import load_to_staging
from data_load.boston_calendar.load_to_edw import load_to_edw
from data_load.connectors.s3_connection import create_s3_logging_connection

# Modular metric tracking
from data_load.helpers.metrics import start_task_metrics, end_task_metrics

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
    python_callable=scrape_boston_calendar,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    provide_context=True,
    dag=dag,
)

# Task 2: Process images and upload to S3
process_images_task = PythonOperator(
    task_id='process_images',
    python_callable=process_images,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    provide_context=True,
    dag=dag,
)

# Task 3: Load to Snowflake staging
load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    provide_context=True,
    dag=dag,
)

# Task 4: Load to Snowflake EDW
load_edw_task = PythonOperator(
    task_id='load_to_edw',
    python_callable=load_to_edw,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    provide_context=True,
    dag=dag,
)

# Task dependencies
scrape_task >> process_images_task >> load_staging_task >> load_edw_task