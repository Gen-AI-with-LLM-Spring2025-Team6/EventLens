"""
DAG for Instagram events pipeline
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the task functions
from data_load.boston_instagram_events.scrape_events import scrape_instagram_events, WEBSITE_NAME
from data_load.boston_instagram_events.process_media import process_media
from data_load.boston_instagram_events.load_to_staging import load_to_staging
from data_load.boston_instagram_events.load_to_edw import load_to_edw

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    f'{WEBSITE_NAME}_pipeline',
    default_args=default_args,
    description=f'Pipeline for scraping {WEBSITE_NAME}',
    schedule_interval=None,  # No automatic scheduling, only manual triggers
    catchup=False,
)

# Task 1: Scrape events from Instagram
scrape_task = PythonOperator(
    task_id=f'scrape_{WEBSITE_NAME}',
    python_callable=scrape_instagram_events,
    dag=dag,
)

# Task 2: Process media (images and videos) and upload to S3
process_media_task = PythonOperator(
    task_id='process_media',
    python_callable=process_media,
    dag=dag,
)

# Task 3: Load to Snowflake staging
load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    dag=dag,
)

# Task 4: Process and load to Snowflake EDW
load_edw_task = PythonOperator(
    task_id='load_to_edw',
    python_callable=load_to_edw,
    dag=dag,
)

# Define task dependencies
scrape_task >> process_media_task >> load_staging_task >> load_edw_task