""" DAG for Boston Calendar events pipeline """
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator 
import snowflake.connector

# Import the task functions
from data_load.boston_calendar.scrape_events import scrape_boston_calendar, WEBSITE_NAME
from data_load.boston_calendar.process_images import process_images
from data_load.boston_calendar.load_to_staging import load_to_staging
from data_load.boston_calendar.load_to_edw import load_to_edw
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
)

# Function to get Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema='EDW', 
        role=SNOWFLAKE_ROLE
    )

# Metrics callback functions
def start_task_metrics(context):  # Accepting the context parameter
    task_instance = context['task_instance']
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    # Generate a run identifier based on DAG ID and execution date
    # This isn't the primary key but will help group related tasks together
    run_identifier = f"{dag_id}_{execution_date.strftime('%Y%m%d%H%M%S')}"
    task_instance.xcom_push(key='run_identifier', value=run_identifier)
    
    # Record task start metrics
    start_time = datetime.now()
    task_instance.xcom_push(key=f"{task_id}_start_time", value=start_time.isoformat())

def end_task_metrics(context):  # Accepting the context parameter
    task_instance = context['task_instance']
    task_id = context['task'].task_id
    
    # Get run identifier to group related tasks
    run_identifier = task_instance.xcom_pull(key='run_identifier')
    
    # Record task end metrics
    end_time = datetime.now()
    
    # Get start time
    start_time_str = task_instance.xcom_pull(key=f"{task_id}_start_time")
    start_time = datetime.fromisoformat(start_time_str)
    
    # Determine status
    status = "succeeded" if context['ti'].state == "success" else "failed"
    
    # Current time for DW_LOAD_TIME
    dw_load_time = datetime.now()
    
    # Insert metrics into Snowflake
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Insert query - let Snowflake generate the surrogate key
        insert_query = """
        INSERT INTO EDW.EVENTLENS_METRICS 
        (RUN_IDENTIFIER, TASK_NAME, START_TIME, END_TIME, STATUS, DW_LOAD_TIME)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # Execute insert
        cursor.execute(
            insert_query,
            (
                run_identifier,
                task_id,
                start_time,
                end_time,
                status,
                dw_load_time
            )
        )
        
        # Commit and close
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"Metrics for task {task_id} inserted into Snowflake")
        
    except Exception as e:
        print(f"Error inserting metrics into Snowflake: {str(e)}")

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
    f'{WEBSITE_NAME}_events_pipeline',
    default_args=default_args,
    description=f'Pipeline for scraping {WEBSITE_NAME} events',
    schedule_interval=None,
    catchup=False,
)

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

# Task 4: Process and load to Snowflake EDW
load_edw_task = PythonOperator(
    task_id='load_to_edw',
    python_callable=load_to_edw,
    on_execute_callback=start_task_metrics,
    on_success_callback=end_task_metrics,
    on_failure_callback=end_task_metrics,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
scrape_task >> process_images_task >> load_staging_task >> load_edw_task
