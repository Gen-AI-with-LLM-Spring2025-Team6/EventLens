from datetime import datetime
from data_load.connectors.db_connection import get_snowflake_connection

def start_task_metrics(context):
    task_instance = context['task_instance']
    task_id = context['task'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    run_identifier = f"{dag_id}_{execution_date.strftime('%Y%m%d%H%M%S')}"
    task_instance.xcom_push(key='run_identifier', value=run_identifier)

    start_time = datetime.now()
    task_instance.xcom_push(key=f"{task_id}_start_time", value=start_time.isoformat())

def end_task_metrics(context):
    task_instance = context['task_instance']
    task_id = context['task'].task_id

    run_identifier = task_instance.xcom_pull(key='run_identifier')
    end_time = datetime.now()

    start_time_str = task_instance.xcom_pull(key=f"{task_id}_start_time")
    start_time = datetime.fromisoformat(start_time_str)

    status = "succeeded" if context['ti'].state == "success" else "failed"
    dw_load_time = datetime.now()

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO EDW.EVENTLENS_METRICS
        (RUN_IDENTIFIER, TASK_NAME, START_TIME, END_TIME, STATUS, DW_LOAD_TIME)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_query,
            (run_identifier, task_id, start_time, end_time, status, dw_load_time)
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Metrics for task {task_id} inserted into Snowflake")

    except Exception as e:
        print(f"Error inserting metrics into Snowflake: {str(e)}")
