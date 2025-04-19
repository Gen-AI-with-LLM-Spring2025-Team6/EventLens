#!/bin/bash

# Initialize the Airflow database
airflow db init

# Create an admin user if it doesn't exist
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

export AIRFLOW__LOGGING__REMOTE_LOGGING=True
export AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=MyS3Conn
export AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://event-lens/airflow-logs


# Start the Airflow webserver and scheduler
airflow webserver -p 8080 &
airflow scheduler &
tail -f /dev/null