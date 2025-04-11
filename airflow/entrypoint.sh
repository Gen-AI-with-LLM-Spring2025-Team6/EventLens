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

# Start the Airflow webserver and scheduler
airflow webserver -p 8080 &
airflow scheduler &
tail -f /dev/null