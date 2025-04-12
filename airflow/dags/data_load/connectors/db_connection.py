import snowflake.connector
import logging
from data_load.parameters.parameter_config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
)

logger = logging.getLogger(__name__)

def get_snowflake_connection():
    try:
        connection = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema="EDW",
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {str(e)}")
        raise
