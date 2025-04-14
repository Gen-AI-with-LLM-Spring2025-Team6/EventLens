import os
import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError
from typing import Optional

# Get Snowflake credentials from environment variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

def validate_snowflake_credentials() -> tuple[bool, str]:
    """
    Validate that all required Snowflake credentials are present.
    Returns: tuple(is_valid: bool, error_message: str)
    """
    required_credentials = {
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
        "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
        "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
        "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE
    }

    missing = [key for key, val in required_credentials.items() if not val]
    if missing:
        return False, f"Missing required Snowflake credentials: {', '.join(missing)}"
    return True, ""

def snowflake_connection() -> Optional[snowflake.connector.SnowflakeConnection]:
    """
    Establishes a connection to Snowflake with error handling.
    Returns: SnowflakeConnection object if successful, or raises descriptive errors.
    """
    try:
        is_valid, error_message = validate_snowflake_credentials()
        if not is_valid:
            raise ValueError(error_message)

        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE,
            client_session_keep_alive=True,
            network_timeout=30
        )

        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        cursor.close()

        return conn

    except ValueError as ve:
        raise ValueError(f"Configuration error: {str(ve)}")

    except SnowflakeError as se:
        error_code = getattr(se, 'errno', 'Unknown')
        if '250001' in str(se):
            raise ConnectionError(f"Invalid Snowflake account: {SNOWFLAKE_ACCOUNT}")
        elif '251001' in str(se):
            raise ConnectionError("Invalid credentials. Please check username and password.")
        elif '250006' in str(se):
            raise ConnectionError(f"Invalid database/warehouse/schema/role. Error code: {error_code}")
        else:
            raise ConnectionError(f"Snowflake connection error: {str(se)}. Code: {error_code}")

    except Exception as e:
        raise Exception(f"Unexpected error while connecting to Snowflake: {str(e)}")

    finally:
        if 'cursor' in locals():
            cursor.close()

def close_connection(conn: Optional[snowflake.connector.SnowflakeConnection]) -> None:
    """
    Safely close the Snowflake connection.
    """
    if conn:
        try:
            conn.close()
        except Exception as e:
            raise Exception(f"Error closing Snowflake connection: {str(e)}")
