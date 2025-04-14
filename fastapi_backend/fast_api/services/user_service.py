import os
import snowflake.connector
from fastapi import HTTPException, status
import pandas as pd
from typing import Optional, Dict, Any

from fastapi_backend.fast_api.config.db_connection import snowflake_connection, close_connection

def fetch_user_by_email(email: str) -> Optional[pd.DataFrame]:
    """
    Fetch user from Snowflake using their email.
    """
    conn = None
    cursor = None

    try:
        if not isinstance(email, str) or not email.strip():
            raise ValueError("Invalid email provided")

        conn = snowflake_connection()
        cursor = conn.cursor()

        select_query = """
        SELECT *
        FROM USERDETAILSGITHUB
        WHERE useremail = %s
        """

        cursor.execute(select_query, (email,))
        columns = [col[0] for col in cursor.description]
        user = cursor.fetchone()

        if user:
            return pd.DataFrame([user], columns=columns)
        return None

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)


def fetch_user(username: str) -> Optional[pd.DataFrame]:
    """
    Fetch user from Snowflake using their username.
    """
    conn = None
    cursor = None

    try:
        if not isinstance(username, str) or not username.strip():
            raise ValueError("Invalid username provided")

        conn = snowflake_connection()
        cursor = conn.cursor()

        select_query = """
        SELECT *
        FROM USERDETAILSGITHUB
        WHERE username = %s
        """

        cursor.execute(select_query, (username,))
        columns = [col[0] for col in cursor.description]
        user = cursor.fetchone()

        if user:
            return pd.DataFrame([user], columns=columns)
        return None

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)


def insert_user(username: str, email: str, password: str, interests: List[str]) -> Dict[str, Any]:
    """
    Insert new user record into Snowflake.
    """
    conn = None
    cursor = None

    try:
        if not all(isinstance(x, str) and x.strip() for x in [username, email, password]):
            raise ValueError("All inputs must be non-empty strings")

        conn = snowflake_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO USERDETAILSGITHUB (username, useremail, userpassword,interests)
        VALUES (%s, %s, %s, %s)
        """

        interests_str = ", ".join(interests) 
        cursor.execute(insert_query, (username, email, password,interests_str))
        conn.commit()

        return {
            "status": "success",
            "message": "User registered successfully",
            "username": username,
            "email": email
        }

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except snowflake.connector.errors.ProgrammingError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error: {str(e)}")
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            close_connection(conn)
