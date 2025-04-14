"""
Simplified configuration parameters for web scraping pipeline
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS settings
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_BUCKET = os.environ.get('S3_BUCKET', '')

# Snowflake settings
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', '')
SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER', '')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD', '')
SNOWFLAKE_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', '')
SNOWFLAKE_SCHEMA = os.environ.get('SNOWFLAKE_SCHEMA', '')
SNOWFLAKE_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', '')
SNOWFLAKE_ROLE = os.environ.get('SNOWFLAKE_ROLE', '')

# Snowflake Connections for Metrics
SNOWFLAKE_ACCOUNT_METRICS = os.environ.get('SNOWFLAKE_ACCOUNT_METRICS', '')
SNOWFLAKE_USER_METRICS = os.environ.get('SNOWFLAKE_USER_METRICS', '')
SNOWFLAKE_PASSWORD_METRICS = os.environ.get('SNOWFLAKE_PASSWORD_METRICS', '')
SNOWFLAKE_DATABASE_METRICS = os.environ.get('SNOWFLAKE_DATABASE_METRICS', '')
SNOWFLAKE_SCHEMA_METRICS = os.environ.get('SNOWFLAKE_SCHEMA_METRICS', '')
SNOWFLAKE_WAREHOUSE_METRICS = os.environ.get('SNOWFLAKE_WAREHOUSE_METRICS', '')
SNOWFLAKE_ROLE_METRICS = os.environ.get('SNOWFLAKE_ROLE_METRICS', '')

# OpenAI settings
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
APIFY_API_KEY =  os.environ.get('APIFY_API_KEY')
# File paths
TEMP_DIR = os.environ.get('TEMP_DIR', '/tmp')
os.makedirs(TEMP_DIR, exist_ok=True)