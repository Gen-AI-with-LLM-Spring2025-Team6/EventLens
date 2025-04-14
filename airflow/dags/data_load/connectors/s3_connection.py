from airflow.models import Connection
from airflow.utils.db import provide_session
from data_load.parameters.parameter_config import AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_REGION
import json

@provide_session
def create_s3_logging_connection(session=None):
    conn_id = "MyS3Conn"
    if not session.query(Connection).filter_by(conn_id=conn_id).first():
        conn = Connection(
            conn_id=conn_id,
            conn_type='aws',
            extra=json.dumps({
                "aws_access_key_id": AWS_ACCESS_KEY_ID,
                "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                "region_name": AWS_REGION
            })
        )
        session.add(conn)
        session.commit()
        print("S3 connection created")
    else:
        print("S3 connection already exists")
