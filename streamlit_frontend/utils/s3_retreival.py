import os
from dotenv import load_dotenv

load_dotenv()

def convert_s3_to_https(s3_path: str) -> str:
    """
    Converts an S3 URI to a public HTTPS URL using env variables for bucket and region.

    Args:
        s3_path (str): The S3 URI (e.g., s3://event-lens/path/to/file.jpg or event-lens/path/to/file.jpg)

    Returns:
        str: A public HTTPS URL if the object is accessible.
    """
    bucket_name = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_REGION")

    # Remove scheme and bucket name if present
    s3_path = s3_path.replace("s3://", "").replace(f"{bucket_name}/", "")

    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_path}"
