import pandas as pd
import logging
from snowflake.connector.pandas_tools import write_pandas
from data_load.connectors.db_connection import get_snowflake_connection
from data_load.helpers.utils import (
    describe_image,
    describe_video_from_url,
    extract_event_fields_from_combined_text,
    parallelize_structuring_and_embedding,
    classify_event_into_group
)

# Constants
WEBSITE_NAME = "instagram_events"
EDW_SCHEMA = "EDW"
EDW_TABLE = "FACT_EVENTS_DETAILS"
STAGING_TABLE = "STAGING.BOSTON_INSTAGRAM_EVENTS_DETAILS"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def load_to_edw(**context):
    try:
        # Step 1: Connect to Snowflake
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Step 2: Read records from staging
        query = f"""
        SELECT 
            EVENT_TITLE, IMAGE_URL, VIDEO_URL, S3_IMAGE_URL, S3_VIDEO_URL,
            DESCRIPTION, EVENT_URL, SOURCE_ACCOUNT
        FROM {STAGING_TABLE}
        """
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        records = [dict(zip(columns, row)) for row in cursor.fetchall()]

        if not records:
            logger.info("No records found in staging table.")
            return

        enriched_records = []
        for record in records:
            # Step 3: Media description
            media_parts = []
            if record.get("S3_IMAGE_URL"):
                try:
                    media_parts.append("Image: " + describe_image(record["IMAGE_URL"]))
                except Exception as e:
                    logger.warning(f"Image description failed: {e}")
            if record.get("S3_VIDEO_URL"):
                try:
                    media_parts.append("Video: " + describe_video_from_url(record["VIDEO_URL"]))
                except Exception as e:
                    logger.warning(f"Video description failed: {e}")

            media_description = "\n".join(media_parts)
            original_desc = record.get("DESCRIPTION", "")
            combined_desc = f"{original_desc}\n\n--- Media Analysis ---\n{media_description}" if media_description else original_desc
            record["DESCRIPTION"] = combined_desc

            # Step 4: Structured fields from full description
            structured = extract_event_fields_from_combined_text(combined_desc, cursor)
            record.update(structured)

            # Step 5: Classify category
            record["CATEGORIES"] = ", ".join(classify_event_into_group(
                title=record.get("EVENT_TITLE", ""),
                description=record.get("DESCRIPTION", ""),
                location=record.get("LOCATION", ""),
                categories="",
                cursor=cursor
            ))

            # Step 6: Occurrences (static for now or add later)
            record["OCCURRENCES"] = "Not Available"

            enriched_records.append(record)

        # Step 7: Convert to DataFrame
        df = pd.DataFrame(enriched_records)

        def build_combined_text(row):
            keys = ["EVENT_TITLE", "START_DATE", "END_DATE", "LOCATION", "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION"]
            return " | ".join([
                f"{k.replace('_', ' ').title()}: {row[k]}" 
                for k in keys if row.get(k) and row[k] != "Not Available"
            ])

        df["COMBINED_TEXT"] = df.apply(build_combined_text, axis=1)

        # Step 8: Structure and embed
        df = parallelize_structuring_and_embedding(df, cursor)

        # Step 9: Add metadata
        df["SOURCE_WEBSITE"] = WEBSITE_NAME
        df["IMAGE_S3_URL"] = df.get("S3_IMAGE_URL").fillna(df.get("S3_VIDEO_URL"))
        df.columns = df.columns.str.upper()

        # Step 10: Final EDW schema
        edw_columns = [
            "EVENT_TITLE", "IMAGE_S3_URL", "START_DATE", "END_DATE", "START_TIME", "END_TIME",
            "OCCURRENCES", "LOCATION", "FULL_ADDRESS", "CATEGORIES", "ADMISSION", "DESCRIPTION",
            "EVENT_URL", "STRUCTURED_TEXT", "VECTOR_EMBEDDING", "SOURCE_WEBSITE"
        ]

        df_edw = df.reindex(columns=edw_columns, fill_value=None)

        logger.info(f"Inserting {len(df_edw)} records into {EDW_SCHEMA}.{EDW_TABLE}")
        write_pandas(conn, df_edw, EDW_TABLE)

        cursor.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error in load_to_edw: {str(e)}")
        raise
