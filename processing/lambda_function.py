import os
import sys
import json
import logging
import boto3
import pandas as pd
import io
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """AWS Lambda handler for converting JSON files to Parquet format"""
    logger.info("Starting JSON to Parquet conversion")
    logger.info(f"Event: {json.dumps(event)}")

    try:
        # Get environment variables with defaults
        src_bucket = os.environ.get("DATA_BUCKET", "ncsh-app-data")
        src_prefix = os.environ.get("JSON_PREFIX", "data/json/")
        dst_bucket = os.environ.get("DATA_BUCKET", "ncsh-app-data")  # Same bucket by default
        dst_prefix = os.environ.get("PARQUET_PREFIX", "data/parquet/")

        # Extract parameters from event if provided
        if isinstance(event, dict):
            src_bucket = event.get('src_bucket', src_bucket)
            src_prefix = event.get('src_prefix', src_prefix)
            dst_bucket = event.get('dst_bucket', dst_bucket)
            dst_prefix = event.get('dst_prefix', dst_prefix)

        # Create version string based on current UTC time
        version = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        # Initialize S3 client
        s3 = boto3.client("s3")

        logger.info(f"Listing JSON files in s3://{src_bucket}/{src_prefix}")

        # List all JSON files in the source bucket/prefix
        response = s3.list_objects_v2(Bucket=src_bucket, Prefix=src_prefix)
        if 'Contents' not in response:
            logger.warning("No JSON files found")
            return {
                "statusCode": 404,
                "body": "No JSON files found in source location"
            }

        # Process each JSON file
        dataframes = []
        for obj in response['Contents']:
            key = obj['Key']
            if not key.endswith('.json'):
                continue

            logger.info(f"Processing {key}")
            try:
                # Read JSON file from S3
                obj_response = s3.get_object(Bucket=src_bucket, Key=key)
                data = obj_response['Body'].read()

                # Try reading as JSON Lines first
                try:
                    df = pd.read_json(io.BytesIO(data), lines=True)
                except ValueError:
                    # Fallback to standard JSON array
                    df = pd.read_json(io.BytesIO(data))

                dataframes.append(df)
                logger.info(f"Successfully processed {key}, shape: {df.shape}")

            except Exception as e:
                logger.error(f"Error processing {key}: {str(e)}")
                continue

        if not dataframes:
            logger.warning("No valid JSON files were processed")
            return {
                "statusCode": 404,
                "body": "No valid JSON files were processed"
            }

        # Combine all dataframes
        logger.info("Combining dataframes")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined shape: {combined_df.shape}")

        # Convert to Parquet
        logger.info("Converting to Parquet format")
        out_buffer = io.BytesIO()
        combined_df.to_parquet(out_buffer, index=False)
        out_buffer.seek(0)

        # Construct destination key with version
        dst_key = f"{dst_prefix}v{version}/data.parquet"

        # Upload Parquet file
        logger.info(f"Uploading Parquet file to s3://{dst_bucket}/{dst_key}")
        s3.put_object(
            Bucket=dst_bucket,
            Key=dst_key,
            Body=out_buffer.getvalue()
        )

        result = {
            "statusCode": 200,
            "body": {
                "message": "Conversion completed successfully",
                "source": f"s3://{src_bucket}/{src_prefix}",
                "destination": f"s3://{dst_bucket}/{dst_key}",
                "version": version,
                "rows_processed": len(combined_df)
            }
        }
        logger.info(f"Success: {json.dumps(result)}")
        return result

    except Exception as e:
        error_msg = f"Error in JSON to Parquet conversion: {str(e)}"
        logger.error(error_msg)
        return {
            "statusCode": 500,
            "body": error_msg
        }

if __name__ == '__main__':
    # Handle command line execution for testing
    if len(sys.argv) != 2:
        print("Usage: python lambda_parquet_runction.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = lambda_handler(event, None)
    print(json.dumps(response, indent=2))