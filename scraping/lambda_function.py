import os
import sys
import json
import logging
from datetime import datetime
from ncsoccer.runner import run_scraper, run_month

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Suppress DEBUG messages from third-party libraries
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

def lambda_handler(event, context):
    """AWS Lambda handler for scraper invocation

    Args:
        event (dict): Event data
        context (LambdaContext): Lambda context

    Returns:
        dict: Response with status code and result
    """
    logger.info(f"Event: {json.dumps(event)}")

    # Special mode for running backfill from the state machine
    if event.get('backfill'):
        from backfill_runner import run_backfill

        try:
            # Extract parameters
            start_year = event.get('start_year')
            start_month = event.get('start_month')
            end_year = event.get('end_year')
            end_month = event.get('end_month')
            force_scrape = event.get('force_scrape', False)

            # Convert string values to integers if needed
            if start_year and isinstance(start_year, str):
                start_year = int(start_year)
            if start_month and isinstance(start_month, str):
                start_month = int(start_month)
            if end_year and isinstance(end_year, str):
                end_year = int(end_year)
            if end_month and isinstance(end_month, str):
                end_month = int(end_month)

            # Get bucket name from environment variables
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
            # Table name no longer needed as we're using file-based lookup

            # Calculate an appropriate timeout - leave 30 seconds for cleanup
            max_lambda_time = context.get_remaining_time_in_millis() if context else 900000
            timeout = (max_lambda_time / 1000) - 30  # Convert to seconds and leave margin

            result = run_backfill(
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                storage_type='s3',
                bucket_name=bucket_name,
                lookup_type='file',
                force_scrape=force_scrape,
                timeout=timeout
            )

            return {"statusCode": 200, "body": json.dumps({"result": result})}

        except Exception as e:
            logger.error(f"Error in backfill mode: {str(e)}", exc_info=True)
            return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    else:
        # Standard scraping mode
        try:
            # Extract parameters from event with defaults
            now = datetime.now()
            year = event.get('year', now.year)
            month = event.get('month', now.month)
            force_scrape = event.get('force_scrape', False)

            # Convert string values to integers if needed
            if isinstance(year, str):
                year = int(year)
            if isinstance(month, str):
                month = int(month)

            # Get bucket name from environment variables
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
            # Table name no longer needed as we're using file-based lookup

            # Common parameters for both day and month modes
            common_params = {
                'storage_type': 's3',
                'bucket_name': bucket_name,
                'html_prefix': 'data/html',
                'json_prefix': 'data/json',
                'lookup_type': 'file',
                'force_scrape': force_scrape
            }

            # Check if day is specified for day-level scraping
            if 'day' in event:
                day = event.get('day')
                if isinstance(day, str):
                    day = int(day)

                logger.info(f"Running day-level scraper for {year}-{month:02d}-{day:02d}")
                success = run_scraper(
                    year=year,
                    month=month,
                    day=day,
                    **common_params
                )
            else:
                # Month-level scraping
                logger.info(f"Running month-level scraper for {year}-{month:02d}")
                success = run_month(
                    year=year,
                    month=month,
                    **common_params
                )

            if success:
                return {"statusCode": 200, "body": json.dumps({"success": True})}
            else:
                return {"statusCode": 500, "body": json.dumps({"success": False})}

        except Exception as e:
            logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
            return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

if __name__ == "__main__":
    # For local testing
    result = lambda_handler({
        'year': 2023,
        'month': 2,
        'day': 1,
        'force_scrape': True
    }, None)
    print(json.dumps(result, indent=2))
