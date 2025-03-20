import os
import sys
import json
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
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

    # Get operation mode from the event
    mode = event.get('mode', 'day')
    parameters = event.get('parameters', {})

    # Get bucket name from environment variables
    bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

    # Common parameters for all modes
    common_params = {
        'storage_type': 's3',
        'bucket_name': bucket_name,
        'html_prefix': 'data/html',
        'json_prefix': 'data/json',
        'lookup_type': 'file',
        'force_scrape': parameters.get('force_scrape', False)
    }

    try:
        # Handle based on mode
        if mode == "day":
            # Daily mode - scrape a single day
            now = datetime.now()
            year = parameters.get('year', now.year)
            month = parameters.get('month', now.month)
            day = parameters.get('day', now.day)

            # Convert string values to integers if needed
            if isinstance(year, str):
                year = int(year)
            if isinstance(month, str):
                month = int(month)
            if isinstance(day, str):
                day = int(day)

            logger.info(f"Running day-level scraper for {year}-{month:02d}-{day:02d}")
            success = run_scraper(
                year=year,
                month=month,
                day=day,
                **common_params
            )

            return {
                "statusCode": 200 if success else 500,
                "body": json.dumps({"success": success})
            }

        elif mode == "month":
            # Monthly mode - scrape an entire month
            now = datetime.now()
            year = parameters.get('year', now.year)
            month = parameters.get('month', now.month)

            # Convert string values to integers if needed
            if isinstance(year, str):
                year = int(year)
            if isinstance(month, str):
                month = int(month)

            logger.info(f"Running month-level scraper for {year}-{month:02d}")
            success = run_month(
                year=year,
                month=month,
                **common_params
            )

            return {
                "statusCode": 200 if success else 500,
                "body": json.dumps({"success": success})
            }

        elif mode == "date_range":
            # Date range mode - iterate through months in the range
            start_date_str = parameters.get('start_date')
            end_date_str = parameters.get('end_date')

            # Parse date strings
            if not start_date_str or not end_date_str:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "start_date and end_date are required for date_range mode"})
                }

            try:
                # Parse dates with format YYYY-MM-DD
                start_parts = start_date_str.split('-')
                end_parts = end_date_str.split('-')

                start_date = date(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))
                end_date = date(int(end_parts[0]), int(end_parts[1]), int(end_parts[2]))

                # Calculate max execution time (leave 30 seconds for cleanup)
                max_lambda_time = context.get_remaining_time_in_millis() if context else 900000
                timeout_time = datetime.now().timestamp() + (max_lambda_time / 1000) - 30

                # Track our progress
                all_success = True
                current_date = start_date.replace(day=1)  # Start at first day of month
                processed_months = []

                # Process each month in the range
                while current_date <= end_date and datetime.now().timestamp() < timeout_time:
                    logger.info(f"Processing month: {current_date.year}-{current_date.month:02d}")

                    success = run_month(
                        year=current_date.year,
                        month=current_date.month,
                        **common_params
                    )

                    if not success:
                        all_success = False
                        logger.warning(f"Failed to process month: {current_date.year}-{current_date.month:02d}")

                    processed_months.append(f"{current_date.year}-{current_date.month:02d}")

                    # Move to next month
                    current_date = current_date + relativedelta(months=1)

                # Check if we've processed all months or if we hit the timeout
                complete = current_date > end_date

                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "success": all_success,
                        "complete": complete,
                        "processed_months": processed_months,
                        "remaining": None if complete else f"{current_date.year}-{current_date.month:02d}"
                    })
                }

            except Exception as e:
                logger.error(f"Error parsing dates: {str(e)}", exc_info=True)
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": f"Invalid date format: {str(e)}"})
                }
        else:
            # Unknown mode
            return {
                "statusCode": 400,
                "body": json.dumps({"error": f"Unknown mode: {mode}"})
            }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

if __name__ == "__main__":
    # For local testing
    result = lambda_handler({
        'mode': 'day',
        'parameters': {
            'year': 2023,
            'month': 2,
            'day': 1,
            'force_scrape': True
        }
    }, None)
    print(json.dumps(result, indent=2))
