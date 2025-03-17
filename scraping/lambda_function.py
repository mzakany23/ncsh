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
    """AWS Lambda handler function
    
    This handler serves as the unified entrypoint for both standard scraping and backfill operations.
    The mode is determined by the BACKFILL_MODE environment variable.
    """
    logger.info("Entered lambda_handler")
    
    # Determine if we're running in backfill mode
    backfill_mode = os.environ.get('BACKFILL_MODE', 'false').lower() == 'true'
    logger.info(f"Running in {'backfill' if backfill_mode else 'standard'} mode")
    
    if backfill_mode:
        # Import backfill runner functionality locally to avoid circular imports
        try:
            from backfill_runner import run_backfill
            
            # Extract parameters from event with defaults
            start_year = event.get('start_year', 2007)
            start_month = event.get('start_month', 1)
            end_year = event.get('end_year')
            end_month = event.get('end_month')
            force_scrape = event.get('force_scrape', False)
            
            # Get bucket name and table name from environment variables
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
            table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')
            
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
                lookup_type='dynamodb',
                table_name=table_name,
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

            # Get bucket name and table name from environment variables
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
            table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')

            # Common parameters for both day and month modes
            common_params = {
                'storage_type': 's3',
                'bucket_name': bucket_name,
                'html_prefix': 'data/html',
                'json_prefix': 'data/json',
                'lookup_type': 'dynamodb',
                'table_name': table_name,
                'force_scrape': force_scrape
            }

            # If day is provided, run in day mode, otherwise run in month mode
            if 'day' in event:
                logger.info("Running in day mode")
                result = run_scraper(
                    year=year,
                    month=month,
                    day=event['day'],
                    **common_params
                )
            else:
                logger.info("Running in month mode")
                result = run_month(
                    year=year,
                    month=month,
                    **common_params
                )

            logger.info("Operation completed with result: %s", result)
            return {"statusCode": 200, "body": json.dumps({"result": result})}
        except Exception as e:
            logger.error(f"Error in standard mode: {str(e)}", exc_info=True)
            return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

if __name__ == '__main__':
    # Handle command line execution
    if len(sys.argv) != 2:
        print("Usage: python lambda_function.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = lambda_handler(event, None)
    print(json.dumps(response))
