import os
import sys
import json
import logging
from datetime import datetime
from runner import run_scraper, run_month

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# Suppress DEBUG messages from third-party libraries
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    logger.info("Entered lambda_handler")
    # Extract parameters from event
    year = event.get('year', datetime.now().year)
    month = event.get('month', datetime.now().month)
    mode = event.get('mode', 'day')
    day = event.get('day', datetime.now().day) if mode == 'day' else None
    force_scrape = event.get('force_scrape', False)
    test_mode = event.get('test_mode', False)

    logger.info("Parameters: year=%s, month=%s, day=%s, mode=%s, force_scrape=%s, test_mode=%s", year, month, day, mode, force_scrape, test_mode)

    # Get bucket name and table name from environment variables
    bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
    table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')

    # Determine storage prefix: in test_mode, use environment variable TEST_DATA_DIR if set, else default to 'test_data'; otherwise, use 'data'
    if test_mode:
        prefix = os.environ.get('TEST_DATA_DIR', 'test_data')
    else:
        prefix = 'data'
    logger.info("Using storage prefix: %s", prefix)

    if mode == 'day':
        logger.info("About to call run_scraper")
        result = run_scraper(
            year=year,
            month=month,
            day=day,
            storage_type='s3',
            bucket_name=bucket_name,
            html_prefix=f'{prefix}/html',
            json_prefix=f'{prefix}/json',
            lookup_file='data/lookup.json',
            lookup_type='dynamodb',
            table_name=table_name,
            force_scrape=force_scrape,
            use_test_data=test_mode
        )
        logger.info("Scraper returned: %s", result)
    else:
        logger.info("Non-day mode not implemented")
        result = None
    return {"statusCode": 200, "body": json.dumps({"result": result})}

if __name__ == '__main__':
    # Handle command line execution
    if len(sys.argv) != 2:
        print("Usage: python lambda_function.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = lambda_handler(event, None)
    print(json.dumps(response))
