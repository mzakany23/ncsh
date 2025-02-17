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
    try:
        # Extract parameters from event
        year = event.get('year', datetime.now().year)
        month = event.get('month', datetime.now().month)
        mode = event.get('mode', 'day')
        day = event.get('day', datetime.now().day) if mode == 'day' else None
        force_scrape = event.get('force_scrape', False)

        # Get bucket name from environment variable
        bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        table_name = os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates')

        # Run scraper with S3 storage and DynamoDB lookup
        result = False
        if mode == 'day':
            result = run_scraper(
                year=year,
                month=month,
                day=day,
                storage_type='s3',
                bucket_name=bucket_name,
                html_prefix='data/html',
                json_prefix='data/json',
                lookup_type='dynamodb',
                table_name=table_name,
                region='us-east-2',
                force_scrape=force_scrape
            )
        else:
            result = run_month(
                year=year,
                month=month,
                storage_type='s3',
                bucket_name=bucket_name,
                html_prefix='data/html',
                json_prefix='data/json',
                lookup_type='dynamodb',
                table_name=table_name,
                region='us-east-2',
                force_scrape=force_scrape
            )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Scraping completed successfully',
                'result': result
            })
        }

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

if __name__ == '__main__':
    # Handle command line execution
    if len(sys.argv) != 2:
        print("Usage: python lambda_function.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = lambda_handler(event, None)
    print(json.dumps(response))