import os
import sys
import json
import logging
from datetime import datetime
from runner import run_scraper, run_month

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Extract parameters from event
        year = event.get('year', datetime.now().year)
        month = event.get('month', datetime.now().month)
        mode = event.get('mode', 'day')
        day = event.get('day', datetime.now().day) if mode == 'day' else None

        # Get bucket name from environment variable
        bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

        # Run scraper with S3 storage and DynamoDB lookup
        result = False
        if mode == 'day':
            result = run_scraper(
                year=year,
                month=month,
                day=day,
                storage_type='s3',
                bucket_name=bucket_name,
                html_prefix='data/html',  # Use production prefix
                json_prefix='data/json',  # Use production prefix
                lookup_type='dynamodb',  # Use DynamoDB lookup in Lambda
                region='us-east-2'
            )
        else:
            result = run_month(
                year=year,
                month=month,
                storage_type='s3',
                bucket_name=bucket_name,
                html_prefix='test_data/html',  # Use test_data prefix
                json_prefix='test_data/json',  # Use test_data prefix
                lookup_type='dynamodb',  # Use DynamoDB lookup in Lambda
                region='us-east-2'
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