import os
import sys

import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    AWS Lambda handler for NC Soccer schedule scraper.

    Expected event format:
    {
        "year": 2024,
        "month": 2,
        "day": null,  # Optional, if not provided will scrape entire month
        "mode": "month"  # or "day" if day is provided
    }
    """

    try:
        logger.info(f"Received event: {json.dumps(event)}")

        # Import runner functions here to delay Twisted imports
        from runner import run_scraper, run_month

        # Validate environment variables
        if 'DATA_BUCKET' not in os.environ:
            raise ValueError("DATA_BUCKET environment variable is required")

        # Extract parameters from event
        year = event.get('year')
        month = event.get('month')
        day = event.get('day')
        mode = event.get('mode', 'month')

        if not year or not month:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters: year and month are required'
                })
            }

        # Run the appropriate scraper function based on mode
        if mode == 'day':
            if not day:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'Day parameter is required for day mode'
                    })
                }
            result = run_scraper(
                year=year,
                month=month,
                day=day,
                storage_type='s3',
                bucket_name=os.environ['DATA_BUCKET']
            )
        else:  # month mode
            result = run_month(
                year=year,
                month=month,
                storage_type='s3',
                bucket_name=os.environ['DATA_BUCKET']
            )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Scraping completed successfully',
                'result': result
            })
        }

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Error processing request: {str(e)}'
            })
        }

if __name__ == '__main__':
    # Handle command line execution
    if len(sys.argv) != 2:
        print("Usage: python lambda_function.py '<event_json>'")
        sys.exit(1)

    event = json.loads(sys.argv[1])
    response = handler(event, None)
    print(json.dumps(response))