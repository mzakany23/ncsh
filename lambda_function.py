import json
import logging
from datetime import datetime
from runner import run_scraper

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

        # Run the scraper
        result = run_scraper(
            mode=mode,
            year=year,
            month=month,
            day=day if mode == 'day' else None
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