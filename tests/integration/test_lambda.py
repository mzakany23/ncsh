import sys
# Remove any existing reactor
if 'twisted.internet.reactor' in sys.modules:
    del sys.modules['twisted.internet.reactor']

import os
import json
import logging
import pytest
import boto3
from botocore.exceptions import ClientError

import asyncio
from twisted.internet import asyncioreactor
asyncioreactor.install(asyncio.new_event_loop())

from lambda_function import handler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
# Suppress DEBUG messages from third-party libraries
logging.getLogger("twisted").setLevel(logging.WARNING)
logging.getLogger("scrapy").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def s3_test_bucket():
    """Create a test bucket for S3 integration tests"""
    bucket_name = "ncsoccer-test-" + os.environ.get('USER', 'default').lower()
    region = os.environ.get('AWS_REGION', 'us-east-2')

    # Create the test bucket
    s3 = boto3.client('s3', region_name=region)
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': region}
        )
    except ClientError as e:
        if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
            raise

    yield bucket_name

    # Cleanup - delete all objects and the bucket
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                s3.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects}
                )
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchBucket':
            raise

def verify_s3_files(bucket_name, date_str):
    """Verify that expected files were created in S3"""
    s3 = boto3.client('s3')
    expected_files = [
        f"data/html/{date_str}.html",
        f"data/json/{date_str}.json",
        f"data/json/{date_str}_meta.json",
        "data/lookup.json"
    ]

    for file_path in expected_files:
        try:
            response = s3.head_object(Bucket=bucket_name, Key=file_path)
            assert response['ContentLength'] > 0, f"S3 file {file_path} is empty"
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                assert False, f"Expected S3 file {file_path} was not created"
            raise

    # Verify the content of the JSON file
    json_response = s3.get_object(Bucket=bucket_name, Key=f"data/json/{date_str}.json")
    data = json.loads(json_response['Body'].read().decode('utf-8'))

    # Verify JSON structure
    assert 'date' in data
    assert 'games_found' in data
    assert 'games' in data
    assert data['date'] == date_str

def test_handler(s3_test_bucket):
    """Test the lambda handler function"""
    # Set environment variables
    os.environ['DATA_BUCKET'] = s3_test_bucket

    # Test event
    event = {
        "year": 2024,
        "month": 3,
        "day": 1,
        "mode": "day"
    }

    # Log test execution
    logger.info("Starting lambda handler test")
    logger.info(f"Test event: {json.dumps(event, indent=2)}")

    # Call handler
    response = handler(event, None)
    logger.info(f"Response: {json.dumps(response, indent=2)}")

    # Verify response structure
    assert response['statusCode'] == 200
    assert 'body' in response

    # Parse response body
    body = json.loads(response['body'])
    assert 'message' in body
    assert 'result' in body
    assert body['message'] == 'Scraping completed successfully'
    assert body['result'] is True

    # Verify files were created in S3
    date_str = f"{event['year']}-{event['month']:02d}-{event['day']:02d}"
    verify_s3_files(s3_test_bucket, date_str)
