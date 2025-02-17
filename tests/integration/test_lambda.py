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

from lambda_function import lambda_handler

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

# Constants
AWS_REGION = 'us-east-2'

@pytest.fixture(scope="module")
def dynamodb_test_table():
    """Create a test DynamoDB table for integration tests"""
    table_name = "ncsh-scraped-dates-test"
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

    # Create the test table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'date', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'date', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )
    table.wait_until_exists()

    yield table_name

    # Cleanup - delete the table
    table.delete()
    table.wait_until_not_exists()

@pytest.fixture(scope="module")
def s3_test_bucket():
    """Set up test bucket prefix for S3 integration tests"""
    bucket_name = 'ncsh-app-data'
    test_prefix = 'test_data'
    s3 = boto3.client('s3', region_name=AWS_REGION)

    yield bucket_name

    # Clean up test objects after tests
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=test_prefix)
        if 'Contents' in objects:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    except ClientError:
        pass

def verify_s3_files(bucket_name, date_str):
    """Verify that expected files were created in S3"""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    expected_files = [
        f"test_data/html/{date_str}.html",
        f"test_data/json/{date_str}.json",
        f"test_data/json/{date_str}_meta.json"
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
    json_response = s3.get_object(Bucket=bucket_name, Key=f"test_data/json/{date_str}.json")
    data = json.loads(json_response['Body'].read().decode('utf-8'))

    # Verify JSON structure
    assert 'date' in data
    assert 'games_found' in data
    assert 'games' in data
    assert data['date'] == date_str

def verify_dynamodb_entry(table_name, date_str):
    """Verify that the DynamoDB entry was created correctly"""
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'date': date_str})

    assert 'Item' in response, f"Date {date_str} not found in DynamoDB"
    item = response['Item']
    assert item['success'], "Spider run was not marked as successful in DynamoDB"
    assert item['games_count'] > 0, "No games were recorded in DynamoDB"
    assert 'timestamp' in item, "Timestamp missing in DynamoDB entry"

def test_handler(s3_test_bucket, dynamodb_test_table):
    """Test the lambda handler function"""
    # Set environment variables
    os.environ['DATA_BUCKET'] = s3_test_bucket
    os.environ['DYNAMODB_TABLE'] = dynamodb_test_table
    os.environ['AWS_DEFAULT_REGION'] = AWS_REGION  # Set both region env vars
    os.environ['AWS_REGION'] = AWS_REGION

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
    response = lambda_handler(event, None)
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

    # Verify DynamoDB entry
    verify_dynamodb_entry(dynamodb_test_table, date_str)
