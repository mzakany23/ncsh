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
from datetime import datetime
from scrapy.http import HtmlResponse, Request
from ncsoccer.spiders.schedule_spider import ScheduleSpider

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
AWS_REGION = 'us-east-2'  # Match our infrastructure region
TEST_DATA_DIR = 'tests/data'

@pytest.fixture(scope="module")
def dynamodb_test_table():
    """Get the test DynamoDB table and clear its contents"""
    table_name = "ncsh-scraped-dates-test"
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(table_name)

    # Clear any existing items
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'date': item['date']})

    yield table_name

    # No cleanup needed - we keep the table around

@pytest.fixture
def s3_test_bucket():
    """Return the production bucket name - we'll use test_data/ prefix for tests"""
    bucket_name = 'ncsh-app-data'
    s3 = boto3.client('s3', region_name=AWS_REGION)

    # Clean up any existing test data
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='test_data/')
        if 'Contents' in objects:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    except Exception as e:
        print(f"Warning: Failed to clean test data: {e}")

    yield bucket_name

    # Clean up test data after tests
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix='test_data/')
        if 'Contents' in objects:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    except Exception as e:
        print(f"Warning: Failed to clean test data: {e}")

@pytest.fixture
def spider():
    return ScheduleSpider(storage_type='file', html_prefix=TEST_DATA_DIR + '/html', json_prefix=TEST_DATA_DIR + '/json')

def load_test_html(date_str):
    """Load test HTML data from file"""
    html_path = os.path.join(TEST_DATA_DIR, 'html', f"{date_str}.html")
    with open(html_path, 'r', encoding='utf-8') as f:
        return f.read()

def create_fake_response(html_content, url):
    """Create a fake Scrapy response for testing"""
    request = Request(url=url)
    return HtmlResponse(
        url=url,
        request=request,
        body=html_content.encode('utf-8'),
        encoding='utf-8'
    )

def verify_game_object(game):
    """Verify that a game object has the correct schema"""
    assert isinstance(game, dict)
    assert 'date' in game
    assert 'time' in game
    assert 'field' in game
    assert 'home_team' in game
    assert 'away_team' in game
    assert 'division' in game

def verify_metadata_object(metadata):
    """Verify that a metadata object has the correct schema"""
    assert isinstance(metadata, dict)
    assert 'date' in metadata
    assert 'games_count' in metadata
    assert isinstance(metadata['games_count'], int)

def verify_s3_partitioned_data(s3_client, bucket, date_str):
    """Verify that data is correctly partitioned in S3"""
    year, month, day = date_str.split('-')

    # Verify raw HTML exists
    html_key = f"raw/html/{year}/{month}/{day}.html"
    s3_client.head_object(Bucket=bucket, Key=html_key)

    # Verify metadata exists
    metadata_key = f"raw/metadata/{year}/{month}/{day}.json"
    s3_client.head_object(Bucket=bucket, Key=metadata_key)

def test_parse_schedule_with_games(spider, s3_test_bucket):
    """Test parsing a schedule page that has games"""
    date_str = "2024-03-01"
    html_content = load_test_html(date_str)
    url = f"https://nc-soccer-hudson.ezleagues.ezfacility.com/schedule.aspx?facility_id=690&date={date_str}"
    response = create_fake_response(html_content, url)
    response.meta['date'] = date_str

    # Create necessary directories
    year, month, day = date_str.split('-')
    metadata_dir = os.path.join(TEST_DATA_DIR, 'json', 'raw', 'metadata', year, month)
    os.makedirs(metadata_dir, exist_ok=True)

    # Process the response
    spider.parse_schedule(response)

    # Verify files were created
    html_path = os.path.join(TEST_DATA_DIR, 'html', f"{date_str}.html")
    metadata_path = os.path.join(TEST_DATA_DIR, 'json', 'raw', 'metadata', year, month, f"{day}.json")

    assert os.path.exists(html_path)
    assert os.path.exists(metadata_path)

    # Verify metadata content
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
        verify_metadata_object(metadata)
        assert metadata['games_count'] > 0

def test_parse_schedule_with_games_march_3(spider, s3_test_bucket):
    """Test parsing a schedule page that has games on March 3rd"""
    date_str = "2024-03-03"  # Using March 3rd instead of March 2nd
    html_content = load_test_html(date_str)
    url = f"https://nc-soccer-hudson.ezleagues.ezfacility.com/schedule.aspx?facility_id=690&date={date_str}"
    response = create_fake_response(html_content, url)
    response.meta['date'] = date_str

    # Create necessary directories
    year, month, day = date_str.split('-')
    metadata_dir = os.path.join(TEST_DATA_DIR, 'json', 'raw', 'metadata', year, month)
    os.makedirs(metadata_dir, exist_ok=True)

    # Process the response
    spider.parse_schedule(response)

    # Verify files were created
    html_path = os.path.join(TEST_DATA_DIR, 'html', f"{date_str}.html")
    metadata_path = os.path.join(TEST_DATA_DIR, 'json', 'raw', 'metadata', year, month, f"{day}.json")

    assert os.path.exists(html_path)
    assert os.path.exists(metadata_path)

    # Verify metadata content
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
        verify_metadata_object(metadata)
        assert metadata['games_count'] == 43

def verify_raw_files(bucket_name, date_str):
    """Verify that raw HTML and metadata files were created in S3"""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    expected_files = [
        f"test_data/html/{date_str}.html",
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

def verify_game_schema(game):
    """Verify that a game object follows the expected schema"""
    required_fields = {
        'league': str,
        'session': str,
        'home_team': str,
        'away_team': str,
        'home_score': (int, type(None)),
        'away_score': (int, type(None)),
        'status': str,
        'venue': str,
        'time': (str, type(None)),
        'officials': str
    }

    for field, expected_type in required_fields.items():
        assert field in game, f"Missing required field: {field}"
        if isinstance(expected_type, tuple):
            assert isinstance(game[field], expected_type), f"Field {field} has wrong type. Expected {expected_type}, got {type(game[field])}"
        else:
            assert isinstance(game[field], expected_type), f"Field {field} has wrong type. Expected {expected_type}, got {type(game[field])}"

def verify_metadata_schema(metadata):
    """Verify that metadata follows the expected schema"""
    required_fields = {
        'date': str,
        'games_found': bool,
        'error': (str, type(None))  # Optional field
    }

    for field, expected_type in required_fields.items():
        if field != 'error':  # Error is optional
            assert field in metadata, f"Missing required field: {field}"
        if field in metadata:
            if isinstance(expected_type, tuple):
                assert isinstance(metadata[field], expected_type), f"Field {field} has wrong type. Expected {expected_type}, got {type(metadata[field])}"
            else:
                assert isinstance(metadata[field], expected_type), f"Field {field} has wrong type. Expected {expected_type}, got {type(metadata[field])}"

def verify_partitioned_data(bucket_name, date_str):
    """Verify that data is correctly partitioned in S3"""
    s3 = boto3.client('s3', region_name=AWS_REGION)
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    year = dt.year
    month = dt.month

    # Check games partition
    games_path = f"test_data/games/year={year}/month={month:02d}/data.jsonl"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=games_path)
        games_data = [json.loads(line) for line in response['Body'].read().decode('utf-8').splitlines()]
        assert len(games_data) > 0, "No games found in partition"
        for game in games_data:
            verify_game_schema(game)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            assert False, f"Games partition not found: {games_path}"
        raise

    # Check metadata partition
    metadata_path = f"test_data/metadata/year={year}/month={month:02d}/data.jsonl"
    try:
        response = s3.get_object(Bucket=bucket_name, Key=metadata_path)
        metadata_data = [json.loads(line) for line in response['Body'].read().decode('utf-8').splitlines()]
        assert len(metadata_data) > 0, "No metadata found in partition"
        for metadata in metadata_data:
            verify_metadata_schema(metadata)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            assert False, f"Metadata partition not found: {metadata_path}"
        raise

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

def test_handler(dynamodb_test_table, s3_test_bucket):
    """Test the lambda handler function"""
    logger.info("Starting lambda handler test")

    # Set up environment variables
    os.environ['DATA_BUCKET'] = s3_test_bucket
    os.environ['DYNAMODB_TABLE'] = dynamodb_test_table

    # Create necessary directories
    date_str = "2024-03-01"
    year, month, day = date_str.split('-')
    metadata_dir = os.path.join(TEST_DATA_DIR, 'json', 'raw', 'metadata', year, month)
    os.makedirs(metadata_dir, exist_ok=True)

    # Test event
    event = {
        "year": int(year),
        "month": int(month),
        "day": int(day),
        "mode": "day",
        "force_scrape": True,  # Force scraping even if date exists in lookup
        "test_mode": True  # Enable test mode to use test_data prefix
    }
    logger.info(f"Test event: {json.dumps(event, indent=2)}")

    # Run handler
    response = lambda_handler(event, None)
    logger.info(f"Response: {json.dumps(response, indent=2)}")

    # Verify response
    response_body = json.loads(response["body"])
    if response["statusCode"] != 200:
        logger.error(f"Lambda failed: {response_body.get('error', 'Unknown error')}")
        assert False, f"Lambda failed with status {response['statusCode']}"

    assert response_body["result"] is True, "Lambda returned success=False"

    # Verify DynamoDB entry
    verify_dynamodb_entry(dynamodb_test_table, date_str)

    # Verify S3 files exist and have content
    s3 = boto3.client('s3', region_name=AWS_REGION)
    expected_files = [
        f"test_data/html/{date_str}.html",
        f"test_data/json/{date_str}_meta.json",
        f"test_data/games/year={year}/month={int(month):02}/data.jsonl",
        f"test_data/metadata/year={year}/month={int(month):02}/data.jsonl"
    ]

    for file_path in expected_files:
        try:
            response = s3.head_object(Bucket=s3_test_bucket, Key=file_path)
            assert response['ContentLength'] > 0, f"S3 file {file_path} is empty"
            logger.info(f"Verified S3 file: {file_path} (size: {response['ContentLength']} bytes)")
        except Exception as e:
            assert False, f"Failed to verify S3 file {file_path}: {str(e)}"

    # Verify partitioned data content
    verify_partitioned_data(s3_test_bucket, date_str)
