import pytest
import os
import shutil
import subprocess
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test files before and after each test"""
    # Clean up local files
    test_dir = 'test_data'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    yield

    # Cleanup after tests
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

@pytest.fixture(scope="module")
def s3_test_bucket():
    """Set up test bucket prefix for S3 storage tests"""
    bucket_name = 'ncsh-app-data'
    test_prefix = 'test_data'
    s3 = boto3.client('s3', region_name='us-east-2')

    yield bucket_name

    # Clean up test objects after tests
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=test_prefix)
        if 'Contents' in objects:
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in objects['Contents']]}
            s3.delete_objects(Bucket=bucket_name, Delete=delete_keys)
    except ClientError:
        pass

@pytest.fixture(scope="module")
def dynamodb_test_table():
    """Create a test DynamoDB table for integration tests"""
    table_name = "ncsh-scraped-dates-test"
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')

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

def verify_json_content(data, date_str):
    """Helper function to verify JSON file content"""
    assert 'date' in data
    assert 'games_found' in data
    assert 'games' in data
    assert data['date'] == date_str

def verify_lookup_content(lookup_data, date_str):
    """Helper function to verify lookup file content"""
    assert date_str in lookup_data, f"Date {date_str} not found in lookup data"
    assert "success" in lookup_data[date_str], "Lookup entry missing 'success' field"
    assert "games_count" in lookup_data[date_str], "Lookup entry missing 'games_count' field"
    assert "timestamp" in lookup_data[date_str], "Lookup entry missing 'timestamp' field"
    assert lookup_data[date_str]["success"], "Spider run was not marked as successful"
    assert lookup_data[date_str]["games_count"] > 0, "No games were recorded in lookup data"

def verify_dynamodb_entry(table_name, date_str):
    """Verify that the DynamoDB entry was created correctly"""
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={'date': date_str})

    assert 'Item' in response, f"Date {date_str} not found in DynamoDB"
    item = response['Item']
    assert item['success'], "Spider run was not marked as successful in DynamoDB"
    assert item['games_count'] > 0, "No games were recorded in DynamoDB"
    assert 'timestamp' in item, "Timestamp missing in DynamoDB entry"

def test_spider_file_creation():
    """Integration test that spider creates the expected files in local filesystem"""
    # Set up test date
    test_year = 2024
    test_month = 3
    test_day = 1
    date_str = f"{test_year}-{test_month:02d}-{test_day:02d}"

    # Run spider via CLI with file lookup
    result = subprocess.run([
        'python', 'runner.py',
        '--mode', 'day',
        '--year', str(test_year),
        '--month', str(test_month),
        '--day', str(test_day),
        '--storage-type', 'file',
        '--html-prefix', 'test_data/html',
        '--json-prefix', 'test_data/json',
        '--lookup-file', 'test_data/lookup.json',
        '--lookup-type', 'file',
        '--region', 'us-east-2'
    ], check=True, capture_output=True, text=True)

    print("Spider output:", result.stdout)
    if result.stderr:
        print("Spider errors:", result.stderr)

    # Check that files were created
    expected_files = [
        f"test_data/html/{date_str}.html",
        f"test_data/json/{date_str}.json",
        f"test_data/json/{date_str}_meta.json",
        "test_data/lookup.json"
    ]

    for file_path in expected_files:
        assert os.path.exists(file_path), f"Expected file {file_path} was not created"
        assert os.path.getsize(file_path) > 0, f"File {file_path} is empty"

    # Verify JSON structure and content
    with open(f"test_data/json/{date_str}.json") as f:
        data = json.load(f)
        verify_json_content(data, date_str)

    # Verify lookup file structure
    with open("test_data/lookup.json") as f:
        lookup_data = json.load(f)
        verify_lookup_content(lookup_data['scraped_dates'], date_str)

def test_spider_s3_creation(s3_test_bucket, dynamodb_test_table):
    """Integration test that spider creates the expected files in S3"""
    # Set up test date
    test_year = 2024
    test_month = 3
    test_day = 1
    date_str = f"{test_year}-{test_month:02d}-{test_day:02d}"

    # Set environment variables
    os.environ['DYNAMODB_TABLE'] = dynamodb_test_table

    # Run spider via CLI with DynamoDB lookup
    result = subprocess.run([
        'python', 'runner.py',
        '--mode', 'day',
        '--year', str(test_year),
        '--month', str(test_month),
        '--day', str(test_day),
        '--storage-type', 's3',
        '--bucket-name', s3_test_bucket,
        '--html-prefix', 'test_data/html',
        '--json-prefix', 'test_data/json',
        '--lookup-type', 'dynamodb',
        '--region', 'us-east-2',
        '--table-name', dynamodb_test_table
    ], check=True, capture_output=True, text=True)

    print("Spider output:", result.stdout)
    if result.stderr:
        print("Spider errors:", result.stderr)

    # Check that files were created in S3
    s3 = boto3.client('s3')
    expected_s3_files = [
        f"test_data/html/{date_str}.html",
        f"test_data/json/{date_str}.json",
        f"test_data/json/{date_str}_meta.json"
    ]

    for s3_path in expected_s3_files:
        try:
            response = s3.head_object(Bucket=s3_test_bucket, Key=s3_path)
            assert response['ContentLength'] > 0, f"S3 file {s3_path} is empty"
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                assert False, f"Expected S3 file {s3_path} was not created"
            raise

    # Verify JSON structure and content
    json_response = s3.get_object(Bucket=s3_test_bucket, Key=f"test_data/json/{date_str}.json")
    data = json.loads(json_response['Body'].read().decode('utf-8'))
    verify_json_content(data, date_str)

    # Verify DynamoDB entry
    verify_dynamodb_entry(dynamodb_test_table, date_str)