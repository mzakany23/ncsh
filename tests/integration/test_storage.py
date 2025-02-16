import pytest
import os
import shutil
import subprocess
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError

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

def verify_json_content(data, date_str):
    """Helper function to verify JSON content structure"""
    # Check required fields
    assert "date" in data, "JSON missing 'date' field"
    assert "games_found" in data, "JSON missing 'games_found' field"
    assert "games" in data, "JSON missing 'games' field"
    assert isinstance(data["games"], list), "'games' field should be a list"

    # Check date format
    assert data["date"] == date_str, f"Date mismatch. Expected {date_str}, got {data['date']}"

    # If games were found, verify game structure
    if data["games_found"] and data["games"]:
        for game in data["games"]:
            required_fields = [
                "league", "session", "home_team", "away_team",
                "home_score", "away_score", "status", "venue",
                "time", "officials"
            ]
            for field in required_fields:
                assert field in game, f"Game missing required field: {field}"

            # Verify score types
            if game["home_score"] is not None:
                assert isinstance(game["home_score"], int), "home_score should be an integer"
            if game["away_score"] is not None:
                assert isinstance(game["away_score"], int), "away_score should be an integer"

            # Verify non-empty strings
            string_fields = ["league", "home_team", "away_team", "status", "venue", "officials"]
            for field in string_fields:
                assert isinstance(game[field], str), f"{field} should be a string"
                assert game[field].strip(), f"{field} should not be empty"

def verify_lookup_content(lookup_data, date_str):
    """Helper function to verify lookup file content"""
    assert date_str in lookup_data, f"Date {date_str} not found in lookup data"
    assert "success" in lookup_data[date_str], "Lookup entry missing 'success' field"
    assert "games_count" in lookup_data[date_str], "Lookup entry missing 'games_count' field"
    assert "timestamp" in lookup_data[date_str], "Lookup entry missing 'timestamp' field"
    assert lookup_data[date_str]["success"], "Spider run was not marked as successful"
    assert lookup_data[date_str]["games_count"] > 0, "No games were recorded in lookup data"

def test_spider_file_creation():
    """Integration test that spider creates the expected files in local filesystem"""
    # Set up test date
    test_year = 2024
    test_month = 3
    test_day = 1
    date_str = f"{test_year}-{test_month:02d}-{test_day:02d}"

    # Run spider via CLI
    result = subprocess.run([
        'python', 'runner.py',
        '--mode', 'day',
        '--year', str(test_year),
        '--month', str(test_month),
        '--day', str(test_day),
        '--storage-type', 'file',
        '--html-prefix', 'test_data/html',
        '--json-prefix', 'test_data/json',
        '--lookup-file', 'test_data/lookup.json'
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
        verify_lookup_content(lookup_data, date_str)

def test_spider_s3_creation(s3_test_bucket):
    """Integration test that spider creates the expected files in S3"""
    # Set up test date
    test_year = 2024
    test_month = 3
    test_day = 1
    date_str = f"{test_year}-{test_month:02d}-{test_day:02d}"

    # Run spider via CLI with S3 storage
    result = subprocess.run([
        'python', 'runner.py',
        '--mode', 'day',
        '--year', str(test_year),
        '--month', str(test_month),
        '--day', str(test_day),
        '--storage-type', 's3',
        '--bucket-name', s3_test_bucket,
        '--html-prefix', 'test/html',
        '--json-prefix', 'test/json',
        '--lookup-file', 'test/lookup.json'
    ], check=True, capture_output=True, text=True)

    print("Spider output:", result.stdout)
    if result.stderr:
        print("Spider errors:", result.stderr)

    # Check that files were created in S3
    s3 = boto3.client('s3')
    expected_s3_files = [
        f"test/html/{date_str}.html",
        f"test/json/{date_str}.json",
        f"test/json/{date_str}_meta.json",
        "test/lookup.json"
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
    json_response = s3.get_object(Bucket=s3_test_bucket, Key=f"test/json/{date_str}.json")
    data = json.loads(json_response['Body'].read().decode('utf-8'))
    verify_json_content(data, date_str)

    # Verify lookup file structure
    lookup_response = s3.get_object(Bucket=s3_test_bucket, Key="test/lookup.json")
    lookup_data = json.loads(lookup_response['Body'].read().decode('utf-8'))
    verify_lookup_content(lookup_data, date_str)