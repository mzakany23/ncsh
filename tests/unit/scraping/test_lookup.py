import os
import json
import pytest
from datetime import datetime
from ncsoccer.pipeline.lookup import LocalFileLookup, DynamoDBLookup, get_lookup_interface

def test_local_file_lookup(tmp_path):
    """Test LocalFileLookup functionality"""
    # Set up test file path
    lookup_file = tmp_path / "test_lookup.json"
    lookup = LocalFileLookup(lookup_file=str(lookup_file))

    # Test initial state
    assert not lookup.is_date_scraped("2024-03-01")

    # Test updating a date
    lookup.update_date("2024-03-01", success=True, games_count=5)
    assert lookup.is_date_scraped("2024-03-01")

    # Verify file contents
    with open(lookup_file) as f:
        data = json.load(f)
        assert "2024-03-01" in data["scraped_dates"]
        assert data["scraped_dates"]["2024-03-01"]["success"]
        assert data["scraped_dates"]["2024-03-01"]["games_count"] == 5

def test_dynamodb_lookup(mocker):
    """Test DynamoDBLookup functionality with mocked DynamoDB"""
    # Mock DynamoDB resource and table
    mock_table = mocker.MagicMock()
    mock_dynamodb = mocker.MagicMock()
    mock_dynamodb.Table.return_value = mock_table
    mocker.patch("boto3.resource", return_value=mock_dynamodb)

    # Create lookup instance
    lookup = DynamoDBLookup(table_name="test-table")

    # Test checking unscraped date
    mock_table.get_item.return_value = {}
    assert not lookup.is_date_scraped("2024-03-01")

    # Test checking scraped date
    mock_table.get_item.return_value = {
        "Item": {
            "date": "2024-03-01",
            "success": True,
            "games_count": 5
        }
    }
    assert lookup.is_date_scraped("2024-03-01")

    # Test updating a date
    lookup.update_date("2024-03-01", success=True, games_count=5)
    mock_table.put_item.assert_called_once()
    item = mock_table.put_item.call_args[1]["Item"]
    assert item["date"] == "2024-03-01"
    assert item["success"]
    assert item["games_count"] == 5

def test_get_lookup_interface(tmp_path):
    """Test lookup interface factory function"""
    # Test file lookup
    lookup_file = tmp_path / "test_lookup.json"
    lookup = get_lookup_interface("file", lookup_file=str(lookup_file))
    assert isinstance(lookup, LocalFileLookup)

    # Test DynamoDB lookup
    lookup = get_lookup_interface("dynamodb", table_name="test-table")
    assert isinstance(lookup, DynamoDBLookup)

    # Test invalid type
    with pytest.raises(ValueError):
        get_lookup_interface("invalid")