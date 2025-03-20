import os
import json
import pytest
from datetime import datetime
from ncsoccer.pipeline.lookup import LocalFileLookup, get_lookup_interface

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

def test_get_lookup_interface(tmp_path):
    """Test lookup interface factory function"""
    # Test file lookup
    lookup_file = tmp_path / "test_lookup.json"
    lookup = get_lookup_interface("file", lookup_file=str(lookup_file))
    assert isinstance(lookup, LocalFileLookup)

    # Test invalid type
    with pytest.raises(ValueError):
        get_lookup_interface("invalid")