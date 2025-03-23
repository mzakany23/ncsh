import pytest
from processing.lambda_function import validate_and_transform_data
from datetime import datetime, timezone

def test_validate_transform_alternative_format():
    """Test that validate_and_transform_data can handle the alternative format with league_name and game_date."""
    # Example of alternative format data (from games.jsonl)
    alternative_data = [
        {
            "league_name": "Cleveland Select Spring 2025",
            "game_date": "2025-02-15",
            "home_team": "Hudson United Tall Ships DB",
            "away_team": "Cleveland Select U8",
            "score": "7 - 2",
            "game_time": "10:00 AM",
            "field": "Field 3",
            "url": "https://example.com/games/123",
            "game_type": "regular",
            "status": 1.0
        }
    ]

    # Run the validation and transformation
    result = validate_and_transform_data(alternative_data)

    # Verify the results
    assert len(result) == 1, "Should have one valid record"

    # Check field mappings
    record = result[0]
    assert isinstance(record["date"], datetime), "Date should be a datetime object"
    assert record["date"].strftime("%Y-%m-%d") == "2025-02-15"
    assert record["home_team"] == "Hudson United Tall Ships DB"
    assert record["away_team"] == "Cleveland Select U8"
    assert record["home_score"] == 7
    assert record["away_score"] == 2
    assert record["league"] == "Cleveland Select Spring 2025"
    assert record["time"] == "10:00 AM"
    assert record["url"] == "https://example.com/games/123"
    assert record["type"] == "regular"
    assert record["status"] == 1.0

def test_validate_transform_standard_format():
    """Test that validate_and_transform_data still works with the standard format."""
    # Example of standard format data
    standard_data = [
        {
            "date": "2025-02-15",
            "games": {
                "home_team": "Hudson United Tall Ships DB",
                "away_team": "Cleveland Select U8",
                "home_score": 7,
                "away_score": 2,
                "league": "Cleveland Select Spring 2025",
                "time": "10:00 AM"
            },
            "url": "https://example.com/games/123",
            "type": "regular",
            "status": 1.0
        }
    ]

    # Run the validation and transformation
    result = validate_and_transform_data(standard_data)

    # Verify the results
    assert len(result) == 1, "Should have one valid record"

    # Check field mappings
    record = result[0]
    assert isinstance(record["date"], datetime), "Date should be a datetime object"
    assert record["date"].strftime("%Y-%m-%d") == "2025-02-15"
    assert record["home_team"] == "Hudson United Tall Ships DB"
    assert record["away_team"] == "Cleveland Select U8"
    assert record["home_score"] == 7
    assert record["away_score"] == 2
    assert record["league"] == "Cleveland Select Spring 2025"
    assert record["time"] == "10:00 AM"
    assert record["url"] == "https://example.com/games/123"
    assert record["type"] == "regular"
    assert record["status"] == 1.0

def test_validate_transform_malformed_score():
    """Test that validate_and_transform_data handles malformed score values gracefully."""
    # Example with a malformed score
    alt_data_with_bad_score = [
        {
            "league_name": "Cleveland Select Spring 2025",
            "game_date": "2025-02-15",
            "home_team": "Hudson United Tall Ships DB",
            "away_team": "Cleveland Select U8",
            "score": "not a score",  # Malformed score
            "game_time": "10:00 AM",
            "field": "Field 3"
        }
    ]

    # Run the validation and transformation
    result = validate_and_transform_data(alt_data_with_bad_score)

    # Verify the results
    assert len(result) == 1, "Should have one valid record even with bad score"

    # Check score fields are None due to parsing failure
    record = result[0]
    assert record["home_score"] is None
    assert record["away_score"] is None