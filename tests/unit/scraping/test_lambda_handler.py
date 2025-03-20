import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
import json

from scraping.lambda_function import lambda_handler

"""Test cases for the lambda_handler function in the scraper Lambda"""

@patch('scraping.lambda_function.datetime')
@patch('scraping.lambda_function.run_scraper')
def test_string_params_conversion(mock_run_scraper, mock_datetime):
    """Test that string parameters are properly converted to integers."""
    # Setup
    mock_now = MagicMock()
    mock_now.year = 2024
    mock_now.month = 6
    mock_now.day = 1
    mock_datetime.now.return_value = mock_now

    mock_run_scraper.return_value = True
    event = {
        'mode': 'day',
        'parameters': {
            'year': '2024',
            'month': '06',
            'day': '01',
            'force_scrape': True
        }
    }
    mock_context = MagicMock()

    # Execute
    result = lambda_handler(event, mock_context)

    # Assert
    assert result['statusCode'] == 200
    mock_run_scraper.assert_called_once()
    # Check that the parameters were passed as integers
    args, kwargs = mock_run_scraper.call_args
    assert kwargs['year'] == 2024
    assert kwargs['month'] == 6
    assert kwargs['day'] == 1
    assert kwargs['force_scrape'] == True

@patch('scraping.lambda_function.datetime')
@patch('scraping.lambda_function.run_month')
def test_string_params_month_mode(mock_run_month, mock_datetime):
    """Test that string parameters are properly converted to integers in month mode."""
    # Setup
    mock_now = MagicMock()
    mock_now.year = 2024
    mock_now.month = 6
    mock_now.day = 1
    mock_datetime.now.return_value = mock_now

    mock_run_month.return_value = True
    event = {
        'mode': 'month',
        'parameters': {
            'year': '2024',
            'month': '06',
            'force_scrape': True
        }
    }
    mock_context = MagicMock()

    # Execute
    result = lambda_handler(event, mock_context)

    # Assert
    assert result['statusCode'] == 200
    mock_run_month.assert_called_once()
    # Check that the parameters were passed as integers
    args, kwargs = mock_run_month.call_args
    assert kwargs['year'] == 2024
    assert kwargs['month'] == 6
    assert kwargs['force_scrape'] == True

@patch('scraping.lambda_function.run_month')
def test_date_range_mode_validation(mock_run_month):
    """Test the date_range mode input validation."""
    # Test with missing start_date
    event = {
        'mode': 'date_range',
        'parameters': {
            'end_date': '2024-01-31',
            'force_scrape': True
        }
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert 'error' in json.loads(result['body'])
    assert 'start_date and end_date are required' in json.loads(result['body'])['error']

    # Test with missing end_date
    event = {
        'mode': 'date_range',
        'parameters': {
            'start_date': '2024-01-01',
            'force_scrape': True
        }
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert 'error' in json.loads(result['body'])
    assert 'start_date and end_date are required' in json.loads(result['body'])['error']

    # Test with invalid date format
    event = {
        'mode': 'date_range',
        'parameters': {
            'start_date': 'not-a-date',
            'end_date': '2024-01-31',
            'force_scrape': True
        }
    }
    result = lambda_handler(event, None)
    assert result['statusCode'] == 400
    assert 'error' in json.loads(result['body'])
    assert 'Invalid date format' in json.loads(result['body'])['error']

    # Verify run_month was never called in error cases
    assert mock_run_month.call_count == 0