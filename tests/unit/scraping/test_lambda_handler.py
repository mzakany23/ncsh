import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, date
import json
import calendar

from scraping.lambda_function import lambda_handler, handle_unified_format

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
@patch('scraping.lambda_function.handle_unified_format')
def test_string_params_month_mode(mock_handle_unified, mock_datetime):
    """Test that month mode is properly converted to unified format."""
    # Setup
    mock_now = MagicMock()
    mock_now.year = 2024
    mock_now.month = 6
    mock_now.day = 1
    mock_datetime.now.return_value = mock_now

    mock_handle_unified.return_value = {'statusCode': 200, 'body': json.dumps({'success': True})}

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
    mock_handle_unified.assert_called_once()

    # Verify unified event was created correctly
    call_args = mock_handle_unified.call_args[0]
    unified_event = call_args[0]

    # Calculate expected start and end dates
    year = 2024
    month = 6
    last_day = calendar.monthrange(year, month)[1]

    assert unified_event['start_date'] == '2024-06-01'
    assert unified_event['end_date'] == f'2024-06-{last_day}'
    assert unified_event['force_scrape'] == True

@patch('scraping.lambda_function.handle_unified_format')
def test_date_range_mode_validation(mock_handle_unified):
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

    # Test with valid parameters
    event = {
        'mode': 'date_range',
        'parameters': {
            'start_date': '2024-01-01',
            'end_date': '2024-01-31',
            'force_scrape': True
        }
    }
    mock_handle_unified.return_value = {'statusCode': 200, 'body': json.dumps({'success': True})}
    result = lambda_handler(event, None)
    assert result['statusCode'] == 200

    # Verify handle_unified_format was called with correct parameters
    mock_handle_unified.assert_called_once()
    call_args = mock_handle_unified.call_args[0]
    unified_event = call_args[0]

    assert unified_event['start_date'] == '2024-01-01'
    assert unified_event['end_date'] == '2024-01-31'
    assert unified_event['force_scrape'] == True