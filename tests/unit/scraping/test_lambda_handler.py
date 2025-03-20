import pytest
from unittest.mock import patch, MagicMock

from scraping.lambda_function import lambda_handler

"""Test cases for the lambda_handler function in the scraper Lambda"""

@patch('scraping.lambda_function.run_scraper')
def test_string_params_conversion(mock_run_scraper):
    """Test that string parameters are properly converted to integers."""
    # Setup
    mock_run_scraper.return_value = True
    event = {
        'year': '2024',
        'month': '06',
        'day': '01',
        'force_scrape': True
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

@patch('scraping.lambda_function.run_month')
def test_string_params_month_mode(mock_run_month):
    """Test that string parameters are properly converted to integers in month mode."""
    # Setup
    mock_run_month.return_value = True
    event = {
        'year': '2024',
        'month': '06',
        'force_scrape': True
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