import unittest
import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path to import the lambda_function module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'scraping'))
# Make sure we're only importing from scraping and not accidentally from processing
from scraping.lambda_function import lambda_handler

class TestLambdaHandler(unittest.TestCase):
    """Test cases for the lambda_handler function in the scraper Lambda"""

    @patch('scraping.lambda_function.run_scraper')
    def test_string_params_conversion(self, mock_run_scraper):
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
        self.assertEqual(result['statusCode'], 200)
        mock_run_scraper.assert_called_once()
        # Check that the parameters were passed as integers
        args, kwargs = mock_run_scraper.call_args
        self.assertEqual(kwargs['year'], 2024)
        self.assertEqual(kwargs['month'], 6)
        self.assertEqual(kwargs['day'], 1)
        self.assertEqual(kwargs['force_scrape'], True)

    @patch('scraping.lambda_function.run_month')
    def test_string_params_month_mode(self, mock_run_month):
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
        self.assertEqual(result['statusCode'], 200)
        mock_run_month.assert_called_once()
        # Check that the parameters were passed as integers
        args, kwargs = mock_run_month.call_args
        self.assertEqual(kwargs['year'], 2024)
        self.assertEqual(kwargs['month'], 6)
        self.assertEqual(kwargs['force_scrape'], True)

if __name__ == '__main__':
    unittest.main()