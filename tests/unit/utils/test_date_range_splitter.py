import json
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add the src directory to the path so we can import the module
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[3] / "utils" / "src"))

import date_range_splitter

class TestDateRangeSplitter(unittest.TestCase):
    """Test the date_range_splitter Lambda function"""

    def setUp(self):
        """Set up test fixtures"""
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {'STATE_MACHINE_ARN': 'arn:aws:states:us-east-2:123456789012:stateMachine:ncsoccer-unified-workflow-recursive'})
        self.env_patcher.start()
        
    def tearDown(self):
        """Tear down test fixtures"""
        self.env_patcher.stop()

    def test_small_date_range_no_split(self):
        """Test that a small date range doesn't get split"""
        event = {
            'start_date': '2023-01-01',
            'end_date': '2023-01-10',
            'max_chunk_size_days': 90,
            'bucket_name': 'test-bucket',
            'force_scrape': False,
            'architecture_version': 'v2',
            'batch_size': 3
        }
        
        result = date_range_splitter.handler(event, MagicMock())
        
        self.assertFalse(result['split_required'])
        self.assertEqual(result['original_range']['start_date'], '2023-01-01')
        self.assertEqual(result['original_range']['end_date'], '2023-01-10')
        
    @patch('boto3.client')
    def test_large_date_range_split(self, mock_boto3_client):
        """Test that a large date range gets split into chunks"""
        # Mock the Step Functions client
        mock_sfn_client = MagicMock()
        mock_boto3_client.return_value = mock_sfn_client
        mock_sfn_client.start_execution.return_value = {'executionArn': 'test-execution-arn'}
        
        event = {
            'start_date': '2023-01-01',
            'end_date': '2023-06-30',  # 181 days, should be split into 4 chunks with 60-day max
            'max_chunk_size_days': 60,
            'bucket_name': 'test-bucket',
            'force_scrape': False,
            'architecture_version': 'v2',
            'batch_size': 3
        }
        
        context = MagicMock()
        context.aws_request_id = 'test-request-id'
        
        result = date_range_splitter.handler(event, context)
        
        self.assertTrue(result['split_required'])
        self.assertEqual(len(result['chunks']), 4)
        
        # Verify first and last chunks
        self.assertEqual(result['chunks'][0]['start_date'], '2023-01-01')
        self.assertEqual(result['chunks'][-1]['end_date'], '2023-06-30')
        
        # Verify that dates are continuous (end of one chunk + 1 day = start of next chunk)
        for i in range(len(result['chunks']) - 1):
            end_date = datetime.strptime(result['chunks'][i]['end_date'], '%Y-%m-%d')
            next_start_date = datetime.strptime(result['chunks'][i+1]['start_date'], '%Y-%m-%d')
            self.assertEqual((next_start_date - end_date).days, 1)
        
        # Verify that start_execution was called for each chunk
        self.assertEqual(mock_sfn_client.start_execution.call_count, 4)

    @patch('boto3.client')
    def test_missing_state_machine_arn(self, mock_boto3_client):
        """Test that an error is raised when STATE_MACHINE_ARN is not set"""
        # Remove the environment variable
        with patch.dict('os.environ', {}, clear=True):
            event = {
                'start_date': '2023-01-01',
                'end_date': '2023-06-30',
                'max_chunk_size_days': 60,
                'bucket_name': 'test-bucket'
            }
            
            with self.assertRaises(ValueError) as context:
                date_range_splitter.handler(event, MagicMock())
            
            self.assertTrue('STATE_MACHINE_ARN environment variable is not set' in str(context.exception))

if __name__ == '__main__':
    unittest.main()
