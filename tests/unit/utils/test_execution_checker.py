import json
import unittest
from unittest.mock import patch, MagicMock

# Add the src directory to the path so we can import the module
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[3] / "utils" / "src"))

import execution_checker

class TestExecutionChecker(unittest.TestCase):
    """Test the execution_checker Lambda function"""

    @patch('boto3.client')
    def test_no_executions(self, mock_boto3_client):
        """Test handling when no executions are provided"""
        event = {
            'executions': []
        }
        
        result = execution_checker.handler(event, MagicMock())
        
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], 'No executions to check')
        self.assertEqual(result['executions'], [])
        
    @patch('boto3.client')
    def test_all_executions_succeeded(self, mock_boto3_client):
        """Test when all executions have succeeded"""
        # Mock the Step Functions client
        mock_sfn_client = MagicMock()
        mock_boto3_client.return_value = mock_sfn_client
        
        # Set up mock responses for describe_execution
        mock_sfn_client.describe_execution.side_effect = [
            {
                'status': 'SUCCEEDED',
                'output': json.dumps({'result': 'success1'})
            },
            {
                'status': 'SUCCEEDED',
                'output': json.dumps({'result': 'success2'})
            }
        ]
        
        event = {
            'executions': [
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test1',
                    'start_date': '2023-01-01',
                    'end_date': '2023-01-31'
                },
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test2',
                    'start_date': '2023-02-01',
                    'end_date': '2023-02-28'
                }
            ]
        }
        
        result = execution_checker.handler(event, MagicMock())
        
        self.assertTrue(result['success'])
        self.assertEqual(result['message'], 'All executions completed successfully')
        self.assertEqual(len(result['executions']), 2)
        self.assertEqual(result['status_counts'], {'SUCCEEDED': 2})
        
    @patch('boto3.client')
    def test_mixed_execution_statuses(self, mock_boto3_client):
        """Test when executions have mixed statuses"""
        # Mock the Step Functions client
        mock_sfn_client = MagicMock()
        mock_boto3_client.return_value = mock_sfn_client
        
        # Set up mock responses for describe_execution
        mock_sfn_client.describe_execution.side_effect = [
            {
                'status': 'SUCCEEDED',
                'output': json.dumps({'result': 'success'})
            },
            {
                'status': 'RUNNING'
            },
            {
                'status': 'FAILED',
                'error': 'Test error',
                'cause': 'Test cause'
            }
        ]
        
        event = {
            'executions': [
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test1',
                    'start_date': '2023-01-01',
                    'end_date': '2023-01-31'
                },
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test2',
                    'start_date': '2023-02-01',
                    'end_date': '2023-02-28'
                },
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test3',
                    'start_date': '2023-03-01',
                    'end_date': '2023-03-31'
                }
            ]
        }
        
        result = execution_checker.handler(event, MagicMock())
        
        self.assertFalse(result['success'])
        self.assertEqual(result['message'], 'Some executions are still running or have failed')
        self.assertEqual(len(result['executions']), 3)
        self.assertEqual(result['status_counts'], {'SUCCEEDED': 1, 'RUNNING': 1, 'FAILED': 1})
        
    @patch('boto3.client')
    def test_api_error_handling(self, mock_boto3_client):
        """Test handling of API errors"""
        # Mock the Step Functions client
        mock_sfn_client = MagicMock()
        mock_boto3_client.return_value = mock_sfn_client
        
        # Set up mock responses for describe_execution
        mock_sfn_client.describe_execution.side_effect = [
            Exception("API Error")
        ]
        
        event = {
            'executions': [
                {
                    'execution_arn': 'arn:aws:states:us-east-2:123456789012:execution:test1',
                    'start_date': '2023-01-01',
                    'end_date': '2023-01-31'
                }
            ]
        }
        
        result = execution_checker.handler(event, MagicMock())
        
        self.assertFalse(result['success'])
        self.assertEqual(len(result['executions']), 1)
        self.assertEqual(result['executions'][0]['status'], 'ERROR')
        self.assertEqual(result['status_counts'], {'ERROR': 1})

if __name__ == '__main__':
    unittest.main()
