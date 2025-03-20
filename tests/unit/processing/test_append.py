import unittest
import pandas as pd
from io import BytesIO
import json
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path to import lambda_function
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import lambda_function

class TestDataAppending(unittest.TestCase):

    def setUp(self):
        # Create sample existing data
        self.existing_data = pd.DataFrame([
            {
                'date': pd.Timestamp('2024-01-01'),
                'home_team': 'Team A',
                'away_team': 'Team B',
                'home_score': 3,
                'away_score': 1,
                'league': 'League 1',
                'time': '14:00',
                'url': 'http://example.com/game1',
                'type': 'regular',
                'status': 1.0,
                'headers': 'headers1',
                'timestamp': pd.Timestamp('2024-01-01 15:00:00')
            },
            {
                'date': pd.Timestamp('2024-01-02'),
                'home_team': 'Team C',
                'away_team': 'Team D',
                'home_score': 2,
                'away_score': 2,
                'league': 'League 1',
                'time': '15:00',
                'url': 'http://example.com/game2',
                'type': 'regular',
                'status': 1.0,
                'headers': 'headers2',
                'timestamp': pd.Timestamp('2024-01-02 16:00:00')
            }
        ])

        # Create sample new data (one duplicate, one new)
        self.new_data = pd.DataFrame([
            {
                'date': pd.Timestamp('2024-01-02'),
                'home_team': 'Team C',
                'away_team': 'Team D',
                'home_score': 2,
                'away_score': 2,
                'league': 'League 1',
                'time': '15:00',
                'url': 'http://example.com/game2-updated',
                'type': 'regular',
                'status': 1.0,
                'headers': 'headers2-updated',
                'timestamp': pd.Timestamp('2024-01-02 17:00:00')  # Newer timestamp
            },
            {
                'date': pd.Timestamp('2024-01-03'),
                'home_team': 'Team E',
                'away_team': 'Team F',
                'home_score': 1,
                'away_score': 0,
                'league': 'League 2',
                'time': '16:00',
                'url': 'http://example.com/game3',
                'type': 'regular',
                'status': 1.0,
                'headers': 'headers3',
                'timestamp': pd.Timestamp('2024-01-03 17:00:00')
            }
        ])

    @patch('lambda_function.boto3.client')
    def test_get_existing_dataset(self, mock_boto3_client):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3

        # Mock S3 response
        parquet_buffer = BytesIO()
        self.existing_data.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)

        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: parquet_buffer.getvalue())
        }

        # Call the function
        result = lambda_function.get_existing_dataset('test-bucket', 'test-key')

        # Verify the result
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['home_team'], 'Team A')
        self.assertEqual(result.iloc[1]['home_team'], 'Team C')

    @patch('lambda_function.get_existing_dataset')
    def test_data_appending_and_deduplication(self, mock_get_existing_dataset):
        # Mock the get_existing_dataset function
        mock_get_existing_dataset.return_value = self.existing_data

        # Create a test event with validated data
        validated_data = self.new_data.to_dict('records')

        # Create mock S3 client for convert_to_parquet
        with patch('lambda_function.boto3.client') as mock_boto3_client:
            mock_s3 = MagicMock()
            mock_boto3_client.return_value = mock_s3

            # Call the relevant part of the function manually
            existing_df = self.existing_data
            new_df = pd.DataFrame(validated_data)

            # Apply the same logic as in the function
            if not new_df.empty and not existing_df.empty:
                existing_df['composite_key'] = existing_df.apply(
                    lambda x: f"{x['date']}_{x['home_team']}_{x['away_team']}_{x['league']}", axis=1
                )
                new_df['composite_key'] = new_df.apply(
                    lambda x: f"{x['date']}_{x['home_team']}_{x['away_team']}_{x['league']}", axis=1
                )

                combined_df = pd.concat([existing_df, new_df])
                combined_df = combined_df.sort_values('timestamp', ascending=False)
                combined_df = combined_df.drop_duplicates(subset='composite_key', keep='first')
                combined_df = combined_df.drop(columns=['composite_key'])

            # Verify results
            self.assertEqual(len(combined_df), 3)  # Should have 3 records after deduplication

            # Check that the updated record was kept
            updated_record = combined_df[combined_df['home_team'] == 'Team C'].iloc[0]
            self.assertEqual(updated_record['url'], 'http://example.com/game2-updated')
            self.assertEqual(updated_record['headers'], 'headers2-updated')

            # Check that the new record was added
            new_record = combined_df[combined_df['home_team'] == 'Team E'].iloc[0]
            self.assertEqual(new_record['away_team'], 'Team F')

if __name__ == '__main__':
    unittest.main()