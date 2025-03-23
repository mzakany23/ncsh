import os
import json
import io
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from processing import lambda_function


def test_list_json_files_excludes_meta_json():
    """Test that list_json_files properly excludes meta.json files"""

    # Mock S3 paginator with both games.jsonl and meta.json files
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {
            'Contents': [
                {'Key': 'v2/processed/json/year=2025/month=02/day=15/games.jsonl', 'LastModified': datetime(2025, 4, 5, 12, 0, 0)},
                {'Key': 'v2/processed/json/year=2025/month=02/day=15/meta.json', 'LastModified': datetime(2025, 4, 5, 12, 0, 0)},
                {'Key': 'v2/processed/json/year=2025/month=02/day=16/games.jsonl', 'LastModified': datetime(2025, 4, 5, 12, 0, 0)}
            ]
        }
    ]

    # Mock S3 client
    mock_s3_client = MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_s3_client.get_object.return_value = {
        'Body': io.BytesIO(json.dumps({'timestamp': '2025-04-01T00:00:00Z'}).encode())
    }

    # Patch boto3.client to return our mock
    with patch('processing.lambda_function.boto3.client', return_value=mock_s3_client):
        # Call the list_json_files function
        files = lambda_function.list_json_files('test-bucket', 'v2/processed/json/', only_recent=False)

        # Verify meta.json is excluded
        assert len(files) == 2
        assert 'meta.json' not in str(files)
        assert all('games.jsonl' in file for file in files)