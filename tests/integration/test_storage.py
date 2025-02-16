import pytest
import os
from datetime import datetime
from ncsoccer.pipeline.config import (
    StorageType,
    get_storage_interface,
    create_scraper_config
)

@pytest.fixture
def test_data():
    return {
        'test_content': 'Hello, World!',
        'test_json': '{"message": "Hello, World!"}',
        'test_html': '<html><body>Hello, World!</body></html>'
    }

@pytest.fixture
def file_storage():
    return get_storage_interface(StorageType.FILE)

@pytest.fixture
def s3_storage():
    # Only create S3 storage if we have credentials and bucket
    if 'DATA_BUCKET' in os.environ:
        return get_storage_interface(StorageType.S3)
    pytest.skip("No S3 credentials available")

def test_file_storage_write_read(file_storage, test_data, tmp_path):
    # Test writing and reading a regular file
    test_file = tmp_path / "test.txt"
    assert file_storage.write(str(test_file), test_data['test_content'])
    assert file_storage.exists(str(test_file))
    assert file_storage.read(str(test_file)) == test_data['test_content']

def test_file_storage_json(file_storage, test_data, tmp_path):
    # Test writing and reading JSON
    test_file = tmp_path / "test.json"
    assert file_storage.write(str(test_file), test_data['test_json'])
    assert file_storage.exists(str(test_file))
    assert file_storage.read(str(test_file)) == test_data['test_json']

@pytest.mark.skipif('DATA_BUCKET' not in os.environ, reason="No S3 credentials")
def test_s3_storage_write_read(s3_storage, test_data):
    # Test writing and reading from S3
    test_key = f"test/integration/{datetime.now().isoformat()}.txt"
    assert s3_storage.write(test_key, test_data['test_content'])
    assert s3_storage.exists(test_key)
    assert s3_storage.read(test_key) == test_data['test_content']

@pytest.mark.skipif('DATA_BUCKET' not in os.environ, reason="No S3 credentials")
def test_s3_storage_json(s3_storage, test_data):
    # Test writing and reading JSON from S3
    test_key = f"test/integration/{datetime.now().isoformat()}.json"
    assert s3_storage.write(test_key, test_data['test_json'])
    assert s3_storage.exists(test_key)
    assert s3_storage.read(test_key) == test_data['test_json']

def test_storage_interface_creation():
    # Test creating storage interfaces with different configurations
    config = create_scraper_config(
        mode='day',
        year=2024,
        month=3,
        day=1,
        storage_type='file'
    )
    storage = get_storage_interface(config.storage_type)
    assert storage.__class__.__name__ == 'FileStorage'

    if 'DATA_BUCKET' in os.environ:
        config = create_scraper_config(
            mode='day',
            year=2024,
            month=3,
            day=1,
            storage_type='s3'
        )
        storage = get_storage_interface(config.storage_type)
        assert storage.__class__.__name__ == 'S3Storage'