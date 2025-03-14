"""
Global pytest configuration for NCSoccer tests.
This file handles path setup and shared fixtures for all tests.
"""
import os
import sys
import pytest
from pathlib import Path

# Add the project root directory to the Python path
# This ensures all modules can be imported properly in tests
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Also add the scraping and processing modules directly
scraping_dir = project_root / 'scraping'
processing_dir = project_root / 'processing'

for path in [str(scraping_dir), str(processing_dir)]:
    if path not in sys.path:
        sys.path.insert(0, path)

@pytest.fixture
def test_data_dir():
    """Return a Path object pointing to the test data directory."""
    return project_root / 'tests' / 'data'

@pytest.fixture
def ensure_test_dirs():
    """Ensure that test output directories exist."""
    test_output = project_root / 'tests' / 'output'
    html_dir = test_output / 'html'
    json_dir = test_output / 'json'
    
    for directory in [test_output, html_dir, json_dir]:
        directory.mkdir(parents=True, exist_ok=True)
    
    return test_output
