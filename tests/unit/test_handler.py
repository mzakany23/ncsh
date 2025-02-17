import os
import json
import pytest
import subprocess
from datetime import datetime

def test_runner_basic_functionality():
    """Test that runner works with basic parameters"""
    # Run the scraper with basic parameters
    result = subprocess.run([
        'python', 'runner.py',
        '--year', '2024',
        '--month', '3',
        '--day', '1',
        '--mode', 'day',
        '--storage-type', 'file',
        '--lookup-type', 'file'
    ], capture_output=True, text=True)

    # Check that the command succeeded
    assert result.returncode == 0, f"Runner failed with output: {result.stderr}"