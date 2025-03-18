"""Test that verifies the fix for the 'time' variable scope issue in runner.py"""

import os
import json
import pytest
import subprocess
import sys
from pathlib import Path

def test_time_variable_scope():
    """Test that the runner properly handles the time variable and doesn't encounter scope issues"""
    # Get the path to the runner script
    runner_path = Path(__file__).parents[3] / "scraping" / "ncsoccer" / "runner.py"
    
    # Make sure the runner exists
    assert runner_path.exists(), f"Runner script not found at {runner_path}"
    
    # Run the scraper with parameters that trigger the time variable usage
    # Using a non-existent date to make the test run faster (it will fail to scrape but still use the time variable)
    result = subprocess.run([
        sys.executable,
        str(runner_path),
        '--year', '2099',  # Future date that doesn't exist
        '--month', '12',
        '--mode', 'month',  # Mode month involves retry logic which uses time.sleep
        '--storage-type', 'file',
        '--lookup-type', 'file',
        '--timeout', '5',  # Short timeout to make test run faster
        '--max-retries', '1'  # Only try once to make test run faster
    ], capture_output=True, text=True)
    
    # Check that the command doesn't fail with a time variable error
    assert "cannot access local variable 'time'" not in result.stderr, \
        f"Time variable scope error detected: {result.stderr}"
    
    # The command might fail for other reasons (like not finding the date),
    # but we don't care about that - we just want to ensure no time variable scope errors
