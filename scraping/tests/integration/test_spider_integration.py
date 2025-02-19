import os
import pytest
import subprocess
from datetime import datetime
from pathlib import Path

def test_local_runner_e2e():
    """Test the local runner.py script end-to-end with file storage"""
    # Set up test parameters
    year = 2024
    month = 3
    day = 1
    date_str = f"{year}-{month:02d}-{day:02d}"

    # Run the scraper with file storage
    result = subprocess.run([
        'python', '-m', 'ncsoccer.runner',
        '--year', str(year),
        '--month', str(month),
        '--day', str(day),
        '--force-scrape',
        '--storage-type', 'file',
        '--lookup-type', 'file'
    ], capture_output=True, text=True)

    # Check that the command succeeded
    assert result.returncode == 0, f"Runner failed with output: {result.stderr}"

    # Expected files
    expected_files = [
        f"data/html/{date_str}.html",
        f"data/json/{date_str}_meta.json",
        f"data/games/year={year}/month={month:02d}/day={day:02d}/data.jsonl",
        f"data/metadata/year={year}/month={month:02d}/day={day:02d}/data.jsonl"
    ]

    # Verify all expected files exist and have content
    for file_path in expected_files:
        assert os.path.exists(file_path), f"File {file_path} should exist"
        assert os.path.getsize(file_path) > 0, f"File {file_path} should not be empty"