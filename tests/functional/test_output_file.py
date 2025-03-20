#!/usr/bin/env python3
"""
Basic test for file output functionality in the NC Soccer scraper.
This test directly tests if we can write HTML and JSON files properly.
"""
import os
import sys
import json
import logging
import pytest
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Sample test data
SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>NC Soccer Test Page</title>
</head>
<body>
    <h1>Test Fixture</h1>
    <div class="game">
        <div class="home-team">Team A</div>
        <div class="away-team">Team B</div>
        <div class="score">3-2</div>
    </div>
</body>
</html>
"""

SAMPLE_JSON = [
    {
        "league": "High School Girls Test League",
        "session": "1 2024-25",
        "home_team": "Team A",
        "away_team": "Team B",
        "status": "Complete",
        "venue": "Field 1",
        "officials": "Test Official",
        "time": None,
        "home_score": 3,
        "away_score": 2
    }
]

def test_file_output(ensure_test_dirs):
    """Test that we can properly write HTML and JSON files."""
    # Create output directories
    output_dir = ensure_test_dirs
    html_dir = output_dir / 'html'
    json_dir = output_dir / 'json'
    html_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    
    # File paths
    date_str = "2025-01-15"
    html_file = html_dir / f"{date_str}.html"
    json_file = json_dir / f"{date_str}.json"
    
    # Write sample HTML file
    with open(html_file, 'w') as f:
        f.write(SAMPLE_HTML)
    
    logger.info(f"Wrote HTML file to {html_file}")
    
    # Write sample JSON file
    with open(json_file, 'w') as f:
        json.dump(SAMPLE_JSON, f, indent=2)
    
    logger.info(f"Wrote JSON file to {json_file}")
    
    # Verify files exist
    assert os.path.exists(html_file), f"HTML file not created at {html_file}"
    assert os.path.exists(json_file), f"JSON file not created at {json_file}"
    
    # Verify file content
    with open(html_file, 'r') as f:
        html_content = f.read()
        assert "NC Soccer Test Page" in html_content
    
    with open(json_file, 'r') as f:
        json_content = json.load(f)
        assert len(json_content) > 0
        assert json_content[0]["home_team"] == "Team A"
    
    logger.info("File output test completed successfully")
    # No return statement needed for pytest functions
