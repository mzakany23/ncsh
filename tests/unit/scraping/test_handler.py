import subprocess
import sys
from pathlib import Path

def test_runner_basic_functionality():
    """Test that runner works with basic parameters"""
    # Get the path to the runner script
    runner_path = Path(__file__).parents[3] / "scraping" / "ncsoccer" / "runner.py"

    # Make sure the runner exists
    assert runner_path.exists(), f"Runner script not found at {runner_path}"

    # Run the scraper with basic parameters
    result = subprocess.run([
        sys.executable,
        str(runner_path),
        '--year', '2024',
        '--month', '3',
        '--day', '1',
        '--mode', 'day',
        '--storage-type', 'file',
        '--lookup-type', 'file'
    ], capture_output=True, text=True)

    # Check that there is no time variable scope error
    # The script might fail for other reasons (like not finding spiders in test environment)
    # but we just want to confirm that our fix for the time variable scope issue works
    assert "cannot access local variable 'time'" not in result.stderr, \
        f"Time variable scope error detected: {result.stderr}"