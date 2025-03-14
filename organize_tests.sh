#!/bin/bash
# Script to organize all tests into a unified test directory structure

# Create test directories if they don't exist
mkdir -p tests/unit/processing
mkdir -p tests/unit/scraping
mkdir -p tests/integration/scraping
mkdir -p tests/integration/processing
mkdir -p tests/functional

# Copy processing tests to appropriate directories
cp -v processing/tests/test_append.py tests/unit/processing/

# Copy scraping tests to appropriate directories
cp -v scraping/tests/integration/test_spider_integration.py tests/integration/scraping/
cp -v scraping/tests/unit/test_handler.py tests/unit/scraping/
cp -v scraping/tests/unit/test_html_parser.py tests/unit/scraping/
cp -v scraping/tests/unit/test_lookup.py tests/unit/scraping/

# Copy utility test scripts
cp -v scripts/test_backfill.py tests/functional/test_backfill.py

# Create an empty __init__.py file in each directory to make it a proper Python package
find tests -type d -exec touch {}/__init__.py \;

echo "Tests have been organized. You can now delete the old test directories if desired."
