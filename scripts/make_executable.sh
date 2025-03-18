#!/bin/bash
# Make all Python scripts executable

echo "Making Python scripts executable..."
chmod +x scripts/process_html.py
chmod +x scripts/checkpoint.py
chmod +x scripts/json_to_parquet.py
chmod +x scripts/run_pipeline.py
chmod +x scripts/cleanup_s3.py

echo "Done!"