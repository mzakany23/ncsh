#!/usr/bin/env python3
"""
Checkpoint Management for NC Soccer Data Pipeline

This module provides functionality for tracking processed files and maintaining checkpoint
data in S3 to enable incremental processing.
"""

import boto3
import json
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CheckpointManager:
    """Manage processing checkpoints in S3."""

    def __init__(self, bucket, checkpoint_name='default', prefix='data/checkpoints/'):
        """Initialize the checkpoint manager.

        Args:
            bucket (str): S3 bucket name
            checkpoint_name (str): Name to use for this checkpoint
            prefix (str): S3 prefix for checkpoint storage
        """
        self.bucket = bucket
        self.prefix = prefix
        self.checkpoint_name = checkpoint_name
        self.checkpoint_key = f"{prefix}{checkpoint_name}.json"
        self.s3 = boto3.client('s3')

    def read_checkpoint(self):
        """Read the current checkpoint data from S3.

        Returns:
            dict: Checkpoint data or None if it doesn't exist
        """
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.checkpoint_key)
            checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))

            # Convert processed_paths from list to dict (ordered set) for O(1) lookup
            if "processed_paths" in checkpoint_data and isinstance(checkpoint_data["processed_paths"], list):
                # Create a dictionary with paths as keys for O(1) lookup
                processed_paths_dict = {path: True for path in checkpoint_data["processed_paths"]}
                checkpoint_data["processed_paths"] = processed_paths_dict

            return checkpoint_data
        except Exception as e:
            logger.warning(f"Could not read checkpoint {self.checkpoint_key}: {str(e)}")
            return None

    def initialize_checkpoint(self, dry_run=False):
        """Create a new checkpoint file if it doesn't exist."""
        try:
            # Check if checkpoint exists
            try:
                self.s3.head_object(Bucket=self.bucket, Key=self.checkpoint_key)
                logger.info(f"Checkpoint already exists: {self.checkpoint_key}")
                return self.read_checkpoint()
            except Exception:
                # Checkpoint doesn't exist, create a new one
                checkpoint_data = {
                    "last_updated": datetime.utcnow().isoformat(),
                    "processed_files_count": 0,
                    "processed_paths": {},  # Using dict for O(1) lookup (ordered set)
                    "date_ranges": [],      # List of processed date ranges
                    "run_history": []
                }

                if not dry_run:
                    # Convert dict to list for JSON storage
                    json_data = checkpoint_data.copy()
                    json_data["processed_paths"] = list(checkpoint_data["processed_paths"].keys())

                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=self.checkpoint_key,
                        Body=json.dumps(json_data, indent=2),
                        ContentType='application/json'
                    )
                    logger.info(f"Created new checkpoint: {self.checkpoint_key}")
                else:
                    logger.info(f"DRY RUN: Would create new checkpoint: {self.checkpoint_key}")

                return checkpoint_data

        except Exception as e:
            logger.error(f"Error initializing checkpoint: {str(e)}")
            return None

    def update_checkpoint(self, processed_data, dry_run=False):
        """Update the checkpoint with new processing information.

        Args:
            processed_data (dict): Dictionary containing processing information including:
                - processed_paths: List of S3 paths that were processed
                - files_processed: Number of files processed in this run
                - success_count: Number of files successfully processed in this run
                - start_date: Start date of processing range (optional)
                - end_date: End date of processing range (optional)
            dry_run (bool): Whether to actually update the checkpoint
        """
        try:
            # Read current checkpoint
            checkpoint_data = self.read_checkpoint()
            if not checkpoint_data:
                checkpoint_data = self.initialize_checkpoint(dry_run)

            # Handle legacy checkpoints that don't have processed_paths
            if "processed_paths" not in checkpoint_data:
                checkpoint_data["processed_paths"] = {}

            if "date_ranges" not in checkpoint_data:
                checkpoint_data["date_ranges"] = []

            # Update checkpoint
            current_time = datetime.utcnow().isoformat()

            # Add run to history
            run_info = {
                "timestamp": current_time,
                "files_processed": processed_data.get("files_processed", 0),
                "success_count": processed_data.get("success_count", 0),
                "start_date": processed_data.get("start_date"),
                "end_date": processed_data.get("end_date"),
                "run_id": processed_data.get("run_id") if processed_data.get("run_id") else f"run_{int(time.time())}"
            }

            checkpoint_data["run_history"].append(run_info)

            # Update summary fields
            checkpoint_data["last_updated"] = current_time

            # Update processed files count
            if "processed_files_count" not in checkpoint_data:
                checkpoint_data["processed_files_count"] = 0
            checkpoint_data["processed_files_count"] += processed_data.get("files_processed", 0)

            # Add new processed paths - using dict as ordered set for O(1) lookup
            new_paths = processed_data.get("processed_paths", [])
            if new_paths:
                # Add only paths that don't already exist
                for path in new_paths:
                    if path not in checkpoint_data["processed_paths"]:
                        checkpoint_data["processed_paths"][path] = True

            # Add date range if provided
            if processed_data.get("start_date") and processed_data.get("end_date"):
                date_range = {
                    "start_date": processed_data["start_date"],
                    "end_date": processed_data["end_date"],
                    "processed_on": current_time
                }
                checkpoint_data["date_ranges"].append(date_range)

            # Write updated checkpoint
            if not dry_run:
                # Convert dict to list for JSON storage
                json_data = checkpoint_data.copy()
                json_data["processed_paths"] = list(checkpoint_data["processed_paths"].keys())

                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=self.checkpoint_key,
                    Body=json.dumps(json_data, indent=2),
                    ContentType='application/json'
                )
                logger.info(f"Updated checkpoint: {self.checkpoint_key}")
            else:
                logger.info(f"DRY RUN: Would update checkpoint: {self.checkpoint_key}")

            return checkpoint_data

        except Exception as e:
            logger.error(f"Error updating checkpoint: {str(e)}")
            return None

    def is_file_processed(self, file_path):
        """Check if a file has already been processed.

        Args:
            file_path (str): S3 path to check

        Returns:
            bool: True if file has been processed, False otherwise
        """
        checkpoint = self.read_checkpoint()
        if not checkpoint or "processed_paths" not in checkpoint:
            return False

        # O(1) lookup in the dict implementation
        return file_path in checkpoint["processed_paths"]

def main():
    """Example usage of the CheckpointManager."""
    import argparse

    parser = argparse.ArgumentParser(description='Checkpoint Management')
    parser.add_argument('--bucket', required=True, help='S3 bucket name')
    parser.add_argument('--checkpoint', default='default', help='Checkpoint name')
    parser.add_argument('--prefix', default='data/checkpoints/', help='Checkpoint prefix')
    parser.add_argument('--initialize', action='store_true', help='Initialize checkpoint')
    parser.add_argument('--dry-run', action='store_true', help='Dry run')
    parser.add_argument('--list', action='store_true', help='List processed files')

    args = parser.parse_args()

    manager = CheckpointManager(
        bucket=args.bucket,
        prefix=args.prefix,
        checkpoint_name=args.checkpoint
    )

    if args.initialize:
        checkpoint = manager.initialize_checkpoint(dry_run=args.dry_run)

        # Convert to JSON-friendly format for display
        if checkpoint and "processed_paths" in checkpoint:
            checkpoint["processed_paths"] = list(checkpoint["processed_paths"].keys())

        print(json.dumps(checkpoint, indent=2))
    elif args.list:
        checkpoint = manager.read_checkpoint()
        if checkpoint and "processed_paths" in checkpoint:
            paths = list(checkpoint["processed_paths"].keys())
            print(f"Processed files ({len(paths)}):")
            for path in paths:
                print(f"- {path}")
        else:
            print("No processed files found")
    else:
        checkpoint = manager.read_checkpoint()

        # Convert to JSON-friendly format for display
        if checkpoint and "processed_paths" in checkpoint:
            checkpoint["processed_paths"] = list(checkpoint["processed_paths"].keys())

        print(json.dumps(checkpoint, indent=2) if checkpoint else "No checkpoint found")

if __name__ == "__main__":
    main()