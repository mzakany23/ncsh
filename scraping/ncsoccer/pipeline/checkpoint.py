"""
Unified checkpoint system for the v2 data architecture.
This module provides a consistent interface for all checkpointing operations.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

class UnifiedCheckpoint:
    """
    Unified checkpoint system that maintains a single checkpoint file
    with sections for different processes (scraping, processing, conversion).
    """

    def __init__(self, checkpoint_file: str, storage_interface=None):
        """
        Initialize the checkpoint system.

        Args:
            checkpoint_file: Path to the checkpoint file
            storage_interface: Optional storage interface for S3 or other remote storage
        """
        self.checkpoint_file = checkpoint_file
        self.storage = storage_interface

        # Detect Lambda environment
        self.in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        if self.in_lambda and self.storage is None:
            logger.warning(
                "UnifiedCheckpoint initialized without storage interface in Lambda environment. "
                "Checkpoint data won't persist between invocations. "
                "Please provide an S3Storage interface.")

        self._data = self._load_checkpoint()

    def _load_checkpoint(self) -> Dict[str, Any]:
        """
        Load the checkpoint data from file.

        Returns:
            Dictionary with checkpoint data
        """
        # Default checkpoint structure
        default_data = {
            'version': 'v2',
            'last_updated': datetime.now().isoformat(),
            'scraping': {
                'last_updated': datetime.now().isoformat(),
                'completed_dates': {}
            },
            'processing': {
                'last_updated': datetime.now().isoformat(),
                'completed_dates': {}
            },
            'parquet_conversion': {
                'last_updated': datetime.now().isoformat(),
                'status': 'initialized',
                'version': None
            }
        }

        # Check if file exists
        if self.storage:
            # Remote storage (e.g., S3)
            exists = self.storage.exists(self.checkpoint_file)
            if exists:
                try:
                    content = self.storage.read(self.checkpoint_file)
                    data = json.loads(content)
                    return data
                except Exception as e:
                    logger.error(f"Error loading checkpoint: {e}")
                    return default_data
            else:
                # Create new checkpoint file
                try:
                    self.storage.write(self.checkpoint_file, json.dumps(default_data, indent=2))
                except Exception as e:
                    logger.error(f"Error creating checkpoint: {e}")
                return default_data
        else:
            # Local file storage
            # Detect Lambda environment
            in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ

            # When in Lambda, we must prepend /tmp to the path
            lambda_tmp_prefix = '/tmp/' if in_lambda else ''
            local_checkpoint_file = f"{lambda_tmp_prefix}{self.checkpoint_file}"

            if os.path.exists(local_checkpoint_file):
                try:
                    with open(local_checkpoint_file, 'r') as f:
                        data = json.load(f)
                    return data
                except Exception as e:
                    logger.error(f"Error loading checkpoint: {e}")
                    return default_data
            else:
                # Create directory if needed, but only if not in Lambda or using /tmp
                os.makedirs(os.path.dirname(local_checkpoint_file), exist_ok=True)
                # Create new checkpoint file
                try:
                    with open(local_checkpoint_file, 'w') as f:
                        json.dump(default_data, f, indent=2)
                except Exception as e:
                    logger.error(f"Error creating checkpoint: {e}")
                return default_data

    def _save_checkpoint(self) -> bool:
        """
        Save the checkpoint data to file.

        Returns:
            Boolean indicating success
        """
        # Update timestamp
        self._data['last_updated'] = datetime.now().isoformat()

        try:
            if self.storage:
                # Remote storage (e.g., S3)
                return self.storage.write(
                    self.checkpoint_file,
                    json.dumps(self._data, indent=2)
                )
            else:
                # Local file storage
                # Detect Lambda environment
                in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ

                # When in Lambda, we must prepend /tmp to the path
                lambda_tmp_prefix = '/tmp/' if in_lambda else ''
                local_checkpoint_file = f"{lambda_tmp_prefix}{self.checkpoint_file}"

                with open(local_checkpoint_file, 'w') as f:
                    json.dump(self._data, f, indent=2)
                return True
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            return False

    def update_scraping(self, date_str: str, success: bool = True, games_count: int = 0, force: bool = False) -> bool:
        """
        Update scraping status for a specific date.

        Args:
            date_str: Date string in format YYYY-MM-DD
            success: Whether scraping was successful
            games_count: Number of games scraped
            force: Force update even if date already exists

        Returns:
            Boolean indicating success
        """
        try:
            # Make sure the structure exists
            if 'scraping' not in self._data:
                self._data['scraping'] = {'completed_dates': {}}
            if 'completed_dates' not in self._data['scraping']:
                self._data['scraping']['completed_dates'] = {}

            # Update timestamp
            self._data['scraping']['last_updated'] = datetime.now().isoformat()

            # Check if date already exists and we're not forcing
            if not force and date_str in self._data['scraping']['completed_dates']:
                existing = self._data['scraping']['completed_dates'][date_str]
                logger.info(f"Date {date_str} already exists in checkpoint with status {existing.get('status')} and games_count {existing.get('games_count')}. Not updating.")
                return True

            # Update date status
            self._data['scraping']['completed_dates'][date_str] = {
                'status': 'success' if success else 'failed',
                'games_count': games_count,
                'timestamp': datetime.now().isoformat()
            }

            # Save the checkpoint
            save_result = self._save_checkpoint()
            if save_result:
                logger.info(f"Successfully updated checkpoint for {date_str} with status {'success' if success else 'failed'} and games_count {games_count}")
            else:
                logger.error(f"Failed to save checkpoint after updating {date_str}")

            return save_result
        except Exception as e:
            logger.error(f"Error updating scraping status: {e}")
            import traceback
            traceback.print_exc()
            return False

    def update_processing(self, date_str: str, success: bool = True) -> bool:
        """
        Update processing status for a specific date.

        Args:
            date_str: Date string in format YYYY-MM-DD
            success: Whether processing was successful

        Returns:
            Boolean indicating success
        """
        try:
            # Make sure the structure exists
            if 'processing' not in self._data:
                self._data['processing'] = {'completed_dates': {}}
            if 'completed_dates' not in self._data['processing']:
                self._data['processing']['completed_dates'] = {}

            # Update timestamp
            self._data['processing']['last_updated'] = datetime.now().isoformat()

            # Update date status
            self._data['processing']['completed_dates'][date_str] = {
                'status': 'success' if success else 'failed',
                'timestamp': datetime.now().isoformat()
            }

            return self._save_checkpoint()
        except Exception as e:
            logger.error(f"Error updating processing status: {e}")
            return False

    def update_parquet_conversion(self, status: str, version: Optional[str] = None) -> bool:
        """
        Update parquet conversion status.

        Args:
            status: Status string (e.g., 'success', 'failed', 'in_progress')
            version: Optional version identifier for the dataset

        Returns:
            Boolean indicating success
        """
        try:
            # Make sure the structure exists
            if 'parquet_conversion' not in self._data:
                self._data['parquet_conversion'] = {}

            # Update timestamp
            self._data['parquet_conversion']['last_updated'] = datetime.now().isoformat()

            # Update status
            self._data['parquet_conversion']['status'] = status

            # Update version if provided
            if version:
                self._data['parquet_conversion']['version'] = version

            return self._save_checkpoint()
        except Exception as e:
            logger.error(f"Error updating parquet conversion status: {e}")
            return False

    def is_date_scraped(self, date_str: str) -> bool:
        """
        Check if a date has been successfully scraped.

        Args:
            date_str: Date string in format YYYY-MM-DD

        Returns:
            Boolean indicating if date was scraped successfully
        """
        try:
            # Check if scraping data exists
            if 'scraping' not in self._data:
                return False
            if 'completed_dates' not in self._data['scraping']:
                return False

            # Check if date exists and was successful
            if date_str in self._data['scraping']['completed_dates']:
                entry = self._data['scraping']['completed_dates'][date_str]
                return entry.get('status') == 'success'

            return False
        except Exception as e:
            logger.error(f"Error checking if date was scraped: {e}")
            return False

    def is_date_processed(self, date_str: str) -> bool:
        """
        Check if a date has been successfully processed.

        Args:
            date_str: Date string in format YYYY-MM-DD

        Returns:
            Boolean indicating if date was processed successfully
        """
        try:
            # Check if processing data exists
            if 'processing' not in self._data:
                return False
            if 'completed_dates' not in self._data['processing']:
                return False

            # Check if date exists and was successful
            if date_str in self._data['processing']['completed_dates']:
                entry = self._data['processing']['completed_dates'][date_str]
                return entry.get('status') == 'success'

            return False
        except Exception as e:
            logger.error(f"Error checking if date was processed: {e}")
            return False

    def get_unprocessed_dates(self) -> List[str]:
        """
        Get a list of dates that have been scraped but not processed.

        Returns:
            List of date strings in format YYYY-MM-DD
        """
        try:
            unprocessed = []

            # Get all scraped dates
            if 'scraping' in self._data and 'completed_dates' in self._data['scraping']:
                # Find successfully scraped dates
                for date_str, entry in self._data['scraping']['completed_dates'].items():
                    if entry.get('status') == 'success':
                        # Check if date has been processed
                        if not self.is_date_processed(date_str):
                            unprocessed.append(date_str)

            # Sort by date
            unprocessed.sort()
            return unprocessed
        except Exception as e:
            logger.error(f"Error getting unprocessed dates: {e}")
            return []

    def get_parquet_conversion_status(self) -> Dict[str, Any]:
        """
        Get the current parquet conversion status.

        Returns:
            Dictionary with status information
        """
        try:
            if 'parquet_conversion' in self._data:
                return self._data['parquet_conversion']
            return {
                'status': 'unknown',
                'version': None,
                'last_updated': None
            }
        except Exception as e:
            logger.error(f"Error getting parquet conversion status: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

    def get_checkpoint_data(self) -> Dict[str, Any]:
        """
        Get the current checkpoint data structure.

        Returns:
            Dictionary containing the checkpoint data, including a cleaned list of completed_dates
        """
        try:
            # Create a simplified version for easier access to the completed dates
            result = {
                'version': self._data.get('version', 'unknown'),
                'last_updated': self._data.get('last_updated', 'unknown')
            }

            # Extract completed_dates from scraping
            if 'scraping' in self._data and 'completed_dates' in self._data['scraping']:
                # Convert the completed_dates dict to a simple list of successful dates
                completed_dates = []
                for date_str, entry in self._data['scraping']['completed_dates'].items():
                    if entry.get('status') == 'success':
                        completed_dates.append(date_str)

                # Sort dates
                completed_dates.sort()
                result['completed_dates'] = completed_dates
                result['total_dates_scraped'] = len(completed_dates)
            else:
                result['completed_dates'] = []
                result['total_dates_scraped'] = 0

            return result
        except Exception as e:
            logger.error(f"Error getting checkpoint data: {e}")
            import traceback
            traceback.print_exc()
            return {'error': str(e), 'completed_dates': []}

def get_checkpoint_manager(checkpoint_file: str, storage_interface=None) -> UnifiedCheckpoint:
    """
    Factory function to create a checkpoint manager.

    Args:
        checkpoint_file: Path to the checkpoint file
        storage_interface: Optional storage interface for S3 or other remote storage

    Returns:
        UnifiedCheckpoint instance
    """
    return UnifiedCheckpoint(checkpoint_file, storage_interface)