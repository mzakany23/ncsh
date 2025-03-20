from abc import ABC, abstractmethod
import json
from datetime import datetime
import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class Lookup(ABC):
    """Base interface for lookup implementations"""

    @abstractmethod
    def is_date_scraped(self, date_str: str) -> bool:
        """Check if a date has been scraped successfully"""
        pass

    @abstractmethod
    def update_date(self, date_str: str, success: bool = True, games_count: int = 0) -> None:
        """Update the lookup data for a date"""
        pass

class LocalFileLookup(Lookup):
    """Local file implementation of the lookup interface"""

    def __init__(self, lookup_file: str = 'data/lookup.json', architecture_version: str = 'v1', **kwargs):
        """Initialize local file lookup

        Args:
            lookup_file (str): Path to the lookup JSON file
            architecture_version (str): 'v1' for legacy or 'v2' for new architecture
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.lookup_file = lookup_file
        self.architecture_version = architecture_version
        self.scraped_dates = self._load_lookup_data()

    def _load_lookup_data(self) -> Dict[str, Any]:
        """Load lookup data from file"""
        if not os.path.exists(self.lookup_file):
            os.makedirs(os.path.dirname(self.lookup_file), exist_ok=True)

            # Create different initial structure based on architecture version
            if self.architecture_version == 'v2':
                initial_data = {
                    'version': 'v2',
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
            else:
                # Legacy v1 structure
                initial_data = {'scraped_dates': {}}

            with open(self.lookup_file, 'w') as f:
                json.dump(initial_data, f, indent=2)

            # For v2, return empty dict for backward compatibility with existing code
            if self.architecture_version == 'v2':
                return {}
            else:
                return {}

        try:
            with open(self.lookup_file, 'r') as f:
                data = json.load(f)

                # Handle different structures based on architecture version
                if self.architecture_version == 'v2':
                    # For compatibility with existing code, convert the v2 structure to v1-like format
                    # This allows v1 code to work with v2 data structure
                    if 'scraping' in data and 'completed_dates' in data['scraping']:
                        completed_dates = {}
                        for date_str, info in data['scraping']['completed_dates'].items():
                            completed_dates[date_str] = {
                                'success': info.get('status') == 'success',
                                'games_count': info.get('games_count', 0),
                                'timestamp': info.get('timestamp')
                            }
                        return completed_dates
                    return {}
                else:
                    # Legacy v1 structure
                    return data.get('scraped_dates', {})
        except Exception as e:
            logger.error(f"Error loading lookup file: {e}")
            return {}

    def _save_lookup_data(self) -> None:
        """Save lookup data to file"""
        try:
            if self.architecture_version == 'v2':
                # Load existing data first to preserve other sections
                if os.path.exists(self.lookup_file):
                    with open(self.lookup_file, 'r') as f:
                        data = json.load(f)
                else:
                    data = {
                        'version': 'v2',
                        'scraping': {'completed_dates': {}},
                        'processing': {'completed_dates': {}},
                        'parquet_conversion': {'status': 'initialized'}
                    }

                # Convert v1-like format back to v2 structure
                completed_dates = {}
                for date_str, info in self.scraped_dates.items():
                    completed_dates[date_str] = {
                        'status': 'success' if info.get('success', False) else 'failed',
                        'games_count': info.get('games_count', 0),
                        'timestamp': info.get('timestamp', datetime.now().isoformat())
                    }

                data['scraping'] = {
                    'last_updated': datetime.now().isoformat(),
                    'completed_dates': completed_dates
                }
            else:
                # Legacy v1 structure
                data = {'scraped_dates': self.scraped_dates}

            with open(self.lookup_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save lookup data: {e}")

    def is_date_scraped(self, date_str: str) -> bool:
        """Check if a date has been successfully scraped

        Args:
            date_str: Date string in YYYY-MM-DD format

        Returns:
            bool: True if date has been successfully scraped, False otherwise
        """
        return date_str in self.scraped_dates and self.scraped_dates[date_str]['success']

    def update_date(self, date_str: str, success: bool = True, games_count: int = 0) -> None:
        """Update status for a date

        Args:
            date_str: Date string in YYYY-MM-DD format
            success: Whether scraping was successful
            games_count: Number of games scraped
        """
        self.scraped_dates[date_str] = {
            'success': success,
            'games_count': games_count,
            'timestamp': datetime.now().isoformat()
        }
        self._save_lookup_data()

    def update_processing_status(self, date_str: str, success: bool = True) -> None:
        """Update processing status for a date (v2 only)

        Args:
            date_str: Date string in YYYY-MM-DD format
            success: Whether processing was successful
        """
        if self.architecture_version != 'v2':
            return

        try:
            with open(self.lookup_file, 'r') as f:
                data = json.load(f)

            if 'processing' not in data:
                data['processing'] = {'completed_dates': {}}

            if 'completed_dates' not in data['processing']:
                data['processing']['completed_dates'] = {}

            data['processing']['last_updated'] = datetime.now().isoformat()
            data['processing']['completed_dates'][date_str] = {
                'status': 'success' if success else 'failed',
                'timestamp': datetime.now().isoformat()
            }

            with open(self.lookup_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to update processing status: {e}")

    def update_parquet_conversion(self, status: str, version: Optional[str] = None) -> None:
        """Update parquet conversion status (v2 only)

        Args:
            status: Status string (e.g., 'success', 'failed', 'in_progress')
            version: Optional version identifier for the dataset
        """
        if self.architecture_version != 'v2':
            return

        try:
            with open(self.lookup_file, 'r') as f:
                data = json.load(f)

            if 'parquet_conversion' not in data:
                data['parquet_conversion'] = {}

            data['parquet_conversion']['last_updated'] = datetime.now().isoformat()
            data['parquet_conversion']['status'] = status

            if version:
                data['parquet_conversion']['version'] = version

            with open(self.lookup_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to update parquet conversion status: {e}")

def get_lookup_interface(lookup_type: str = 'file', architecture_version: str = 'v1', **kwargs) -> Lookup:
    """Factory function to get the appropriate lookup interface

    Args:
        lookup_type (str, optional): Type of lookup to use. Only 'file' is supported. Defaults to 'file'.
        architecture_version (str, optional): 'v1' for legacy or 'v2' for new architecture. Defaults to 'v1'.
        **kwargs: Additional arguments to pass to the lookup interface:
            - lookup_file (str): Path to lookup file (for file lookup)

    Returns:
        Lookup: The configured lookup interface

    Raises:
        ValueError: If an unsupported lookup type is specified
    """
    if lookup_type == 'file':
        kwargs['architecture_version'] = architecture_version
        return LocalFileLookup(**kwargs)
    else:
        raise ValueError(f"Unsupported lookup type: {lookup_type}. Only 'file' is supported.")