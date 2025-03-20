from abc import ABC, abstractmethod
import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import os

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

class DynamoDBLookup(Lookup):
    """DynamoDB implementation of the lookup interface"""

    def __init__(self, table_name: str = None, region: str = "us-east-2", **kwargs):
        """Initialize DynamoDB lookup

        Args:
            table_name (str): Name of the DynamoDB table
            region (str): AWS region name
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name or os.environ.get('DYNAMODB_TABLE', 'ncsh-scraped-dates'))

    def is_date_scraped(self, date_str: str) -> bool:
        try:
            response = self.table.get_item(Key={'date': date_str})
            if 'Item' in response:
                return response['Item'].get('success', False)
            return False
        except ClientError as e:
            print(f"Error checking date {date_str}: {str(e)}")
            return False

    def update_date(self, date_str: str, success: bool = True, games_count: int = 0) -> None:
        try:
            self.table.put_item(
                Item={
                    'date': date_str,
                    'success': success,
                    'games_count': games_count,
                    'timestamp': datetime.now().isoformat()
                }
            )
        except ClientError as e:
            print(f"Error updating date {date_str}: {str(e)}")

class LocalFileLookup(Lookup):
    """Local file implementation of the lookup interface"""

    def __init__(self, lookup_file: str = 'data/lookup.json', **kwargs):
        """Initialize local file lookup

        Args:
            lookup_file (str): Path to the lookup JSON file
            **kwargs: Additional arguments (ignored, for compatibility)
        """
        self.lookup_file = lookup_file
        self.scraped_dates = self._load_lookup_data()

    def _load_lookup_data(self):
        """Load lookup data from file"""
        if not os.path.exists(self.lookup_file):
            os.makedirs(os.path.dirname(self.lookup_file), exist_ok=True)
            with open(self.lookup_file, 'w') as f:
                json.dump({'scraped_dates': {}}, f)
            return {}

        try:
            with open(self.lookup_file, 'r') as f:
                data = json.load(f)
                return data.get('scraped_dates', {})
        except Exception as e:
            print(f"Error loading lookup file: {e}")
            return {}

    def _save_lookup_data(self):
        """Save lookup data to file"""
        try:
            with open(self.lookup_file, 'w') as f:
                json.dump({'scraped_dates': self.scraped_dates}, f, indent=2)
        except Exception as e:
            print(f"Failed to save lookup data: {e}")

    def is_date_scraped(self, date_str: str) -> bool:
        return date_str in self.scraped_dates and self.scraped_dates[date_str]['success']

    def update_date(self, date_str: str, success: bool = True, games_count: int = 0) -> None:
        self.scraped_dates[date_str] = {
            'success': success,
            'games_count': games_count,
            'timestamp': datetime.now().isoformat()
        }
        self._save_lookup_data()

def get_lookup_interface(lookup_type: str = 'file', **kwargs) -> Lookup:
    """Factory function to get the appropriate lookup interface

    Args:
        lookup_type (str, optional): Type of lookup to use ('file' or 'dynamodb'). Defaults to 'file'.
        **kwargs: Additional arguments to pass to the lookup interface:
            - lookup_file (str): Path to lookup file (for file lookup)
            - table_name (str): DynamoDB table name (for dynamodb lookup)
            - region (str): AWS region (for dynamodb lookup)

    Returns:
        Lookup: The configured lookup interface

    Raises:
        ValueError: If an unsupported lookup type is specified
    """
    if lookup_type == 'dynamodb':
        return DynamoDBLookup(**kwargs)
    elif lookup_type == 'file':
        return LocalFileLookup(**kwargs)
    else:
        raise ValueError(f"Unsupported lookup type: {lookup_type}")