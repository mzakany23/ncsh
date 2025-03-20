from dataclasses import dataclass
from typing import Optional, Union
from datetime import datetime, timedelta
from enum import Enum
import os
import boto3

class ScrapeMode(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

class StorageType(Enum):
    FILE = "file"
    S3 = "s3"
    # Add other storage types here (e.g., DATABASE = "database")

class DataArchitectureVersion(Enum):
    V1 = "v1"  # Legacy architecture
    V2 = "v2"  # New partitioned architecture

@dataclass
class ScraperConfig:
    mode: ScrapeMode
    start_date: datetime
    storage_type: StorageType = StorageType.S3
    skip_existing: bool = True
    bucket_name: str = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
    architecture_version: DataArchitectureVersion = DataArchitectureVersion.V1

    @property
    def end_date(self) -> datetime:
        if self.mode == ScrapeMode.DAY:
            return self.start_date
        elif self.mode == ScrapeMode.WEEK:
            return self.start_date + timedelta(days=6)
        else:  # MONTH
            if self.start_date.month == 12:
                return datetime(self.start_date.year + 1, 1, 1) - timedelta(days=1)
            else:
                return datetime(self.start_date.year, self.start_date.month + 1, 1) - timedelta(days=1)

class DataPathManager:
    """
    Manages path construction for different data architecture versions.
    This handles the transition from the legacy architecture to the new partitioned structure.
    """

    def __init__(self, architecture_version=DataArchitectureVersion.V1, base_prefix=""):
        """
        Initialize the path manager.

        Args:
            architecture_version: Which architecture version to use (V1 or V2)
            base_prefix: Optional base prefix to prepend to all paths
        """
        self.architecture_version = architecture_version
        if isinstance(architecture_version, str):
            self.architecture_version = DataArchitectureVersion(architecture_version.lower())

        self.base_prefix = base_prefix

    def get_html_path(self, date_obj):
        """
        Get the path for storing HTML content.

        Args:
            date_obj: datetime object for the date

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            return os.path.join(self.base_prefix, 'data/html', f"{date_obj.strftime('%Y-%m-%d')}.html")
        else:  # V2
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(
                self.base_prefix,
                'v2/raw/html',
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}",
                f"{date_obj.strftime('%Y-%m-%d')}.html"
            )

    def get_json_meta_path(self, date_obj):
        """
        Get the path for storing JSON metadata.

        Args:
            date_obj: datetime object for the date

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            return os.path.join(self.base_prefix, 'data/json', f"{date_obj.strftime('%Y-%m-%d')}_meta.json")
        else:  # V2
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(
                self.base_prefix,
                'v2/processed/json',
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}",
                f"{date_obj.strftime('%Y-%m-%d')}_meta.json"
            )

    def get_games_path(self, date_obj):
        """
        Get the path for storing game data.

        Args:
            date_obj: datetime object for the date

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(self.base_prefix, 'data/games', f"year={year}", f"month={month:02d}", f"day={day:02d}", "data.jsonl")
        else:  # V2
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(
                self.base_prefix,
                'v2/processed/json',
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}",
                "games.jsonl"
            )

    def get_metadata_path(self, date_obj):
        """
        Get the path for storing additional metadata.

        Args:
            date_obj: datetime object for the date

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(self.base_prefix, 'data/metadata', f"year={year}", f"month={month:02d}", f"day={day:02d}", "data.jsonl")
        else:  # V2
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            return os.path.join(
                self.base_prefix,
                'v2/processed/json',
                f"year={year}",
                f"month={month:02d}",
                f"day={day:02d}",
                "metadata.jsonl"
            )

    def get_checkpoint_path(self):
        """
        Get the path for the checkpoint file.

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            return os.path.join(self.base_prefix, 'data/checkpoints/html_processing.json')
        else:  # V2
            return os.path.join(self.base_prefix, 'v2/control/checkpoints.json')

    def get_parquet_path(self, version=None):
        """
        Get the path for parquet dataset.

        Args:
            version: Optional version identifier

        Returns:
            Path string
        """
        if self.architecture_version == DataArchitectureVersion.V1:
            if version:
                return os.path.join(self.base_prefix, 'data/parquet', f"ncsoccer_games_{version}.parquet")
            else:
                return os.path.join(self.base_prefix, 'data/parquet', "ncsoccer_games_latest.parquet")
        else:  # V2
            if version:
                return os.path.join(self.base_prefix, 'v2/analytical/parquet', f"ncsoccer_games_{version}.parquet")
            else:
                return os.path.join(self.base_prefix, 'v2/analytical/parquet', "ncsoccer_games_latest.parquet")

class StorageInterface:
    """Abstract base class for storage implementations"""

    def exists(self, path: str) -> bool:
        """Check if a path exists"""
        raise NotImplementedError

    def write(self, path: str, content: str) -> bool:
        """Write content to a path"""
        raise NotImplementedError

    def read(self, path: str) -> str:
        """Read content from a path"""
        raise NotImplementedError

class FileStorage(StorageInterface):
    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def write(self, path: str, content: str) -> bool:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception:
            return False

    def read(self, path: str) -> str:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

class S3Storage(StorageInterface):
    def __init__(self, bucket_name: str, region: str = "us-east-2"):
        self.s3 = boto3.client('s3', region_name=region)
        self.bucket = bucket_name

    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return True
        except:
            return False

    def write(self, path: str, content: str) -> bool:
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=content.encode('utf-8'),
                ContentType='text/html' if path.endswith('.html') else 'application/json'
            )
            return True
        except Exception:
            return False

    def read(self, path: str) -> str:
        response = self.s3.get_object(Bucket=self.bucket, Key=path)
        return response['Body'].read().decode('utf-8')

def get_storage_interface(storage_type: str | StorageType, bucket_name: str = None, region: str = "us-east-2") -> StorageInterface:
    """Get the appropriate storage interface based on type

    Args:
        storage_type (Union[str, StorageType]): Type of storage to use ('file' or 's3')
        bucket_name (str, optional): Name of S3 bucket for S3 storage. Defaults to None.
        region (str, optional): AWS region for S3 storage. Defaults to "us-east-2".

    Returns:
        StorageInterface: The configured storage interface
    """
    # Convert string to StorageType if needed
    if isinstance(storage_type, str):
        storage_type = StorageType(storage_type.lower())

    if storage_type == StorageType.FILE:
        return FileStorage()
    elif storage_type == StorageType.S3:
        if not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        return S3Storage(bucket_name, region=region)
    raise ValueError(f"Unsupported storage type: {storage_type}")

@dataclass
class PipelineConfig:
    """Pipeline configuration for running the data pipeline"""
    run_scraper: bool = True
    run_parser: bool = True
    run_validator: bool = True
    scraper_config: Optional[ScraperConfig] = None

def create_scraper_config(
    mode: str,
    year: int,
    month: int,
    day: Optional[int] = None,
    skip_existing: bool = True,
    storage_type: str = "s3",
    bucket_name: str = None,
    architecture_version: str = "v1"
) -> ScraperConfig:
    """Create a scraper configuration from command line arguments"""
    mode = ScrapeMode(mode.lower())
    storage = StorageType(storage_type.lower())
    architecture = DataArchitectureVersion(architecture_version.lower())

    if day:
        start_date = datetime(year, month, day)
    else:
        start_date = datetime(year, month, 1)

    return ScraperConfig(
        mode=mode,
        start_date=start_date,
        storage_type=storage,
        skip_existing=skip_existing,
        bucket_name=bucket_name,
        architecture_version=architecture
    )

def create_pipeline_config(
    scraper_config: Optional[ScraperConfig] = None,
    run_scraper: bool = True,
    run_parser: bool = True,
    run_validator: bool = True
) -> PipelineConfig:
    """Create a pipeline configuration"""
    return PipelineConfig(
        run_scraper=run_scraper,
        run_parser=run_parser,
        run_validator=run_validator,
        scraper_config=scraper_config
    )