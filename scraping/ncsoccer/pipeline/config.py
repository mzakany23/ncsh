from dataclasses import dataclass
from typing import Optional, Union
from datetime import datetime, timedelta
from enum import Enum
import os
import boto3
import logging
import time

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
    V2 = "v2"  # Partitioned architecture

@dataclass
class ScraperConfig:
    mode: ScrapeMode
    start_date: datetime
    storage_type: StorageType = StorageType.S3
    skip_existing: bool = True
    bucket_name: str = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
    architecture_version: DataArchitectureVersion = DataArchitectureVersion.V2

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

    def __init__(self, architecture_version=DataArchitectureVersion.V2, base_prefix=""):
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

        # Detect Lambda environment to adjust paths if needed
        self.in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        self.logger = logging.getLogger(__name__)

        # For v1 architecture in Lambda, we should use /tmp
        # For v2 architecture, we should always use S3 paths that don't need /tmp
        if self.in_lambda:
            # Make this more robust to handle both string and enum values
            arch_version_str = self.architecture_version.value if hasattr(self.architecture_version, 'value') else str(self.architecture_version).lower()
            
            if arch_version_str == 'v1':
                # Only adjust paths for v1 architecture
                if not self.base_prefix.startswith('/tmp/') and not self.base_prefix.startswith('s3://'):
                    self.logger.warning(f"In Lambda with v1 architecture - adjusting base_prefix to use /tmp")
                    self.base_prefix = f"/tmp/{self.base_prefix}" if self.base_prefix else "/tmp"
            elif arch_version_str == 'v2':
                # For v2, ensure we're NOT using /tmp - data should go directly to S3
                if self.base_prefix and self.base_prefix.startswith('/tmp/'):
                    self.logger.warning(f"In Lambda with v2 architecture - removing /tmp prefix to ensure S3 storage")
                    self.base_prefix = self.base_prefix.replace('/tmp/', '')

    def get_html_path(self, date_obj):
        """
        Get the path to the HTML file for a given date.

        Args:
            date_obj: A datetime object representing the date

        Returns:
            Path string
        """
        # Only v2 architecture is supported now
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        path = os.path.join(
            self.base_prefix,
            'v2/raw/html',
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}",
            f"{date_obj.strftime('%Y-%m-%d')}.html"
        )
        return path

    def get_json_meta_path(self, date_obj):
        """
        Get the path to the JSON metadata file for a given date.

        Args:
            date_obj: A datetime object representing the date

        Returns:
            Path string
        """
        # Only v2 architecture is supported now
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        path = os.path.join(
            self.base_prefix,
            'v2/processed/json',
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}",
            f"{date_obj.strftime('%Y-%m-%d')}_meta.json"
        )
        return path

    def get_games_path(self, date_obj):
        """
        Get the path to the games JSON file for a given date.

        Args:
            date_obj: A datetime object representing the date

        Returns:
            Path string
        """
        # Only v2 architecture is supported now
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        path = os.path.join(
            self.base_prefix,
            'v2/processed/json',
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}",
            f"{date_obj.strftime('%Y-%m-%d')}_games.jsonl"
        )
        return path

    def get_metadata_path(self, date_obj):
        """
        Get the path to the JSON metadata file for a given date.
        This is an alias for get_json_meta_path for backward compatibility.

        Args:
            date_obj: A datetime object representing the date

        Returns:
            Path string
        """
        return self.get_json_meta_path(date_obj)

    def get_checkpoint_path(self):
        """
        Get the path to the checkpoint file.

        Returns:
            Path string
        """
        # Only v2 architecture is supported now
        path = os.path.join(self.base_prefix, 'v2/metadata/checkpoints/html_processing.json')
        return path

    def get_parquet_path(self, version=None):
        """
        Get the path to the Parquet file.

        Args:
            version: Optional version string for the Parquet file

        Returns:
            Path string
        """
        # Only v2 architecture is supported now
        if version:
            path = os.path.join(self.base_prefix, 'v2/processed/parquet', version, "data.parquet")
        else:
            path = os.path.join(self.base_prefix, 'v2/processed/parquet', "data.parquet")
        return path

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
    def __init__(self):
        """Initialize the FileStorage interface"""
        # Detect Lambda environment
        self.in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ
        self.tmp_prefix = '/tmp/' if self.in_lambda else ''
        self.logger = logging.getLogger(__name__)

        if self.in_lambda:
            self.logger.warning(
                "USING FILESYSTEM STORAGE IN LAMBDA IS STRONGLY DISCOURAGED. "
                "Lambda has limited /tmp space and files don't persist between invocations. "
                "Please use S3Storage instead.")

    def exists(self, path: str) -> bool:
        # Warn about Lambda usage
        if self.in_lambda:
            self.logger.warning(f"FileStorage.exists called in Lambda environment. Use S3Storage instead: {path}")

        # Use /tmp prefix in Lambda
        local_path = f"{self.tmp_prefix}{path}"
        return os.path.exists(local_path)

    def write(self, path: str, content: str) -> bool:
        try:
            # Warn about Lambda usage
            if self.in_lambda:
                self.logger.warning(f"FileStorage.write called in Lambda environment. Use S3Storage instead: {path}")

            # Use /tmp prefix in Lambda
            local_path = f"{self.tmp_prefix}{path}"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        except Exception as e:
            self.logger.error(f"FileStorage: Error writing to {path}: {str(e)}")
            return False

    def read(self, path: str) -> str:
        # Warn about Lambda usage
        if self.in_lambda:
            self.logger.warning(f"FileStorage.read called in Lambda environment. Use S3Storage instead: {path}")

        # Use /tmp prefix in Lambda
        local_path = f"{self.tmp_prefix}{path}"
        with open(local_path, 'r', encoding='utf-8') as f:
            return f.read()

class S3Storage(StorageInterface):
    def __init__(self, bucket_name: str, region: str = "us-east-2"):
        self.s3 = boto3.client('s3', region_name=region)
        self.bucket = bucket_name
        self.logger = logging.getLogger(__name__)

    def exists(self, path: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=path)
            return True
        except:
            return False

    def write(self, path: str, content: str) -> bool:
        try:
            self.logger.info(f"S3Storage: Writing to {self.bucket}/{path} (content length: {len(content)} bytes)")
            write_start = time.time()

            self.s3.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=content.encode('utf-8'),
                ContentType='text/html' if path.endswith('.html') else 'application/json'
            )

            write_duration = time.time() - write_start
            self.logger.info(f"S3Storage: Successfully wrote to {self.bucket}/{path} in {write_duration:.2f}s")

            # Verify the file was actually written
            try:
                self.s3.head_object(Bucket=self.bucket, Key=path)
                self.logger.info(f"S3Storage: Successfully verified {self.bucket}/{path} exists after write")
                return True
            except Exception as e:
                self.logger.error(f"S3Storage: File verification failed for {self.bucket}/{path}: {str(e)}")
                return False

        except Exception as e:
            self.logger.error(f"S3Storage: Failed to write to {self.bucket}/{path}: {str(e)}")
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
    logger = logging.getLogger(__name__)
    in_lambda = 'AWS_LAMBDA_FUNCTION_NAME' in os.environ

    # Convert string to StorageType if needed
    if isinstance(storage_type, str):
        storage_type = StorageType(storage_type.lower())

    # If in Lambda environment, enforce S3 storage
    if in_lambda and storage_type == StorageType.FILE:
        logger.warning("Attempting to use file storage in Lambda environment - forcing S3 storage instead")
        storage_type = StorageType.S3

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
    architecture_version: str = "v2"
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