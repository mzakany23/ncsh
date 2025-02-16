from dataclasses import dataclass
from typing import Optional
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

@dataclass
class ScraperConfig:
    mode: ScrapeMode
    start_date: datetime
    storage_type: StorageType = StorageType.S3
    skip_existing: bool = True
    bucket_name: str = os.environ.get('DATA_BUCKET', 'ncsh-app-data')

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

@dataclass
class PipelineConfig:
    run_scraper: bool = True
    run_parser: bool = True
    run_validator: bool = True
    scraper_config: Optional[ScraperConfig] = None

class StorageInterface:
    def exists(self, path: str) -> bool:
        raise NotImplementedError

    def write(self, path: str, content: str) -> bool:
        raise NotImplementedError

    def read(self, path: str) -> str:
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
    def __init__(self, bucket_name: str):
        self.s3 = boto3.client('s3')
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

def get_storage_interface(storage_type: StorageType, bucket_name: str = None) -> StorageInterface:
    if storage_type == StorageType.FILE:
        return FileStorage()
    elif storage_type == StorageType.S3:
        if not bucket_name:
            bucket_name = os.environ.get('DATA_BUCKET', 'ncsh-app-data')
        return S3Storage(bucket_name)
    raise ValueError(f"Unsupported storage type: {storage_type}")

def create_scraper_config(
    mode: str,
    year: int,
    month: int,
    day: Optional[int] = None,
    skip_existing: bool = True,
    storage_type: str = "s3",
    bucket_name: str = None
) -> ScraperConfig:
    """Create a scraper configuration from command line arguments"""
    mode = ScrapeMode(mode.lower())
    storage = StorageType(storage_type.lower())

    if day:
        start_date = datetime(year, month, day)
    else:
        start_date = datetime(year, month, 1)

    return ScraperConfig(
        mode=mode,
        start_date=start_date,
        storage_type=storage,
        skip_existing=skip_existing,
        bucket_name=bucket_name
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