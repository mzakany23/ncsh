from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timedelta
from enum import Enum

class ScrapeMode(Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

class StorageType(Enum):
    FILE = "file"
    # Add other storage types here (e.g., DATABASE = "database")

@dataclass
class ScraperConfig:
    mode: ScrapeMode
    start_date: datetime
    storage_type: StorageType = StorageType.FILE
    skip_existing: bool = True

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

class FileStorage(StorageInterface):
    def exists(self, path: str) -> bool:
        import os
        return os.path.exists(path)

def get_storage_interface(storage_type: StorageType) -> StorageInterface:
    if storage_type == StorageType.FILE:
        return FileStorage()
    raise ValueError(f"Unsupported storage type: {storage_type}")

def create_scraper_config(
    mode: str,
    year: int,
    month: int,
    day: Optional[int] = None,
    skip_existing: bool = True,
    storage_type: str = "file"
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
        skip_existing=skip_existing
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