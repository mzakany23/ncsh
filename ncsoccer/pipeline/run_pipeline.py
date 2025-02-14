import os
import sys
import logging
import subprocess
from datetime import datetime
from html_to_json import HTMLParser
from validate_json import GameValidator
from config import (
    create_scraper_config,
    create_pipeline_config,
    ScraperConfig,
    PipelineConfig,
    ScrapeMode
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def run_spider(self) -> bool:
        """Run the Scrapy spider to collect HTML"""
        if not self.config.scraper_config:
            logger.error("No scraper configuration provided")
            return False

        logger.info(f"Starting spider for {self.config.scraper_config.start_date}")
        try:
            cmd = [
                'scrapy', 'crawl', 'schedule',
                '-a', f'mode={self.config.scraper_config.mode.value}',
                '-a', f'year={self.config.scraper_config.start_date.year}',
                '-a', f'month={self.config.scraper_config.start_date.month}',
                '-a', f'day={self.config.scraper_config.start_date.day}',
                '-a', f'skip_existing={self.config.scraper_config.skip_existing}'
            ]

            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("Spider completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Spider failed: {e}")
            logger.error(f"Output: {e.output}")
            return False

    def parse_html(self) -> bool:
        """Parse HTML files to JSON"""
        if not self.config.scraper_config:
            logger.error("No scraper configuration provided")
            return False

        logger.info(f"Starting HTML parsing for {self.config.scraper_config.start_date}")
        try:
            parser = HTMLParser(
                self.config.scraper_config.start_date.year,
                self.config.scraper_config.start_date.month
            )
            total_games = parser.process_month()
            logger.info(f"Parsed {total_games} games")
            return True
        except Exception as e:
            logger.error(f"HTML parsing failed: {e}")
            return False

    def validate_data(self) -> bool:
        """Validate parsed JSON data"""
        if not self.config.scraper_config:
            logger.error("No scraper configuration provided")
            return False

        logger.info(f"Starting validation for {self.config.scraper_config.start_date}")
        try:
            validator = GameValidator(
                self.config.scraper_config.start_date.year,
                self.config.scraper_config.start_date.month
            )
            results = validator.validate_month()

            # Check if we have significant validation errors
            error_rate = 1 - (results['total_valid_games'] / results['total_games']) if results['total_games'] > 0 else 1
            if error_rate > 0.1:  # More than 10% error rate
                logger.warning(f"High error rate: {error_rate:.2%}")
                return False

            return True
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False

    def run(self) -> bool:
        """Run the complete pipeline"""
        start_time = datetime.now()
        logger.info(f"Starting pipeline with configuration: {self.config}")

        if self.config.run_scraper:
            if not self.run_spider():
                logger.error("Pipeline failed at spider stage")
                return False

        if self.config.run_parser:
            if not self.parse_html():
                logger.error("Pipeline failed at HTML parsing stage")
                return False

        if self.config.run_validator:
            if not self.validate_data():
                logger.error("Pipeline failed at validation stage")
                return False

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Pipeline completed successfully in {duration}")
        return True

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run the soccer schedule pipeline')
    parser.add_argument('mode', choices=['day', 'week', 'month'], help='Scraping mode')
    parser.add_argument('year', type=int, help='Year to scrape')
    parser.add_argument('month', type=int, help='Month to scrape')
    parser.add_argument('--day', type=int, help='Day to scrape (required for day mode)')
    parser.add_argument('--skip-existing', action='store_true', help='Skip existing files')
    parser.add_argument('--skip-scraper', action='store_true', help='Skip scraping stage')
    parser.add_argument('--skip-parser', action='store_true', help='Skip parsing stage')
    parser.add_argument('--skip-validator', action='store_true', help='Skip validation stage')

    args = parser.parse_args()

    if args.mode == 'day' and not args.day:
        parser.error("Day is required when mode is 'day'")

    if not (1 <= args.month <= 12):
        parser.error("Month must be between 1 and 12")

    if args.day and not (1 <= args.day <= 31):
        parser.error("Day must be between 1 and 31")

    # Create configurations
    scraper_config = create_scraper_config(
        mode=args.mode,
        year=args.year,
        month=args.month,
        day=args.day,
        skip_existing=args.skip_existing
    )

    pipeline_config = create_pipeline_config(
        scraper_config=scraper_config,
        run_scraper=not args.skip_scraper,
        run_parser=not args.skip_parser,
        run_validator=not args.skip_validator
    )

    # Run pipeline
    pipeline = Pipeline(pipeline_config)
    success = pipeline.run()
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()