import scrapy
from scrapy.http import FormRequest, HtmlResponse, Request
import os
import json
import re
import logging
from datetime import datetime, timedelta
from calendar import monthrange
import time
from ..pipeline.config import (
    ScraperConfig,
    ScrapeMode,
    get_storage_interface,
    create_scraper_config
)
from ..pipeline.lookup import get_lookup_interface
from .schedule_spider import ScheduleSpider

class BackfillSpider(ScheduleSpider):
    """Spider that efficiently scrapes multiple months in a single session.
    
    This spider starts at the most recent month and navigates backward one month at a time,
    scraping each month as it goes. This is much more efficient than starting from scratch
    for each historical month.
    """
    name = 'backfill_spider'
    
    def __init__(self, start_year=None, start_month=None, end_year=None, end_month=None, 
                 skip_existing=True, checkpoint_file=None, *args, **kwargs):
        """
        Initialize the backfill spider.
        
        Args:
            start_year (int): The oldest year to scrape (lower bound)
            start_month (int): The oldest month to scrape (lower bound) 
            end_year (int): The newest year to scrape (upper bound)
            end_month (int): The newest month to scrape (upper bound)
            skip_existing (bool): Whether to skip already scraped dates
            checkpoint_file (str): Path to store checkpoint information
        """
        super(BackfillSpider, self).__init__(*args, **kwargs)
        
        # Set up date range - defaults to current month if not specified
        now = datetime.now()
        self.start_year = int(start_year) if start_year else 2007  # Default to 2007
        self.start_month = int(start_month) if start_month else 1  # Default to January
        self.end_year = int(end_year) if end_year else now.year
        self.end_month = int(end_month) if end_month else now.month
        
        # Target date tracking - we'll start with the most recent month
        self.current_target_year = self.end_year
        self.current_target_month = self.end_month
        
        # Tracking of already navigated pages
        self.last_scraped_date = None
        
        # Checkpoint tracking
        self.checkpoint_file = checkpoint_file or os.path.join(self.json_prefix, "backfill_checkpoint.json")
        self.load_checkpoint()
        
        # Statistics
        self.months_scraped = 0
        self.months_skipped = 0
        self.start_time = time.time()
        
        # Debug info
        self.logger.info(f"Backfill range: {self.start_year}-{self.start_month:02d} to {self.end_year}-{self.end_month:02d}")
        self.logger.info(f"Starting with target: {self.current_target_year}-{self.current_target_month:02d}")

    def load_checkpoint(self):
        """Load the checkpoint data if it exists."""
        try:
            if self.storage.exists(self.checkpoint_file):
                checkpoint_data = json.loads(self.storage.read(self.checkpoint_file))
                self.current_target_year = checkpoint_data.get('current_year', self.end_year)
                self.current_target_month = checkpoint_data.get('current_month', self.end_month)
                self.months_scraped = checkpoint_data.get('months_scraped', 0)
                self.months_skipped = checkpoint_data.get('months_skipped', 0)
                self.logger.info(f"Loaded checkpoint: {self.current_target_year}-{self.current_target_month:02d}, "
                               f"scraped: {self.months_scraped}, skipped: {self.months_skipped}")
                
                # Adjust for completed months
                if self.is_target_complete():
                    self.move_to_previous_month()
            else:
                self.logger.info("No checkpoint found, starting fresh")
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
    
    def save_checkpoint(self):
        """Save the current state as a checkpoint."""
        try:
            checkpoint_data = {
                'current_year': self.current_target_year,
                'current_month': self.current_target_month,
                'months_scraped': self.months_scraped,
                'months_skipped': self.months_skipped,
                'last_update': datetime.now().isoformat()
            }
            self.storage.write(
                self.checkpoint_file, 
                json.dumps(checkpoint_data, indent=2)
            )
            self.logger.info(f"Saved checkpoint: {self.current_target_year}-{self.current_target_month:02d}")
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
    
    def is_target_complete(self):
        """Check if the current target month has been fully scraped."""
        month_days = monthrange(self.current_target_year, self.current_target_month)[1]
        
        # Check if all days in the month have been scraped
        for day in range(1, month_days + 1):
            date_str = f"{self.current_target_year}-{self.current_target_month:02d}-{day:02d}"
            if not self._is_date_scraped(datetime(self.current_target_year, self.current_target_month, day)):
                return False
        return True
    
    def move_to_previous_month(self):
        """Move the target date to the previous month."""
        if self.current_target_month == 1:
            self.current_target_year -= 1
            self.current_target_month = 12
        else:
            self.current_target_month -= 1
            
        self.logger.info(f"Moving to previous month: {self.current_target_year}-{self.current_target_month:02d}")
        
        # Check if we've gone past the start date
        if (self.current_target_year < self.start_year or 
            (self.current_target_year == self.start_year and self.current_target_month < self.start_month)):
            self.logger.info("Reached start date limit, backfill complete")
            return False
        return True
    
    def is_backfill_complete(self):
        """Check if the backfill process is complete."""
        return (self.current_target_year < self.start_year or
                (self.current_target_year == self.start_year and self.current_target_month < self.start_month))
    
    def start_requests(self):
        """Start the backfill process with improved session handling."""
        # Check if we're already done
        if self.is_backfill_complete():
            self.logger.info("Backfill already complete")
            return
        
        # First, check if current month is already fully scraped
        if self.is_target_complete() and self.skip_existing:
            self.logger.info(f"Month {self.current_target_year}-{self.current_target_month:02d} already complete, skipping")
            self.months_skipped += 1
            self.save_checkpoint()
            if self.move_to_previous_month():
                # Skip to next month
                return self.start_requests()
            return
        
        # Set up cookies for the initial session
        self.logger.info("Establishing initial session with the website...")
        
        # First visit the homepage to establish a session and get cookies
        yield scrapy.Request(
            url='https://nc-soccer-hudson.ezleagues.ezfacility.com/',
            callback=self.visit_facility_page,
            meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [301, 302],
                'target_year': self.current_target_year,
                'target_month': self.current_target_month
            }
        )
        
    def visit_facility_page(self, response):
        """Visit the facility page to get necessary cookies."""
        self.logger.info("Visiting facility page to establish cookies...")
        
        # Now visit the facility page to get additional cookies
        yield scrapy.Request(
            url='https://nc-soccer-hudson.ezleagues.ezfacility.com/facilities/facilities.aspx',
            callback=self.visit_schedule_page,
            meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [301, 302],
                'target_year': response.meta.get('target_year'),
                'target_month': response.meta.get('target_month')
            }
        )
    
    def visit_schedule_page(self, response):
        """Visit the schedule page with established cookies."""
        self.logger.info("Now visiting schedule page with established session...")
        
        # Start with initial request to the schedule page
        self.logger.info(f"Starting backfill from {self.current_target_year}-{self.current_target_month:02d}")
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.handle_calendar_navigation,
            dont_filter=True,  # Important to avoid duplicate filtering
            meta={
                'dont_redirect': True, 
                'handle_httpstatus_list': [301, 302],
                'target_year': response.meta.get('target_year'),
                'target_month': response.meta.get('target_month')
            }
        )
    
    def handle_calendar_navigation(self, response):
        """Navigate to the target month and start scraping days."""
        target_year = response.meta.get('target_year', self.current_target_year)
        target_month = response.meta.get('target_month', self.current_target_month)
        
        # Validate the current page
        is_valid, current_month_date, error = self.validate_current_page(response)
        if not is_valid:
            self.logger.error(f"Page validation failed: {error}")
            return
        
        self.logger.info(f"Currently on: {current_month_date.strftime('%B %Y')}, "
                       f"Target: {target_year}-{target_month:02d}")
        
        # Update our current month tracking
        self.current_month_date = current_month_date
        
        # If we're not in the correct month, navigate
        if current_month_date.year != target_year or current_month_date.month != target_month:
            # Determine if we need to go forward or back
            months_diff = (target_year - current_month_date.year) * 12 + (target_month - current_month_date.month)
            self.logger.info(f"Need to navigate {abs(months_diff)} months {'forward' if months_diff > 0 else 'back'}")
            
            # Find the appropriate navigation button
            button_title = "Go to the previous month" if months_diff < 0 else "Go to the next month"
            nav_button = response.css(f'a[title="{button_title}"]')
            if not nav_button:
                self.logger.error(f"Could not find {button_title} button")
                return
            
            # Extract the __doPostBack arguments from the href
            href = nav_button.attrib['href']
            match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
            if not match:
                self.logger.error(f"Could not extract postback arguments from href: {href}")
                return
            
            event_target, event_argument = match.groups()
            
            # Extract ASP.NET form fields
            viewstate = response.css('#__VIEWSTATE::attr(value)').get()
            eventvalidation = response.css('#__EVENTVALIDATION::attr(value)').get()
            viewstategenerator = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
            
            # Create form data for the navigation button
            formdata = {
                '__EVENTTARGET': event_target,
                '__EVENTARGUMENT': event_argument,
                '__VIEWSTATE': viewstate,
                '__EVENTVALIDATION': eventvalidation,
                '__VIEWSTATEGENERATOR': viewstategenerator,
            }
            
            self.logger.info(f"Clicking {button_title}...")
            yield FormRequest(
                url=self.start_urls[0],
                formdata=formdata,
                callback=self.handle_calendar_navigation,
                meta={
                    'dont_redirect': True,
                    'handle_httpstatus_list': [301, 302],
                    'target_year': target_year,
                    'target_month': target_month
                },
                dont_filter=True
            )
            return
        
        # We're on the correct month, now scrape all days in the month
        self.logger.info(f"Reached target month: {target_year}-{target_month:02d}, scraping days")
        
        # Get the number of days in this month
        month_days = monthrange(target_year, target_month)[1]
        
        # Extract ASP.NET form fields for reuse
        viewstate = response.css('#__VIEWSTATE::attr(value)').get()
        eventvalidation = response.css('#__EVENTVALIDATION::attr(value)').get()
        viewstategenerator = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        
        # Track if we need to scrape any days
        days_to_scrape = []
        
        for day in range(1, month_days + 1):
            # Skip if already scraped and skip_existing is True
            date = datetime(target_year, target_month, day)
            if self.skip_existing and self._is_date_scraped(date):
                self.logger.debug(f"Skipping {date.strftime('%Y-%m-%d')} (already scraped)")
                continue
            
            days_to_scrape.append(day)
        
        if not days_to_scrape:
            self.logger.info(f"All days in {target_year}-{target_month:02d} already scraped")
            # Mark this month as complete and move to previous month
            self.months_skipped += 1
            if self.move_to_previous_month():
                self.save_checkpoint()
                
                # Navigate to previous month
                yield from self.navigate_to_previous_month(response)
            return
            
        # Start with first day to scrape
        yield from self.scrape_day(response, days_to_scrape[0], days_to_scrape[1:])
    
    def scrape_day(self, response, day, remaining_days):
        """Scrape a specific day in the current month using direct URL access."""
        target_year = response.meta.get('target_year', self.current_target_year)
        target_month = response.meta.get('target_month', self.current_target_month)
        
        # Create a date string for tracking
        date_str = f"{target_year}-{target_month:02d}-{day:02d}"
        target_date = datetime(target_year, target_month, day)
        
        self.logger.info(f"Scraping day: {date_str} using direct URL access")
        
        # Store reference to response for backfill continuation
        month_response = response
        
        # Use the direct date access method from the parent class
        direct_url = self.get_direct_date_url(target_date)
        self.logger.info(f"Direct URL: {direct_url}")
        
        yield scrapy.Request(
            url=direct_url,
            callback=self.handle_day_scrape,
            meta={
                'date': date_str,
                'expected_date': target_date,
                'current_month_date': self.current_month_date,
                'month_response': month_response,
                'remaining_days': remaining_days,
                'target_year': target_year,
                'target_month': target_month,
                'direct_access': True  # Flag to indicate we're using direct access
            },
            dont_filter=True,
            errback=self.handle_error
        )
    
    def handle_day_scrape(self, response):
        """Handle the response for a specific day and continue backfill."""
        date_str = response.meta.get('date')
        month_response = response.meta.get('month_response')
        remaining_days = response.meta.get('remaining_days', [])
        target_year = response.meta.get('target_year')
        target_month = response.meta.get('target_month')
        
        # Process the day's data using the parent class method
        yield from self.parse_schedule(response)
        
        # Continue with next day or move to previous month
        if remaining_days:
            # More days to scrape in current month
            yield from self.scrape_day(month_response, remaining_days[0], remaining_days[1:])
        else:
            # Month complete, move to previous month
            self.months_scraped += 1
            self.logger.info(f"Month {target_year}-{target_month:02d} complete. "
                            f"Total scraped: {self.months_scraped}, skipped: {self.months_skipped}")
            
            if self.move_to_previous_month():
                self.save_checkpoint()
                # Continue backfill with the previous month
                yield from self.navigate_to_previous_month(month_response)
            else:
                # Backfill complete
                duration = time.time() - self.start_time
                self.logger.info(f"Backfill complete! Total time: {duration:.2f}s, "
                               f"Months scraped: {self.months_scraped}, skipped: {self.months_skipped}")
    
    def navigate_to_previous_month(self, response):
        """Navigate to the previous month from the current view."""
        # Find the previous month button
        button_title = "Go to the previous month"
        nav_button = response.css(f'a[title="{button_title}"]')
        if not nav_button:
            self.logger.error(f"Could not find {button_title} button")
            return
        
        # Extract the __doPostBack arguments from the href
        href = nav_button.attrib['href']
        match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
        if not match:
            self.logger.error(f"Could not extract postback arguments from href: {href}")
            return
        
        event_target, event_argument = match.groups()
        
        # Extract ASP.NET form fields
        viewstate = response.css('#__VIEWSTATE::attr(value)').get()
        eventvalidation = response.css('#__EVENTVALIDATION::attr(value)').get()
        viewstategenerator = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        
        # Create form data for the navigation button
        formdata = {
            '__EVENTTARGET': event_target,
            '__EVENTARGUMENT': event_argument,
            '__VIEWSTATE': viewstate,
            '__EVENTVALIDATION': eventvalidation,
            '__VIEWSTATEGENERATOR': viewstategenerator,
        }
        
        self.logger.info(f"Navigating to previous month: {self.current_target_year}-{self.current_target_month:02d}")
        yield FormRequest(
            url=self.start_urls[0],
            formdata=formdata,
            callback=self.handle_calendar_navigation,
            meta={
                'dont_redirect': True,
                'handle_httpstatus_list': [301, 302],
                'target_year': self.current_target_year,
                'target_month': self.current_target_month
            },
            dont_filter=True
        )
    
    def handle_error(self, failure):
        """Handle any errors during request processing."""
        self.logger.error(f"Request failed: {failure.value}")
        meta = failure.request.meta
        self.logger.error(f"Failed while requesting date: {meta.get('date')}")
        
        # Try to recover if possible
        remaining_days = meta.get('remaining_days', [])
        month_response = meta.get('month_response')
        
        if remaining_days and month_response:
            # Skip to next day
            self.logger.info(f"Trying to recover by moving to next day: {remaining_days[0]}")
            return self.scrape_day(month_response, remaining_days[0], remaining_days[1:])
        elif self.move_to_previous_month() and month_response:
            # Move to previous month
            self.logger.info("Trying to recover by moving to previous month")
            self.save_checkpoint()
            return self.navigate_to_previous_month(month_response)