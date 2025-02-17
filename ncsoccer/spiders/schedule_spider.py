import scrapy
from scrapy.http import FormRequest
from ncsoccer.items import NcsoccerItem
from datetime import datetime, timedelta
import os
import json
import re
from calendar import monthrange
from ..pipeline.config import (
    ScraperConfig,
    ScrapeMode,
    get_storage_interface,
    create_scraper_config
)
from ..pipeline.lookup import get_lookup_interface

class ScheduleSpider(scrapy.Spider):
    name = 'schedule'
    allowed_domains = ['nc-soccer-hudson.ezleagues.ezfacility.com']
    base_url = 'https://nc-soccer-hudson.ezleagues.ezfacility.com'
    facility_id = '690'
    start_urls = [f'https://nc-soccer-hudson.ezleagues.ezfacility.com/schedule.aspx?facility_id=690']

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': False,
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG': True,
        'DOWNLOAD_DELAY': 1,
        'CONCURRENT_REQUESTS': 1
    }

    def __init__(self, mode='day', year=None, month=None, day=None, skip_existing=True,
                 html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
                 storage_type='s3', bucket_name=None, lookup_type='file', region='us-east-2',
                 table_name=None, *args, **kwargs):
        super(ScheduleSpider, self).__init__(*args, **kwargs)

        # Parse configuration
        self.target_year = int(year) if year else datetime.now().year
        self.target_month = int(month) if month else datetime.now().month
        self.target_day = int(day) if day else datetime.now().day
        self.skip_existing = str(skip_existing).lower() == 'true'

        # Set up storage paths
        self.html_prefix = html_prefix
        self.json_prefix = json_prefix
        self.lookup_file = lookup_file

        # Create scraper configuration
        self.config = create_scraper_config(
            mode='day',  # Always use day mode, we'll handle multiple days externally
            year=self.target_year,
            month=self.target_month,
            day=self.target_day,
            skip_existing=self.skip_existing,
            storage_type=storage_type,
            bucket_name=bucket_name
        )

        # Set up storage interface
        self.storage = get_storage_interface(self.config.storage_type, self.config.bucket_name, region=region)

        # Set up lookup interface
        self.lookup = get_lookup_interface(lookup_type, lookup_file=lookup_file, region=region, table_name=table_name)

        # Track current month to avoid unnecessary navigation
        self.current_month_date = None

    def _is_date_scraped(self, date):
        """Check if a date has already been scraped using the lookup data"""
        date_str = date.strftime('%Y-%m-%d')
        return self.lookup.is_date_scraped(date_str)

    def start_requests(self):
        """Override start_requests to use GET first to establish session"""
        if self.skip_existing and self._is_date_scraped(datetime(self.target_year, self.target_month, self.target_day)):
            self.logger.info(f"Skipping {self.target_year}-{self.target_month}-{self.target_day} (already scraped)")
            return

        # If we're already on the correct month, we can reuse that state
        if (self.current_month_date and
            self.current_month_date.year == self.target_year and
            self.current_month_date.month == self.target_month):
            self.logger.info(f"Already on correct month: {self.current_month_date.strftime('%B %Y')}")
            # Create a fake response to reuse the parse method
            response = scrapy.http.HtmlResponse(
                url=self.start_urls[0],
                body=b'',  # Empty body since we'll make a new request anyway
                encoding='utf-8'
            )
            return self.parse(response)

        # Otherwise start fresh from the current date
        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse,
            dont_filter=True,
            meta={'dont_redirect': True, 'handle_httpstatus_list': [301, 302]}
        )

    def validate_current_page(self, response, expected_date=None):
        """Validate that we're on the expected page
        Returns: (is_valid, current_date, error_message)
        """
        # Get the current month and year from the header
        month_year = response.css('td[align="center"][style*="width:70%"]::text').get()
        if not month_year:
            return False, None, "Could not find month/year header"

        try:
            current_month_date = datetime.strptime(month_year.strip(), '%B %Y')
        except Exception as e:
            return False, None, f"Could not parse month/year: {e}"

        # If we're not validating against an expected date, just return success
        if not expected_date:
            return True, current_month_date, None

        # Check if we're in the correct month and year
        if (current_month_date.year != expected_date.year or
            current_month_date.month != expected_date.month):
            return False, current_month_date, (
                f"Wrong month/year. Expected: {expected_date.strftime('%B %Y')}, "
                f"Got: {current_month_date.strftime('%B %Y')}"
            )

        # Find the currently selected date (usually highlighted or marked as selected)
        selected_date = response.css('a[style*="color:White"]::text, a.SelectedDate::text').get()
        if not selected_date:
            return False, current_month_date, "Could not find selected date"

        try:
            selected_day = int(selected_date.strip())
            if selected_day != expected_date.day:
                return False, current_month_date, (
                    f"Wrong day selected. Expected: {expected_date.day}, "
                    f"Got: {selected_day}"
                )
        except ValueError as e:
            return False, current_month_date, f"Could not parse selected date: {e}"

        return True, current_month_date, None

    def parse(self, response):
        """Initial parse of the schedule page"""
        # Validate the current page (no expected date yet, just checking we can parse it)
        is_valid, current_month_date, error = self.validate_current_page(response)
        if not is_valid:
            self.logger.error(f"Initial page validation failed: {error}")
            return

        # Only store HTML for target date, not navigation pages
        target_date = datetime(self.target_year, self.target_month, self.target_day)
        if response.meta.get('date') == target_date.strftime('%Y-%m-%d'):
            self.store_raw_html(response)

        # Update our current month tracking
        self.current_month_date = current_month_date
        self.logger.info(f"Current page shows: {current_month_date.strftime('%B %Y')}")

        # If we're not in the correct month, click the back/forward button
        if current_month_date.year != target_date.year or current_month_date.month != target_date.month:
            self.logger.info(f"Need to navigate from {current_month_date.strftime('%B %Y')} to {target_date.strftime('%B %Y')}")

            # Determine if we need to go forward or back
            months_diff = (target_date.year - current_month_date.year) * 12 + (target_date.month - current_month_date.month)
            self.logger.info(f"Need to navigate {abs(months_diff)} months {'forward' if months_diff > 0 else 'back'}")

            # Find the appropriate navigation button
            button_title = "Go to the previous month" if months_diff < 0 else "Go to the next month"
            nav_button = response.css(f'a[title="{button_title}"]')
            if not nav_button:
                self.logger.error(f"Could not find {button_title} button")
                # Log all links to help debug
                for link in response.css('a'):
                    self.logger.info(f"Found link: {link.get()}")
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
                callback=self.parse,
                meta={
                    'dont_redirect': True,
                    'handle_httpstatus_list': [301, 302],
                    'current_month_date': current_month_date  # Pass along our current position
                },
                dont_filter=True
            )
            return

        self.logger.info(f"Found correct month, looking for day {self.target_day}")
        # Extract ASP.NET form fields
        viewstate = response.css('#__VIEWSTATE::attr(value)').get()
        eventvalidation = response.css('#__EVENTVALIDATION::attr(value)').get()
        viewstategenerator = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()

        # Find the link for our target date
        target_date_str = f"{self.target_day}"  # The text content we're looking for
        date_link = response.css(f'a[title*="{current_month_date.strftime("%B")} {self.target_day}"]')

        if not date_link:
            self.logger.error(f"Could not find link for date: {target_date_str}")
            return

        # Extract the __doPostBack arguments from the href
        href = date_link.attrib['href']
        match = re.search(r"__doPostBack\('([^']+)','([^']+)'\)", href)
        if not match:
            self.logger.error(f"Could not extract postback arguments from href: {href}")
            return

        event_target, event_argument = match.groups()

        # Create form data for the postback
        formdata = {
            '__EVENTTARGET': event_target,
            '__EVENTARGUMENT': event_argument,
            '__VIEWSTATE': viewstate,
            '__EVENTVALIDATION': eventvalidation,
            '__VIEWSTATEGENERATOR': viewstategenerator,
        }

        self.logger.info(f"Submitting form for date: {current_month_date.strftime('%B')} {self.target_day}")

        target_date = datetime(self.target_year, self.target_month, self.target_day)
        yield FormRequest(
            url=self.start_urls[0],
            formdata=formdata,
            callback=self.parse_schedule,
            meta={
                'date': target_date.strftime('%Y-%m-%d'),
                'expected_date': target_date,
                'current_month_date': current_month_date  # Pass along our current position
            },
            dont_filter=True,
            errback=self.handle_error
        )

    def handle_error(self, failure):
        """Handle any errors during request processing"""
        self.logger.error(f"Request failed: {failure.value}")
        meta = failure.request.meta
        self.logger.error(f"Failed while requesting date: {meta.get('date')}")

    def parse_schedule(self, response):
        """Parse the schedule for a specific day"""
        date_str = response.meta.get('date')
        success = False
        games_count = 0

        try:
            # Store raw HTML immediately
            self.store_raw_html(response)

            # Find the schedule table
            schedule_table = response.css('table#ctl00_c_Schedule1_GridView1')

            if not schedule_table:
                self.logger.warning(f"No schedule table found for {date_str}")
                metadata = {
                    'date': date_str,
                    'games_found': False,
                    'error': 'No schedule table found'
                }
                self.store_metadata(metadata, date_str)
                return

            # Find the events table for game times
            events_table = response.css('table.ezl-base-table.league-border-color.mb-0.w-100 tr')
            game_times = {}

            # Extract times from events table
            for row in events_table:
                cells = row.css('td[align="left"]::text').getall()
                if len(cells) >= 2:
                    start_time = cells[0].strip()
                    if start_time:  # Only store if we have a valid time
                        venue = cells[3].strip() if len(cells) > 3 else None
                        game_times[venue] = start_time

            # Initialize the JSON structure
            json_data = {
                'date': date_str,
                'games_found': True,
                'games': []
            }

            # Process games one at a time
            for row in schedule_table.css('tr')[1:]:  # Skip header row
                cells = row.css('td')
                if len(cells) >= 7:  # Ensure we have all expected columns
                    # Parse score into home and away
                    score_text = cells[2].css('span::text').get('').strip()
                    home_score = away_score = None
                    if score_text and ' - ' in score_text:
                        scores = score_text.split(' - ')
                        if len(scores) == 2:
                            try:
                                home_score = int(scores[0])
                                away_score = int(scores[1])
                            except ValueError:
                                self.logger.warning(f"Could not parse scores from: {score_text}")

                    # Get status or game time
                    status_text = cells[4].css('a::text').get('').strip()
                    status = time = None

                    # Check if it's a time (e.g., "7:00 PM") or status (e.g., "Complete")
                    if 'PM' in status_text or 'AM' in status_text:
                        status = 'Scheduled'
                        time = status_text
                    else:
                        status = status_text
                        venue = cells[5].css('a::text').get('').strip()
                        time = game_times.get(venue)

                    # Parse league and session from the league field
                    league_text = cells[0].css('a::text').get('').strip()
                    league = session = None
                    if '"' in league_text:  # e.g. 'Mens Open "C" Indoor session 2 2025'
                        parts = league_text.split('"')
                        if len(parts) >= 3:
                            # Extract league (e.g. "Mens Open C")
                            league = (parts[0] + parts[1] + parts[2].split(' ')[0]).strip()
                            # Extract session (e.g. "Indoor session 2 2025")
                            session = ' '.join(parts[2].split(' ')[1:]).strip()
                    else:
                        league = league_text
                        session = ''

                    venue = cells[5].css('a::text').get('').strip()
                    game = {
                        'league': league,
                        'session': session,
                        'home_team': cells[1].css('a::text').get('').strip(),
                        'away_team': cells[3].css('a::text').get('').strip(),
                        'home_score': home_score,
                        'away_score': away_score,
                        'status': status,
                        'venue': venue,
                        'time': time,
                        'officials': cells[6].css('::text').get('').strip()
                    }

                    json_data['games'].append(game)
                    games_count += 1

            # Write the complete JSON file
            json_filename = f"{self.json_prefix}/{date_str}.json"
            self.storage.write(json_filename, json.dumps(json_data, indent=2))

            self.logger.info(f"Found {games_count} games for {date_str}")
            success = True

        finally:
            self.lookup.update_date(date_str, success=success, games_count=games_count)

    def store_raw_html(self, response, date_str=None):
        """Store raw HTML response to storage"""
        if not response or not response.text:
            self.logger.error("Cannot store empty response")
            return

        if not date_str:
            date_str = response.meta.get('date', datetime.now().strftime('%Y-%m-%d'))

        # Validate date string format
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            self.logger.error(f"Invalid date format for {date_str}, expected YYYY-MM-DD")
            return

        # Store raw HTML with YYYY-MM-DD.html naming
        html_path = f"{self.html_prefix}/{date_str}.html"
        if not self.storage.write(html_path, response.text):
            self.logger.error(f"Failed to write HTML file to {html_path}")
            return

        # Store response metadata
        meta_path = f"{self.json_prefix}/{date_str}_meta.json"
        meta_data = {
            'url': response.url,
            'date': date_str,
            'type': 'daily',
            'status': response.status,
            'headers': {k.decode('utf-8'): (v[0].decode('utf-8') if (v[0] is not None and isinstance(v[0], bytes)) else (v[0] or '')) for k, v in response.headers.items()},
            'timestamp': datetime.now().isoformat()
        }
        if not self.storage.write(meta_path, json.dumps(meta_data, indent=2)):
            self.logger.error(f"Failed to write metadata JSON file to {meta_path}")
            return

    def store_metadata(self, metadata, date_str):
        """Store game metadata in JSON format"""
        # Validate date string format
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            self.logger.error(f"Invalid date format for {date_str}, expected YYYY-MM-DD")
            return

        # Validate metadata structure
        required_fields = ['date', 'games_found']
        if not all(field in metadata for field in required_fields):
            self.logger.error(f"Missing required fields in metadata: {required_fields}")
            return

        json_path = f"{self.json_prefix}/{date_str}.json"
        if not self.storage.write(json_path, json.dumps(metadata, indent=2)):
            self.logger.error(f"Failed to write game data JSON file to {json_path}")
            return
