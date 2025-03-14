import scrapy
from scrapy.http import FormRequest, HtmlResponse, Request
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

# Import the score extraction agent (will be used as a fallback)
try:
    from ..agents.score_extraction_agent import ScoreExtractionAgent
    AGENT_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    AGENT_AVAILABLE = False

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
        'DOWNLOAD_DELAY': 2,  # Increased to avoid rate limiting
        'CONCURRENT_REQUESTS': 1,
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'sec-ch-ua': '"Chromium";v="121", "Not A(Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        },
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 5,  # Increased for better reliability
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 403]
    }

    def __init__(self, mode='day', year=None, month=None, day=None, skip_existing=True,
                 html_prefix='data/html', json_prefix='data/json', lookup_file='data/lookup.json',
                 storage_type='s3', bucket_name=None, lookup_type='file', region='us-east-2',
                 table_name=None, force_scrape=False, use_test_data=False, use_agent=True, 
                 anthropic_api_key=None, *args, **kwargs):
        super(ScheduleSpider, self).__init__(*args, **kwargs)

        # Parse configuration
        self.target_year = int(year) if year else datetime.now().year
        self.target_month = int(month) if month else datetime.now().month
        self.target_day = int(day) if day else datetime.now().day

        # Fix force_scrape handling - convert string to bool properly
        force_scrape = str(force_scrape).lower() == 'true'
        self.skip_existing = not force_scrape
        self.use_test_data = use_test_data

        # Set up storage paths
        # Use test_data prefix if use_test_data is True
        if use_test_data:
            self.html_prefix = 'test_data/html'
            self.json_prefix = 'test_data/json'
        else:
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
        
        # Set up score extraction agent if available and requested
        self.use_agent = use_agent and AGENT_AVAILABLE
        self.agent = None
        self.anthropic_api_key = anthropic_api_key
        
        if self.use_agent:
            try:
                self.agent = ScoreExtractionAgent(api_key=self.anthropic_api_key)
                self.logger.info("Score extraction agent initialized successfully")
            except Exception as e:
                self.logger.error(f"Failed to initialize score extraction agent: {e}")
                self.use_agent = False

    def _is_date_scraped(self, date):
        """Check if a date has already been scraped using the lookup data"""
        date_str = date.strftime('%Y-%m-%d')
        return self.lookup.is_date_scraped(date_str)

    def start_requests(self):
        """Override start_requests to use GET first to establish session"""
        try:
            if self.skip_existing and self._is_date_scraped(datetime(self.target_year, self.target_month, self.target_day)):
                self.logger.info(f"Skipping {self.target_year}-{self.target_month}-{self.target_day} (already scraped)")
                return

            # Try to load local HTML file first, regardless of test mode
            date_str = f"{self.target_year}-{self.target_month:02d}-{self.target_day:02d}"
            html_path = os.path.join(self.html_prefix, f"{date_str}.html")
            try:
                if os.path.exists(html_path):
                    self.logger.info(f"Loading HTML from local file: {html_path}")
                    with open(html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    request = Request(url=self.start_urls[0])
                    response = HtmlResponse(
                        url=self.start_urls[0],
                        body=html_content.encode('utf-8'),
                        encoding='utf-8',
                        request=request
                    )
                    response.meta['date'] = date_str
                    yield from self.parse_schedule(response)
                    return
                else:
                    self.logger.info(f"Local HTML file not found at {html_path}, proceeding with network request.")
                    req = Request(url=self.start_urls[0], callback=self.parse_schedule)
                    req.meta['date'] = date_str
                    yield req
            except Exception as e:
                self.logger.error(f"Failed to load HTML: {e}")
                raise RuntimeError(f"Failed to load HTML: {e}")

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
        except Exception as e:
            self.logger.error(f"Error in start_requests: {e}")
            raise RuntimeError(f"Error in start_requests: {e}")

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

            # Initialize empty games list
            games = []

            if not schedule_table:
                self.logger.warning(f"No schedule table found for {date_str}")
                metadata = {
                    'date': date_str,
                    'games_found': False,
                    'error': 'No schedule table found',
                    'games_count': 0
                }
                self.store_metadata(metadata, date_str)
                # Store empty games list instead of returning
                self.store_games(games, date_str)
                success = True  # This is still a successful scrape, just with no games
                games_count = 0
                yield {  # Yield empty result instead of returning
                    'date': date_str,
                    'games_found': False,
                    'games_count': 0
                }
                return

            # Process games one at a time
            rows = schedule_table.css('tr')[1:]  # Skip header row
            for row in rows:
                cells = row.css('td')
                if len(cells) >= 7:  # Ensure we have all expected columns
                    games_count += 1
                    league = cells[0].css('a::text').get('').strip()
                    # Extract session from league name (e.g., "session 2 2024" from "Mens 40+ Friday night 7v7 Indoor session 2 2024")
                    session = league.split('session')[-1].strip() if 'session' in league else ''

                    # Enhanced score extraction to handle different HTML structures
                    home_score = None
                    away_score = None
                    extraction_method = "standard"
                    
                    # Try the standard method first (direct span text in third column)
                    versus_text = cells[2].css('span::text').get('').strip()
                    
                    # If that doesn't have scores, check if we have a td with schedule-versus-column class (2010 format)
                    if not versus_text or ' - ' not in versus_text:
                        # First, check if the current cell has the schedule-versus-column class
                        if 'class' in cells[2].attrib and 'schedule-versus-column' in cells[2].attrib['class']:
                            versus_text = cells[2].css('span::text').get('').strip()
                            extraction_method = "schedule-versus-column"
                        else:
                            # If not, try finding any cell with the schedule-versus-column class
                            for i, cell in enumerate(cells):
                                if 'class' in cell.attrib and 'schedule-versus-column' in cell.attrib['class']:
                                    versus_text = cell.css('span::text').get('').strip()
                                    extraction_method = f"schedule-versus-column-cell-{i}"
                                    break
                    
                    # Parse scores if we found them
                    if versus_text and ' - ' in versus_text:
                        try:
                            scores = versus_text.split(' - ')
                            home_score = int(scores[0].strip())
                            away_score = int(scores[1].strip())
                            self.logger.info(f"Successfully extracted scores: {home_score} - {away_score} using {extraction_method}")
                        except (ValueError, IndexError):
                            self.logger.warning(f"Failed to parse scores from: {versus_text}")
                            home_score = None
                            away_score = None
                    elif versus_text == 'v':
                        # This is a pending game, scores should be None
                        home_score = None
                        away_score = None
                        extraction_method = "pending-game"
                    else:
                        self.logger.warning(f"Standard extraction methods failed with text: {versus_text}")
                        
                        # If all standard methods fail and the agent is available, try it as a fallback
                        if self.use_agent and self.agent and status.lower() == 'complete':
                            try:
                                # Get the HTML of the current row for the agent
                                row_html = row.get()
                                
                                # Also get the table HTML for context if this is the first row
                                table_html = None
                                if games_count == 1:  # First game in this page
                                    table_html = schedule_table.get()
                                
                                self.logger.info("Attempting score extraction using Claude agent")
                                agent_home_score, agent_away_score, agent_method = self.agent.extract_scores(
                                    row_html=row_html,
                                    table_html=table_html
                                )
                                
                                if agent_home_score is not None and agent_away_score is not None:
                                    home_score = agent_home_score
                                    away_score = agent_away_score
                                    extraction_method = f"agent-{agent_method}"
                                    self.logger.info(f"Agent successfully extracted scores: {home_score} - {away_score} using {extraction_method}")
                            except Exception as e:
                                self.logger.error(f"Agent-based score extraction failed: {e}")

                    # Extract status (no times available)
                    status = cells[4].css('a::text').get('').strip()

                    games.append({
                        'league': league,
                        'session': session,
                        'home_team': cells[1].css('a::text').get('').strip(),
                        'away_team': cells[3].css('a::text').get('').strip(),
                        'status': status,
                        'venue': cells[5].css('a::text').get('').strip(),
                        'officials': cells[6].css('::text').get('').strip(),
                        'time': None,  # No times available in the HTML
                        'home_score': home_score,
                        'away_score': away_score
                    })

            # Write the complete JSON file
            json_data = {
                'date': date_str,
                'games_found': True,
                'games': games
            }
            json_filename = f"{self.json_prefix}/{date_str}.json"
            self.storage.write(json_filename, json.dumps(json_data, indent=2))

            # Store games in partitioned format
            self.store_games(games, date_str)

            # Store metadata in partitioned format
            metadata = {
                'date': date_str,
                'games_found': True,
                'games_count': games_count
            }
            self.store_metadata(metadata, date_str)

            self.logger.info(f"Found {games_count} games for {date_str}")
            success = True

            # Yield the result
            yield {
                'date': date_str,
                'games_found': True,
                'games_count': games_count,
                'games': games
            }

        finally:
            # Always update the lookup with the result
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
        """Store metadata in JSON format using partitioned storage"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            self.logger.error(f"Invalid date format for {date_str}, expected YYYY-MM-DD")
            return

        # Store in partitioned format
        year = dt.year
        month = dt.month
        day = dt.day
        base_output = self.json_prefix.split('/json')[0]  # Get the base directory (e.g., test_data or data)
        write_record(metadata, base_output, "metadata", year, month, day, storage=self.storage)

    def store_games(self, games, date_str):
        """Store games data in JSON format using partitioned storage"""
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            self.logger.error(f"Invalid date format for {date_str}, expected YYYY-MM-DD")
            return

        # Store in partitioned format
        year = dt.year
        month = dt.month
        day = dt.day
        base_output = self.json_prefix.split('/json')[0]  # Get the base directory (e.g., test_data or data)
        write_record(games, base_output, "games", year, month, day, storage=self.storage)

def write_record(data, base_output, record_type, year, month, day, storage=None):
    """Write a record to partitioned storage.
    Both games and metadata are stored in data.jsonl files under year/month/day partitions.

    Args:
        data: The data to write (list or dict)
        base_output: Base directory for output (e.g. 'data' or 'test_data')
        record_type: Type of record ('games' or 'metadata')
        year: Year for partitioning
        month: Month for partitioning
        day: Day for partitioning (required)
        storage: Optional storage interface (e.g. for S3)
    """
    import os, json

    # Build the directory path with required day-level partitioning
    directory = os.path.join(base_output, record_type, f"year={year}", f"month={month:02d}", f"day={day:02d}")
    file_path = os.path.join(directory, "data.jsonl")

    # Prepare content to write
    if isinstance(data, list):
        content = "".join(json.dumps(item) + '\n' for item in data)
    else:
        content = json.dumps(data) + '\n'

    # If a storage interface is provided, use it (e.g., for s3)
    if storage is not None:
        return storage.write(file_path, content)
    else:
        os.makedirs(directory, exist_ok=True)
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode, encoding='utf-8') as f:
            f.write(content)
