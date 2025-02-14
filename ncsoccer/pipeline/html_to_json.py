import os
import json
from bs4 import BeautifulSoup
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HTMLParser:
    def __init__(self, year, month):
        self.year = year
        self.month = month
        self.raw_html_dir = os.path.join('data', 'raw', 'html', str(year), f"{month:02d}")
        self.parsed_json_dir = os.path.join('data', 'parsed', 'json', str(year), f"{month:02d}")
        os.makedirs(self.parsed_json_dir, exist_ok=True)

    def parse_daily_schedule(self, html_content, date):
        """Parse daily schedule HTML into structured data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        games = []

        # Parse games from the schedule grid
        schedule_table = soup.select_one('#ctl00_ContentPlaceHolder1_gvGames')
        if not schedule_table:
            return games

        for row in schedule_table.select('tr'):
            # Skip header row
            if row.find('th'):
                continue

            # Extract cells
            cells = row.find_all('td')
            if len(cells) < 6:
                continue

            # Parse date and time
            date_text = cells[0].get_text(strip=True)
            time_text = cells[1].get_text(strip=True)

            try:
                if date_text and time_text:
                    dt = datetime.strptime(f"{date_text} {time_text}", '%m/%d/%Y %I:%M %p')
                else:
                    continue
            except Exception as e:
                logger.error(f"Date parsing error: {e}")
                continue

            game_data = {
                'date': dt.strftime('%Y-%m-%d'),
                'time': dt.strftime('%I:%M %p'),
                'home_team': cells[2].get_text(strip=True),
                'away_team': cells[3].get_text(strip=True),
                'field': cells[4].get_text(strip=True),
                'league_name': cells[5].get_text(strip=True)
            }
            games.append(game_data)

        return games

    def parse_league_schedule(self, html_content, league_name, date):
        """Parse league schedule HTML into structured data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        games = []

        # Parse games from the schedule grid
        schedule_table = soup.select_one('#ctl00_ContentPlaceHolder1_gvGames')
        if not schedule_table:
            return games

        for row in schedule_table.select('tr'):
            # Skip header row
            if row.find('th'):
                continue

            # Extract cells
            cells = row.find_all('td')
            if len(cells) < 5:
                continue

            # Parse date and time
            date_text = cells[0].get_text(strip=True)
            time_text = cells[1].get_text(strip=True)

            try:
                if date_text and time_text:
                    dt = datetime.strptime(f"{date_text} {time_text}", '%m/%d/%Y %I:%M %p')
                else:
                    continue
            except Exception as e:
                logger.error(f"Date parsing error: {e}")
                continue

            game_data = {
                'date': dt.strftime('%Y-%m-%d'),
                'time': dt.strftime('%I:%M %p'),
                'league_name': league_name,
                'home_team': cells[2].get_text(strip=True),
                'away_team': cells[3].get_text(strip=True),
                'field': cells[4].get_text(strip=True)
            }
            games.append(game_data)

        return games

    def process_day(self, day_dir):
        """Process all HTML files for a single day"""
        date = os.path.basename(day_dir)
        games = []

        # Process daily schedule
        daily_html_path = os.path.join(day_dir, 'daily.html')
        if os.path.exists(daily_html_path):
            with open(daily_html_path, 'r', encoding='utf-8') as f:
                daily_games = self.parse_daily_schedule(f.read(), date)
                games.extend(daily_games)

        # Process league schedules
        for filename in os.listdir(day_dir):
            if filename.startswith('league_schedule_') and filename.endswith('.html'):
                league_name = filename[15:-5]  # Extract league name from filename
                with open(os.path.join(day_dir, filename), 'r', encoding='utf-8') as f:
                    league_games = self.parse_league_schedule(f.read(), league_name, date)
                    games.extend(league_games)

        # Save parsed data
        output_file = os.path.join(self.parsed_json_dir, f"{date}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(games, f, indent=2)

        return len(games)

    def process_month(self):
        """Process all days in the month"""
        total_games = 0
        for day_dir in sorted(os.listdir(self.raw_html_dir)):
            day_path = os.path.join(self.raw_html_dir, day_dir)
            if os.path.isdir(day_path):
                games_count = self.process_day(day_path)
                total_games += games_count
                logger.info(f"Processed {day_dir}: {games_count} games found")

        logger.info(f"Total games processed for {self.year}-{self.month:02d}: {total_games}")
        return total_games

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python html_to_json.py <year> <month>")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    parser = HTMLParser(year, month)
    parser.process_month()