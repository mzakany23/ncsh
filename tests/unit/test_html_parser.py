import os
import pytest
from scrapy.http import HtmlResponse, Request
from ncsoccer.spiders.schedule_spider import ScheduleSpider

class TestHtmlParser:
    """Unit tests for HTML parsing functionality"""

    @pytest.fixture
    def sample_html(self):
        """Load sample HTML for testing"""
        html_path = os.path.join('data/html', '2024-03-01.html')
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()

    @pytest.fixture
    def mock_response(self, sample_html):
        """Create a mock response with our sample HTML"""
        url = 'https://nc-soccer-hudson.ezleagues.ezfacility.com/schedule.aspx'
        request = Request(url=url)
        response = HtmlResponse(
            url=url,
            body=sample_html.encode('utf-8'),
            encoding='utf-8',
            request=request
        )
        response.meta['date'] = '2024-03-01'
        return response

    def test_extract_complete_game_scores(self, mock_response):
        """Test extraction of scores from a completed game"""
        # Find first completed game
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header
        game_row = next(row for row in rows if row.css('td')[4].css('a::text').get().strip() == 'Complete')
        cells = game_row.css('td')

        # Test score format
        versus_text = cells[2].css('span::text').get('').strip()
        assert ' - ' in versus_text, "Score should contain ' - ' separator"

        # Test score values
        scores = versus_text.split(' - ')
        assert len(scores) == 2, "Should have home and away scores"
        assert all(score.strip().isdigit() for score in scores), "Scores should be numeric"

    def test_extract_game_status(self, mock_response):
        """Test extraction of game status from the schedule table"""
        # Find the schedule table
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header
        statuses = []

        # Extract all statuses from the Time/Status column
        for row in rows:
            cells = row.css('td')
            if len(cells) >= 5:  # Make sure we have enough cells
                status = cells[4].css('a::text').get('').strip()
                if status:  # Only get non-empty statuses
                    statuses.append(status)

        # Verify we found statuses
        assert len(statuses) > 0, "Should find at least one game status"

        # Verify all statuses are either Complete or time-based (e.g. "7:00 PM")
        for status in statuses:
            assert status == 'Complete' or 'PM' in status or 'AM' in status, f"Status {status} should be either 'Complete' or a time"

        # Count completed games
        complete_games = sum(1 for status in statuses if status == 'Complete')
        assert complete_games > 0, "Should have at least one completed game"

    def test_extract_team_names(self, mock_response):
        """Test extraction of team names"""
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header

        for row in rows:
            cells = row.css('td')
            home_team = cells[1].css('a::text').get('').strip()
            away_team = cells[3].css('a::text').get('').strip()

            # Test that team names are not empty
            assert home_team, "Home team name should not be empty"
            assert away_team, "Away team name should not be empty"

            # Test that team names are properly formatted
            # They should be non-empty strings with reasonable length
            assert len(home_team) >= 2, "Home team name should be at least 2 characters"
            assert len(away_team) >= 2, "Away team name should be at least 2 characters"

            # Test that team names don't contain HTML or excessive whitespace
            assert '<' not in home_team and '>' not in home_team, "Home team name should not contain HTML"
            assert '<' not in away_team and '>' not in away_team, "Away team name should not contain HTML"
            assert home_team == home_team.strip(), "Home team name should not have leading/trailing whitespace"
            assert away_team == away_team.strip(), "Away team name should not have leading/trailing whitespace"

    def test_extract_league_info(self, mock_response):
        """Test extraction of league information"""
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header

        for row in rows:
            cells = row.css('td')
            league = cells[0].css('a::text').get('').strip()

            assert league, "League name should not be empty"
            assert any(year in league for year in ['2024']), "League should contain year"

            if 'session' in league.lower():
                session = league.split('session')[-1].strip()
                assert session, "Session should not be empty when present"

    def test_extract_venue(self, mock_response):
        """Test extraction of venue information"""
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header

        for row in rows:
            cells = row.css('td')
            venue = cells[5].css('a::text').get('').strip()
            assert venue.startswith('Field '), "Venue should start with 'Field '"
            assert venue.split(' ')[1].isdigit(), "Venue number should be numeric"

    def test_extract_officials(self, mock_response):
        """Test extraction of officials information"""
        schedule_table = mock_response.css('table#ctl00_c_Schedule1_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header

        for row in rows:
            cells = row.css('td')
            officials = cells[6].css('::text').get('').strip()

            # Officials field might be empty for some games
            if officials:
                assert len(officials) > 0, "Officials should not be empty when present"
                assert not officials.isdigit(), "Officials should be a name, not a number"