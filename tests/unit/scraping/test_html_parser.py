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
        # Find completed games
        schedule_table = mock_response.css('table#ctl04_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header
        
        # Looking for "Complete" text in any cell
        complete_games = []
        for row in rows:
            cells = row.css('td')
            if any(cell.css('a::text').get('').strip() == 'Complete' for cell in cells):
                complete_games.append(row)
        
        # Skip if no complete games
        if not complete_games:
            pytest.skip("No completed games found in the sample")
            
        game_row = complete_games[0]
        cells = game_row.css('td')
        
        # Find the score cells (they can be in different positions)
        score_cells = [cell for cell in cells if any(' - ' in text for text in cell.css('::text').extract())]
        
        if not score_cells:
            pytest.skip("No scores found in the completed game")
            
        # Get the text with the score
        score_text = ""
        for text in score_cells[0].css('::text').extract():
            if ' - ' in text:
                score_text = text.strip()
                break
        
        assert ' - ' in score_text, "Score should contain ' - ' separator"
        
        # Test score values
        scores = score_text.split(' - ')
        assert len(scores) == 2, "Should have home and away scores"
        assert all(score.strip().isdigit() for score in scores), "Scores should be numeric"

    def test_extract_game_status(self, mock_response):
        """Test extraction of game status from the schedule table"""
        # Find the schedule table
        schedule_table = mock_response.css('table#ctl04_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header
        
        # Check for any Complete text in the table
        complete_found = False
        for row in rows:
            all_texts = row.css('a::text').extract()
            if 'Complete' in all_texts:
                complete_found = True
                break
                
        # Verify we found the 'Complete' status somewhere in the table
        assert complete_found, "Should find at least one game with 'Complete' status"

    def test_extract_team_names(self, mock_response):
        """Test extraction of team names"""
        schedule_table = mock_response.css('table#ctl04_GridView1')
        rows = schedule_table.css('tr')[1:]  # Skip header

        # Find cells with team data
        team_cells = []
        for row in rows:
            cells = row.css('td')
            for i, cell in enumerate(cells):
                if cell.css('a::text').get('') and not cell.css('a::text').get('').startswith('Fri-') and not cell.css('a::text').get('') == 'Complete':
                    team_cells.append(cell.css('a::text').get('').strip())
        
        # Verify we found team names
        assert len(team_cells) > 0, "Should find at least one team name"
        
        # Verify team names are not empty
        for team in team_cells:
            assert team, "Team name should not be empty"
            # They should be non-empty strings with reasonable length
            assert len(team) >= 2, "Team name should be at least 2 characters"
            
            # Test that team names don't contain HTML
            assert '<' not in team and '>' not in team, "Team name should not contain HTML"
            assert team == team.strip(), "Team name should not have leading/trailing whitespace"

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