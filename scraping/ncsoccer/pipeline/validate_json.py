import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GameValidator:
    REQUIRED_FIELDS = ['date', 'league_name', 'home_team', 'away_team', 'field']

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        self.parsed_json_dir = os.path.join('data', 'parsed', 'json', str(year), f"{month:02d}")
        self.validation_dir = os.path.join('data', 'validation', str(year), f"{month:02d}")
        os.makedirs(self.validation_dir, exist_ok=True)

    def validate_game(self, game: Dict[str, Any]) -> List[str]:
        """Validate a single game entry"""
        errors = []

        # Check required fields
        for field in self.REQUIRED_FIELDS:
            if field not in game or not game[field]:
                errors.append(f"Missing required field: {field}")

        # Validate date format
        try:
            datetime.strptime(game['date'], '%Y-%m-%d')
        except (ValueError, KeyError):
            errors.append("Invalid date format")

        # Validate teams are different
        if game.get('home_team') == game.get('away_team'):
            errors.append("Home team and away team are the same")

        # Validate field format (should start with "Field")
        if game.get('field') and not game['field'].startswith('Field'):
            errors.append("Invalid field format")

        return errors

    def validate_day(self, date: str) -> Dict[str, Any]:
        """Validate all games for a specific day"""
        input_file = os.path.join(self.parsed_json_dir, f"{date}.json")
        if not os.path.exists(input_file):
            return {
                'date': date,
                'status': 'missing',
                'errors': [f"No data file found for {date}"],
                'games_count': 0,
                'valid_games_count': 0
            }

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                games = json.load(f)
        except json.JSONDecodeError as e:
            return {
                'date': date,
                'status': 'invalid_json',
                'errors': [f"Invalid JSON format: {str(e)}"],
                'games_count': 0,
                'valid_games_count': 0
            }

        validation_results = {
            'date': date,
            'status': 'validated',
            'errors': [],
            'games_count': len(games),
            'valid_games_count': 0,
            'invalid_games': []
        }

        valid_games = []
        for i, game in enumerate(games):
            errors = self.validate_game(game)
            if errors:
                validation_results['invalid_games'].append({
                    'game_index': i,
                    'errors': errors,
                    'game_data': game
                })
            else:
                valid_games.append(game)
                validation_results['valid_games_count'] += 1

        # Save valid games to a new file
        if valid_games:
            output_file = os.path.join(self.validation_dir, f"{date}_valid.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(valid_games, f, indent=2)

        return validation_results

    def validate_month(self) -> Dict[str, Any]:
        """Validate all days in the month"""
        month_results = {
            'year': self.year,
            'month': self.month,
            'total_games': 0,
            'total_valid_games': 0,
            'days_with_errors': 0,
            'daily_results': []
        }

        # Get all JSON files in the parsed directory
        for filename in sorted(os.listdir(self.parsed_json_dir)):
            if filename.endswith('.json'):
                date = filename[:-5]  # Remove .json extension
                day_results = self.validate_day(date)
                month_results['daily_results'].append(day_results)

                month_results['total_games'] += day_results['games_count']
                month_results['total_valid_games'] += day_results['valid_games_count']

                if day_results['status'] != 'validated' or day_results.get('invalid_games'):
                    month_results['days_with_errors'] += 1

        # Save month validation results
        results_file = os.path.join(self.validation_dir, 'validation_results.json')
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(month_results, f, indent=2)

        logger.info(f"Validation complete for {self.year}-{self.month:02d}")
        logger.info(f"Total games: {month_results['total_games']}")
        logger.info(f"Valid games: {month_results['total_valid_games']}")
        logger.info(f"Days with errors: {month_results['days_with_errors']}")

        return month_results

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python validate_json.py <year> <month>")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    validator = GameValidator(year, month)
    validator.validate_month()