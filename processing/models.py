from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field, validator

class Game(BaseModel):
    """Schema for a single game with strict validation"""
    home_team: str = Field(..., min_length=1)
    away_team: str = Field(..., min_length=1)
    home_score: Optional[int] = Field(None, ge=0)
    away_score: Optional[int] = Field(None, ge=0)
    league: str = Field(..., min_length=1)
    time: Optional[str] = None

    @validator('home_team', 'away_team', 'league')
    def validate_strings(cls, v):
        """Ensure strings are properly formatted"""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return v.strip()

    @validator('home_score', 'away_score')
    def validate_scores(cls, v):
        """Ensure scores are valid when present"""
        if v is not None and v < 0:
            raise ValueError("Score cannot be negative")
        return v

class GameData(BaseModel):
    """Schema for game data record with strict validation"""
    date: datetime = Field(..., description="Date of the game")
    games: Game = Field(..., description="Game details")
    url: Optional[str] = Field(None, min_length=1)
    type: Optional[str] = None
    status: Optional[float] = Field(None, ge=0, le=1)
    headers: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @validator('url')
    def validate_url(cls, v):
        """Ensure URL is properly formatted when present"""
        if v is not None:
            if not v.strip():
                raise ValueError("URL cannot be empty or whitespace")
            if not (v.startswith('http://') or v.startswith('https://')):
                raise ValueError("URL must start with http:// or https://")
        return v

    @validator('date', pre=True)
    def validate_date(cls, v):
        """Convert string dates to datetime objects"""
        if isinstance(v, str):
            try:
                # First try ISO format (YYYY-MM-DD)
                dt = datetime.fromisoformat(v.replace('Z', '+00:00') if 'Z' in v else v)
                # Ensure the datetime is timezone-naive for consistent Parquet handling
                if dt.tzinfo is not None:
                    dt = dt.replace(tzinfo=None)
                return dt
            except ValueError:
                try:
                    # Try other formats (e.g., "Sat-Jun 1")
                    import re
                    import datetime as dt

                    # Handle format like "Sat-Jun 1"
                    match = re.match(r'(?:\w+)-(\w+) (\d+)', v)
                    if match:
                        month_str, day_str = match.groups()
                        month_map = {
                            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                        }
                        month = month_map.get(month_str, 1)  # Default to January if not recognized
                        day = int(day_str)
                        # Since we don't have year in this format, use current year
                        current_year = dt.datetime.now().year
                        return datetime(current_year, month, day)

                    # If all parsing attempts fail, raise error
                    raise ValueError(f"Unable to parse date string: {v}")
                except Exception as e:
                    raise ValueError(f"Invalid date format: {v}, error: {str(e)}")
        # Ensure any provided datetime is timezone-naive for Parquet
        elif isinstance(v, datetime) and v.tzinfo is not None:
            return v.replace(tzinfo=None)
        return v

    @validator('timestamp', pre=True)
    def validate_timestamp(cls, v):
        """Ensure timestamp is properly formatted and timezone-aware"""
        if isinstance(v, str):
            try:
                # Handle ISO format with 'Z' or timezone offset
                dt = datetime.fromisoformat(v.replace('Z', '+00:00') if 'Z' in v else v)
                # Ensure UTC timezone for storage
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                raise ValueError(f"Invalid timestamp format: {v}")
        # Ensure any provided datetime is timezone-aware
        elif isinstance(v, datetime) and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    def to_dict(self) -> dict:
        """Convert to a flat dictionary structure for Parquet storage"""
        base_dict = self.model_dump(exclude={'games'})
        game_dict = self.games.model_dump()

        # Convert datetime objects to consistent format
        if 'date' in base_dict and isinstance(base_dict['date'], datetime):
            # For Parquet, use timezone-naive datetime for better compatibility
            if base_dict['date'].tzinfo is not None:
                base_dict['date'] = base_dict['date'].replace(tzinfo=None)

        if 'timestamp' in base_dict and isinstance(base_dict['timestamp'], datetime):
            # For Parquet, use timezone-naive datetime for better compatibility
            if base_dict['timestamp'].tzinfo is not None:
                base_dict['timestamp'] = base_dict['timestamp'].replace(tzinfo=None)

        return {**base_dict, **game_dict}