from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Game(BaseModel):
    """Schema for a single game"""
    home_team: str
    away_team: str
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    league: str
    time: Optional[str] = None

class GameData(BaseModel):
    """Schema for game data record"""
    date: datetime
    games: Game
    url: Optional[str] = None
    type: Optional[str] = None
    status: Optional[float] = None
    headers: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    def to_dict(self):
        """Convert to a flat dictionary structure"""
        base_dict = self.model_dump(exclude={'games'})
        game_dict = self.games.model_dump()
        return {**base_dict, **game_dict}