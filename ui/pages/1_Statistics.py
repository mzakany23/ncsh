import streamlit as st
import sys
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
import pandas as pd

# Add the parent directory to Python path to import llama_query
sys.path.append(str(Path(__file__).parent.parent.parent))
from llama_query.query_engine import setup_database

# Page config
st.set_page_config(
    page_title="NC Soccer Hub - Statistics",
    page_icon="📊",
    layout="wide",
)

# Title
st.title("📊 League Statistics")

# Initialize database connection
@st.cache_resource
def get_engine():
    return setup_database()

engine = get_engine()

# Function to get team statistics
def get_team_stats():
    sql = """
    WITH team_matches AS (
        SELECT
            REGEXP_REPLACE(home_team, '\s*\(\d+\)\s*$', '') as team,
            date,
            home_score as goals_for,
            away_score as goals_against,
            CASE
                WHEN home_score > away_score THEN 'W'
                WHEN home_score = away_score THEN 'D'
                ELSE 'L'
            END as result
        FROM matches
        WHERE EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)
        UNION ALL
        SELECT
            REGEXP_REPLACE(away_team, '\s*\(\d+\)\s*$', '') as team,
            date,
            away_score as goals_for,
            home_score as goals_against,
            CASE
                WHEN away_score > home_score THEN 'W'
                WHEN away_score = home_score THEN 'D'
                ELSE 'L'
            END as result
        FROM matches
        WHERE EXTRACT(year FROM date) = EXTRACT(year FROM CURRENT_DATE)
    ),
    team_stats AS (
        SELECT
            team,
            COUNT(DISTINCT date) as games_played,
            COUNT(CASE WHEN result = 'W' THEN 1 END) as wins,
            COUNT(CASE WHEN result = 'D' THEN 1 END) as draws,
            COUNT(CASE WHEN result = 'L' THEN 1 END) as losses,
            SUM(goals_for) as goals_for,
            SUM(goals_against) as goals_against,
            SUM(goals_for - goals_against) as goal_diff,
            ROUND(CAST(COUNT(CASE WHEN result = 'W' THEN 1 END) as FLOAT) /
                  NULLIF(COUNT(DISTINCT date), 0) * 100, 1) as win_percentage
        FROM team_matches
        GROUP BY team
        HAVING COUNT(DISTINCT date) > 0
    )
    SELECT *
    FROM team_stats
    ORDER BY games_played DESC, wins DESC
    """

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df

# Function to get recent matches
def get_recent_matches(limit=10):
    sql = """
    SELECT
        date,
        home_team,
        away_team,
        home_score,
        away_score,
        home_score || '-' || away_score as score
    FROM matches
    WHERE home_score IS NOT NULL
    ORDER BY date DESC
    LIMIT :limit
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"limit": limit})
    return df

# Get data
with st.spinner("Loading statistics..."):
    team_stats = get_team_stats()
    recent_matches = get_recent_matches()

# Create two columns for the layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Teams by Games Played")

    # Bar chart of games played
    fig = px.bar(
        team_stats.head(10),
        x='team',
        y='games_played',
        title='Top 10 Teams by Games Played',
        labels={'team': 'Team', 'games_played': 'Games Played'}
    )
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent Matches")
    st.dataframe(
        recent_matches,
        column_config={
            "date": "Date",
            "home_team": "Home Team",
            "away_team": "Away Team",
            "score": "Score"
        },
        hide_index=True
    )

with col2:
    st.subheader("Team Performance")

    # Scatter plot of wins vs goals scored
    fig = px.scatter(
        team_stats,
        x='goals_for',
        y='wins',
        size='games_played',
        color='win_percentage',
        hover_data=['team', 'games_played', 'goal_diff'],
        title='Team Performance: Wins vs Goals Scored',
        labels={
            'goals_for': 'Goals Scored',
            'wins': 'Wins',
            'win_percentage': 'Win %'
        }
    )
    st.plotly_chart(fig, use_container_width=True)

    # Top teams table
    st.subheader("League Table")
    st.dataframe(
        team_stats,
        column_config={
            "team": "Team",
            "games_played": "GP",
            "wins": "W",
            "draws": "D",
            "losses": "L",
            "goals_for": "GF",
            "goals_against": "GA",
            "goal_diff": "GD",
            "win_percentage": st.column_config.NumberColumn(
                "Win %",
                format="%.1f%%"
            )
        },
        hide_index=True
    )