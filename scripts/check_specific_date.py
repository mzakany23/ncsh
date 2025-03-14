#!/usr/bin/env python3
"""
Quick script to check for games on a specific date in the dataset
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Date to check - last Tuesday (March 11, 2025)
target_date = "2025-03-11"

# Make the dataset path absolute
dataset_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output/dataset.parquet')

# Load the dataset
if not os.path.exists(dataset_path):
    print(f"Dataset not found: {dataset_path}")
    sys.exit(1)

print(f"Loading dataset from {dataset_path}")
df = pd.read_parquet(dataset_path)
print(f"Dataset loaded successfully with {len(df)} records")

# Filter for the target date
date_games = df[df['source_date'] == target_date]

# Print results
print(f"\n=== GAMES FOR {target_date} ===")
print(f"Number of games: {len(date_games)}")

if len(date_games) > 0:
    print("\nGame details:")
    for idx, game in date_games.iterrows():
        status = game['status']
        venue = game['venue']
        home_team = game['home_team']
        away_team = game['away_team']
        league = game['league']
        
        # Check if scores are available
        if pd.notna(game['home_score']) and pd.notna(game['away_score']):
            score_info = f"Score: {game['home_score']}-{game['away_score']}"
        else:
            score_info = f"Status: {status}"
        
        print(f"{idx+1}. {home_team} vs {away_team} ({venue}) - {score_info} - {league}")
    
    # Print status distribution
    print("\nStatus distribution:")
    print(date_games['status'].value_counts())
else:
    print("No games found for this date.")
