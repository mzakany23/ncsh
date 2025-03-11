#!/usr/bin/env python3
"""
Exploratory Data Analysis (EDA) on the NC Soccer dataset using Plotly for interactive visualizations.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import numpy as np
import argparse
import os

def load_dataset(file_path):
    """Load the dataset from a Parquet file."""
    print(f"Loading dataset from {file_path}...")
    df = pd.read_parquet(file_path)
    print(f"Dataset loaded with {len(df)} records.")
    return df

def general_stats(df):
    """Print general statistics about the dataset."""
    print("\n=== General Dataset Statistics ===")
    print(f"Total records: {len(df)}")
    
    # Date range
    print(f"Date range: {df['full_date'].min()} to {df['full_date'].max()}")
    
    # Number of unique values in key columns
    for col in ['league', 'division', 'season', 'home_team', 'away_team']:
        if col in df.columns:
            unique_vals = df[col].nunique()
            print(f"Unique {col}: {unique_vals}")
    
    # Count records by year
    if 'year' in df.columns:
        year_counts = df['year'].value_counts().sort_index()
        print("\nGames per year:")
        for year, count in year_counts.items():
            print(f"  {year}: {count} games")

def examine_columns(df):
    """Examine and report on the columns in the dataset."""
    print("\n=== Column Analysis ===")
    print(f"Number of columns: {len(df.columns)}")
    print("\nColumns and their data types:")
    for col in sorted(df.columns):
        print(f"  {col}: {df[col].dtype}")
    
    # Missing values
    print("\nMissing values by column:")
    missing = df.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if len(missing) > 0:
        for col, count in missing.items():
            pct = (count / len(df)) * 100
            print(f"  {col}: {count} missing ({pct:.2f}%)")
    else:
        print("  No missing values found.")

def analyze_leagues(df):
    """Analyze data by league."""
    if 'league' not in df.columns:
        print("\n=== League Analysis ===")
        print("No 'league' column in dataset.")
        return
    
    print("\n=== League Analysis ===")
    league_counts = df['league'].value_counts()
    print("\nGames by league:")
    for league, count in league_counts.items():
        print(f"  {league}: {count} games")

def analyze_venues(df):
    """Analyze data by venue."""
    if 'venue' not in df.columns:
        print("\n=== Venue Analysis ===")
        print("No 'venue' column in dataset.")
        return
    
    print("\n=== Venue Analysis ===")
    venue_counts = df['venue'].value_counts().head(10)
    print("\nTop 10 venues by number of games:")
    for venue, count in venue_counts.items():
        print(f"  {venue}: {count} games")

def analyze_teams(df):
    """Analyze data by team."""
    if 'home_team' not in df.columns or 'away_team' not in df.columns:
        print("\n=== Team Analysis ===")
        print("Required team columns missing in dataset.")
        return
    
    print("\n=== Team Analysis ===")
    
    # Get all teams (both home and away)
    home_teams = df['home_team'].value_counts()
    away_teams = df['away_team'].value_counts()
    
    # Combine home and away appearances
    all_teams = pd.concat([home_teams, away_teams], axis=1, sort=True).fillna(0)
    all_teams.columns = ['Home Games', 'Away Games']
    all_teams['Total Games'] = all_teams['Home Games'] + all_teams['Away Games']
    
    print("\nTop 10 teams by total games:")
    teams_by_total = all_teams.sort_values('Total Games', ascending=False).head(10)
    for team, row in teams_by_total.iterrows():
        print(f"  {team}: {int(row['Total Games'])} games ({int(row['Home Games'])} home, {int(row['Away Games'])} away)")

def analyze_scores(df):
    """Analyze game scores."""
    if 'home_score' not in df.columns or 'away_score' not in df.columns:
        print("\n=== Score Analysis ===")
        print("Required score columns missing in dataset.")
        return
    
    print("\n=== Score Analysis ===")
    
    # Convert scores to numeric if they're not already
    home_scores = pd.to_numeric(df['home_score'], errors='coerce')
    away_scores = pd.to_numeric(df['away_score'], errors='coerce')
    
    # Calculate total goals
    total_goals = home_scores.sum() + away_scores.sum()
    avg_goals_per_game = total_goals / len(df)
    
    print(f"Total goals: {total_goals}")
    print(f"Average goals per game: {avg_goals_per_game:.2f}")
    
    # Calculate home vs away advantage
    home_wins = len(df[home_scores > away_scores])
    away_wins = len(df[home_scores < away_scores])
    draws = len(df[home_scores == away_scores])
    
    print(f"Home wins: {home_wins} ({home_wins/len(df)*100:.2f}%)")
    print(f"Away wins: {away_wins} ({away_wins/len(df)*100:.2f}%)")
    print(f"Draws: {draws} ({draws/len(df)*100:.2f}%)")
    
    # Common score lines
    df['score_line'] = df['home_score'].astype(str) + '-' + df['away_score'].astype(str)
    score_lines = df['score_line'].value_counts().head(10)
    
    print("\nTop 10 most common score lines:")
    for score, count in score_lines.items():
        print(f"  {score}: {count} games ({count/len(df)*100:.2f}%)")

def create_visualizations(df, output_dir='.'):
    """Create visualizations for the dataset using Plotly."""
    print("\n=== Creating Visualizations ===")
    
    # Make sure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Games per year
    if 'year' in df.columns:
        year_counts = df['year'].value_counts().sort_index().reset_index()
        year_counts.columns = ['Year', 'Count']
        
        fig = px.bar(year_counts, x='Year', y='Count', 
                     title='Games per Year',
                     labels={'Count': 'Number of Games'},
                     text='Count')
                     
        fig.update_layout(
            title_font_size=24,
            xaxis_title_font_size=18,
            yaxis_title_font_size=18,
            template='plotly_white'
        )
        
        fig.write_html(f"{output_dir}/games_per_year.html")
        print(f"Saved games_per_year.html to {output_dir}")
    
    # 2. Home vs Away wins by year
    if all(col in df.columns for col in ['year', 'home_score', 'away_score']):
        # Create a copy of the dataframe to avoid modifying the original
        result_df = df.copy()
        result_df['result'] = 'Draw'
        result_df.loc[result_df['home_score'] > result_df['away_score'], 'result'] = 'Home Win'
        result_df.loc[result_df['home_score'] < result_df['away_score'], 'result'] = 'Away Win'
        
        # Count results by year
        result_counts = result_df.groupby(['year', 'result']).size().reset_index(name='count')
        result_pivot = result_counts.pivot(index='year', columns='result', values='count').fillna(0)
        
        # Calculate percentages
        result_pivot_pct = result_pivot.div(result_pivot.sum(axis=1), axis=0) * 100
        result_pivot_pct = result_pivot_pct.reset_index()
        
        # Melt for Plotly
        melted_df = pd.melt(result_pivot_pct, id_vars=['year'], 
                            value_vars=['Home Win', 'Away Win', 'Draw'] if all(x in result_pivot_pct.columns for x in ['Home Win', 'Away Win', 'Draw']) 
                            else result_pivot_pct.columns.drop('year'),
                            var_name='Result', value_name='Percentage')
        
        fig = px.bar(melted_df, x='year', y='Percentage', color='Result', 
                     barmode='stack',
                     title='Match Results by Year (Percentage)',
                     labels={'year': 'Year', 'Percentage': 'Percentage (%)', 'Result': 'Outcome'},
                     category_orders={"Result": ["Home Win", "Draw", "Away Win"]},
                     color_discrete_map={"Home Win": "lightgreen", "Draw": "lightblue", "Away Win": "salmon"})
        
        fig.update_layout(
            title_font_size=24,
            xaxis_title_font_size=18,
            yaxis_title_font_size=18,
            template='plotly_white'
        )
        
        fig.write_html(f"{output_dir}/results_by_year.html")
        print(f"Saved results_by_year.html to {output_dir}")
    
    # 3. Distribution of scores
    if all(col in df.columns for col in ['home_score', 'away_score']):
        # Convert scores to numeric if they're not already
        score_df = df.copy()
        score_df['home_score'] = pd.to_numeric(score_df['home_score'], errors='coerce')
        score_df['away_score'] = pd.to_numeric(score_df['away_score'], errors='coerce')
        
        # Create a pivot table for heatmap
        score_counts = score_df.groupby(['home_score', 'away_score']).size().reset_index(name='count')
        
        fig = px.density_heatmap(score_counts, x='home_score', y='away_score', z='count',
                                nbinsx=15, nbinsy=15,
                                title='Distribution of Match Scores',
                                labels={'home_score': 'Home Score', 'away_score': 'Away Score', 'count': 'Number of Games'})
        
        fig.update_layout(
            title_font_size=24,
            xaxis_title_font_size=18,
            yaxis_title_font_size=18,
            template='plotly_white'
        )
        
        fig.write_html(f"{output_dir}/score_distribution.html")
        print(f"Saved score_distribution.html to {output_dir}")
    
    # 4. Games per month
    if 'month' in df.columns:
        month_names = {
            1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
            7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
        }
        
        # Create a copy to avoid modifying the original dataframe
        month_df = df.copy()
        month_df['month_name'] = month_df['month'].map(month_names)
        
        # Count games by month
        month_counts = month_df['month_name'].value_counts().reset_index()
        month_counts.columns = ['Month', 'Count']
        
        # Ensure correct month order
        month_order = [month_names[i] for i in range(1, 13)]
        month_counts['Month'] = pd.Categorical(month_counts['Month'], categories=month_order, ordered=True)
        month_counts = month_counts.sort_values('Month')
        
        fig = px.bar(month_counts, x='Month', y='Count', 
                     title='Games per Month (All Years)',
                     labels={'Count': 'Number of Games'},
                     text='Count',
                     color_discrete_sequence=['indianred'])
        
        fig.update_layout(
            title_font_size=24,
            xaxis_title_font_size=18,
            yaxis_title_font_size=18,
            template='plotly_white'
        )
        
        fig.write_html(f"{output_dir}/games_per_month.html")
        print(f"Saved games_per_month.html to {output_dir}")
    
    # 5. Timeline of games (added visualization)
    if 'full_date' in df.columns:
        # Group by date and count games
        timeline_df = df.groupby('full_date').size().reset_index(name='games')
        
        fig = px.scatter(timeline_df, x='full_date', y='games',
                        title='Timeline of Games (2007-2025)',
                        labels={'full_date': 'Date', 'games': 'Number of Games'},
                        size='games', size_max=15,
                        color='games', color_continuous_scale='Viridis')
        
        fig.update_layout(
            title_font_size=24,
            xaxis_title_font_size=18,
            yaxis_title_font_size=18,
            template='plotly_white'
        )
        
        fig.write_html(f"{output_dir}/games_timeline.html")
        print(f"Saved games_timeline.html to {output_dir}")

def main():
    parser = argparse.ArgumentParser(description='Analyze NC Soccer dataset')
    parser.add_argument('--file', default='final_dataset.parquet', help='Path to Parquet file')
    parser.add_argument('--output', default='.', help='Directory for output visualizations')
    args = parser.parse_args()
    
    # Load the dataset
    df = load_dataset(args.file)
    
    # Run analyses
    general_stats(df)
    examine_columns(df)
    analyze_leagues(df)
    analyze_venues(df)
    analyze_teams(df)
    analyze_scores(df)
    
    # Create visualizations
    create_visualizations(df, args.output)
    
    print("\nAnalysis complete!")

if __name__ == '__main__':
    main()