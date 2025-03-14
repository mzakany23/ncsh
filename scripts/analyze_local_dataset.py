#!/usr/bin/env python3
"""
Analyze NC Soccer Dataset

This script performs an exploratory data analysis (EDA) on the locally built
NC Soccer dataset to verify data quality and completeness.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('analyze-dataset')

def load_dataset(dataset_path):
    """Load the dataset from Parquet file."""
    if not os.path.exists(dataset_path):
        logger.error(f"Dataset not found: {dataset_path}")
        return None
    
    logger.info(f"Loading dataset from {dataset_path}")
    try:
        df = pd.read_parquet(dataset_path)
        logger.info(f"Dataset loaded successfully with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}")
        return None

def analyze_data_completeness(df):
    """Analyze data completeness - check for missing values."""
    logger.info("\n=== DATA COMPLETENESS ANALYSIS ===")
    
    # Check for missing values
    missing_values = df.isnull().sum()
    missing_pct = (missing_values / len(df)) * 100
    
    # Create a DataFrame to display missing values information
    missing_info = pd.DataFrame({
        'Missing Values': missing_values,
        'Missing Percentage': missing_pct.round(2)
    })
    
    # Sort by missing percentage (descending)
    missing_info = missing_info.sort_values('Missing Percentage', ascending=False)
    
    print("\nMissing Values Analysis:")
    print(missing_info)
    
    # Highlight fields with high missing rates
    high_missing = missing_info[missing_info['Missing Percentage'] > 20]
    if not high_missing.empty:
        print("\nFields with high missing rates (>20%):")
        print(high_missing)
    
    return missing_info

def analyze_date_coverage(df):
    """Analyze date coverage to ensure all dates were scraped."""
    logger.info("\n=== DATE COVERAGE ANALYSIS ===")
    
    # Extract date fields
    if all(field in df.columns for field in ['year', 'month', 'day']):
        # Convert to datetime
        df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
        
        # Count games per date
        date_counts = df.groupby(df['date'].dt.date).size().reset_index(name='game_count')
        date_counts = date_counts.sort_values('date')
        
        print("\nDate Coverage:")
        print(f"Number of unique dates: {len(date_counts)}")
        
        # Check for dates with zero games
        min_games = date_counts['game_count'].min()
        max_games = date_counts['game_count'].max()
        print(f"Min games per date: {min_games}, Max games per date: {max_games}")
        
        # Identify dates with unusually low game counts
        low_game_dates = date_counts[date_counts['game_count'] < 5]
        if not low_game_dates.empty:
            print("\nDates with unusually low game counts (<5):")
            print(low_game_dates)
        
        # Plot games per date
        plt.figure(figsize=(12, 6))
        plt.bar(date_counts['date'].astype(str), date_counts['game_count'])
        plt.title('Games per Date')
        plt.xlabel('Date')
        plt.ylabel('Number of Games')
        plt.xticks(rotation=90)
        plt.tight_layout()
        
        # Save the plot to output directory
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
        plots_dir = os.path.join(output_dir, 'plots')
        os.makedirs(plots_dir, exist_ok=True)
        plot_path = os.path.join(plots_dir, 'games_per_date.png')
        plt.savefig(plot_path)
        logger.info(f"Games per date plot saved to {plot_path}")
        
        return date_counts
    else:
        logger.warning("Date fields (year, month, day) not found in dataset")
        return None

def analyze_fields(df):
    """Analyze key fields in the dataset to verify data quality."""
    logger.info("\n=== KEY FIELDS ANALYSIS ===")
    
    # Check leagues
    if 'league' in df.columns:
        league_counts = df['league'].value_counts().reset_index()
        league_counts.columns = ['league', 'count']
        
        print("\nLeagues Distribution:")
        print(f"Number of unique leagues: {df['league'].nunique()}")
        print("\nTop 10 leagues:")
        print(league_counts.head(10))
    
    # Check teams
    team_fields = []
    if 'home_team' in df.columns:
        team_fields.append('home_team')
    if 'away_team' in df.columns:
        team_fields.append('away_team')
    
    if team_fields:
        teams = []
        for field in team_fields:
            teams.extend(df[field].dropna().unique())
        
        unique_teams = len(set(teams))
        print(f"\nNumber of unique teams: {unique_teams}")
    
    # Check venues
    if 'venue' in df.columns:
        venue_counts = df['venue'].value_counts().reset_index()
        venue_counts.columns = ['venue', 'count']
        
        print("\nVenues Distribution:")
        print(f"Number of unique venues: {df['venue'].nunique()}")
        print("\nTop 10 venues:")
        print(venue_counts.head(10))
    
    # Check game status
    if 'status' in df.columns:
        status_counts = df['status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        
        print("\nGame Status Distribution:")
        print(status_counts)
        
        # Check if the status field contains times or scores
        time_pattern = r'^\d{1,2}:\d{2}\s*(?:AM|PM)?$'
        score_pattern = r'\d+-\d+'
        
        time_statuses = df['status'].str.match(time_pattern, na=False).sum()
        score_statuses = df['status'].str.contains(score_pattern, na=False).sum()
        
        print(f"\nStatus field analysis:")
        print(f"Statuses with time format: {time_statuses} ({time_statuses/len(df)*100:.2f}%)")
        print(f"Statuses with score format: {score_statuses} ({score_statuses/len(df)*100:.2f}%)")
    
    return {
        'leagues': df['league'].nunique() if 'league' in df.columns else 0,
        'teams': unique_teams if 'team_fields' in locals() else 0,
        'venues': df['venue'].nunique() if 'venue' in df.columns else 0
    }

def generate_summary_report(df, dataset_path):
    """Generate a summary report of the dataset."""
    logger.info("\n=== GENERATING SUMMARY REPORT ===")
    
    # Basic stats
    total_records = len(df)
    total_fields = len(df.columns)
    
    # Calculate the dataset size in MB
    dataset_size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
    
    # Generate the report
    report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'dataset_path': dataset_path,
        'dataset_size_mb': f"{dataset_size_mb:.2f} MB",
        'total_records': total_records,
        'total_fields': total_fields,
        'date_range': {
            'start_date': df['source_date'].min() if 'source_date' in df.columns else None,
            'end_date': df['source_date'].max() if 'source_date' in df.columns else None,
            'days_covered': df['source_date'].nunique() if 'source_date' in df.columns else None
        },
        'field_stats': {
            column: {
                'data_type': str(df[column].dtype),
                'unique_values': df[column].nunique(),
                'missing_count': df[column].isnull().sum(),
                'missing_percentage': f"{(df[column].isnull().sum() / len(df) * 100):.2f}%"
            }
            for column in df.columns
        }
    }
    
    # Print summary report
    print("\n=== DATASET SUMMARY REPORT ===")
    print(f"Dataset: {report['dataset_path']} ({report['dataset_size_mb']})")
    print(f"Analyzed at: {report['timestamp']}")
    print(f"Total records: {report['total_records']}")
    print(f"Total fields: {report['total_fields']}")
    
    if 'source_date' in df.columns:
        print(f"Date range: {report['date_range']['start_date']} to {report['date_range']['end_date']}")
        print(f"Days covered: {report['date_range']['days_covered']}")
    
    # Save report to file
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
    report_dir = os.path.join(output_dir, 'reports')
    os.makedirs(report_dir, exist_ok=True)
    
    report_path = os.path.join(report_dir, 'dataset_analysis_report.txt')
    
    with open(report_path, 'w') as f:
        f.write("=== DATASET SUMMARY REPORT ===\n")
        f.write(f"Dataset: {report['dataset_path']} ({report['dataset_size_mb']})\n")
        f.write(f"Analyzed at: {report['timestamp']}\n")
        f.write(f"Total records: {report['total_records']}\n")
        f.write(f"Total fields: {report['total_fields']}\n")
        
        if 'source_date' in df.columns:
            f.write(f"Date range: {report['date_range']['start_date']} to {report['date_range']['end_date']}\n")
            f.write(f"Days covered: {report['date_range']['days_covered']}\n")
        
        f.write("\n=== FIELD STATISTICS ===\n")
        for field, stats in report['field_stats'].items():
            f.write(f"\n{field}:\n")
            f.write(f"  Data type: {stats['data_type']}\n")
            f.write(f"  Unique values: {stats['unique_values']}\n")
            f.write(f"  Missing values: {stats['missing_count']} ({stats['missing_percentage']})\n")
    
    logger.info(f"Summary report saved to {report_path}")
    return report_path

def main():
    parser = argparse.ArgumentParser(description='Analyze the NC Soccer dataset')
    parser.add_argument('--dataset', default='output/dataset.parquet', help='Path to the dataset Parquet file')
    args = parser.parse_args()
    
    # Make the dataset path absolute if it's not already
    if not os.path.isabs(args.dataset):
        args.dataset = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.dataset)
    
    # Load the dataset
    df = load_dataset(args.dataset)
    if df is None:
        return 1
    
    # Print basic information
    print("\nDataset Shape:", df.shape)
    print("\nDataset Columns:")
    for col in df.columns:
        print(f"- {col}")
    
    print("\nDataset Sample:")
    print(df.head())
    
    print("\nDataset Info:")
    df.info()
    
    print("\nDataset Statistical Summary:")
    print(df.describe(include='all').T)
    
    # Analyze data completeness
    missing_info = analyze_data_completeness(df)
    
    # Analyze date coverage
    date_counts = analyze_date_coverage(df)
    
    # Analyze key fields
    field_stats = analyze_fields(df)
    
    # Generate summary report
    report_path = generate_summary_report(df, args.dataset)
    
    logger.info("\n=== ANALYSIS COMPLETE ===")
    logger.info(f"Summary report saved to {report_path}")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
