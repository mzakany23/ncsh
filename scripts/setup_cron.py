#!/usr/bin/env python3
"""
Setup script for cron jobs to automate NC Soccer scraping and processing.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime

def parse_args():
    parser = argparse.ArgumentParser(description='Setup cron jobs for NC Soccer data scraping')
    parser.add_argument('--daily', action='store_true', help='Set up daily scraping of current day')
    parser.add_argument('--daily-time', type=str, default='23:00',
                        help='Time for daily scrape in 24-hour format (default: 23:00)')
    parser.add_argument('--backfill', action='store_true', help='Set up a one-time backfill job')
    parser.add_argument('--start-year', type=int, default=2007, help='Start year for backfill (default: 2007)')
    parser.add_argument('--start-month', type=int, default=1, help='Start month for backfill (default: 1)')
    parser.add_argument('--end-year', type=int, default=datetime.now().year,
                        help='End year for backfill (default: current year)')
    parser.add_argument('--end-month', type=int, default=datetime.now().month,
                        help='End month for backfill (default: current month)')
    parser.add_argument('--force-scrape', action='store_true', help='Force re-scrape for all cron jobs')
    parser.add_argument('--aws-account', default='552336166511', help='AWS account number')

    return parser.parse_args()

def get_project_root():
    """Get the absolute path to the project root directory."""
    # Assume this script is in scripts/ directory
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

def create_cron_job(job_schedule, command, comment):
    """Create a cron job with the given schedule and command."""
    # Create a temporary file with the current crontab
    tmp_file = '/tmp/crontab.tmp'
    subprocess.run('crontab -l > {0} 2>/dev/null || touch {0}'.format(tmp_file), shell=True)

    # Check if the job already exists
    with open(tmp_file, 'r') as f:
        crontab_content = f.read()
        if command in crontab_content:
            print(f"Cron job already exists: {comment}")
            return False

    # Add the new cron job with a comment
    with open(tmp_file, 'a') as f:
        f.write(f"\n# {comment} - Added on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{job_schedule} {command}\n")

    # Install the new crontab
    result = subprocess.run(f'crontab {tmp_file}', shell=True)

    # Clean up
    os.remove(tmp_file)

    if result.returncode == 0:
        print(f"Successfully added cron job: {comment}")
        return True
    else:
        print(f"Failed to add cron job: {comment}")
        return False

def setup_daily_scrape(time_str, project_root, force_scrape, aws_account):
    """Set up a daily scrape cron job at the specified time."""
    # Parse the time string (format: HH:MM)
    try:
        hour, minute = map(int, time_str.split(':'))
        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError
    except ValueError:
        print(f"Invalid time format: {time_str}. Please use HH:MM format (24-hour).")
        return False

    # Create the cron schedule (minute hour * * *)
    schedule = f"{minute} {hour} * * *"

    # Build the command
    force_flag = "--force-scrape" if force_scrape else ""
    command = (f"cd {project_root} && "
               f"python scripts/daily_scrape.py --aws-account {aws_account} {force_flag} "
               f">> {project_root}/logs/daily_scrape_$(date +\\%Y\\%m\\%d).log 2>&1")

    # Create the cron job
    return create_cron_job(schedule, command, "NC Soccer Daily Scrape")

def setup_backfill_job(start_year, start_month, end_year, end_month, project_root, force_scrape, aws_account):
    """Set up a one-time backfill job to run in 5 minutes."""
    # Schedule for 5 minutes from now
    now = datetime.now()
    minute = (now.minute + 5) % 60
    hour = now.hour + ((now.minute + 5) // 60)
    hour = hour % 24
    day = now.day + (hour < now.hour)  # Increment day if hour rolled over

    # Create the cron schedule (minute hour day month *)
    schedule = f"{minute} {hour} {day} {now.month} *"

    # Build the command
    force_flag = "--force-scrape" if force_scrape else ""
    command = (f"cd {project_root} && "
               f"python scripts/backfill_scrape.py "
               f"--start-year {start_year} --start-month {start_month} "
               f"--end-year {end_year} --end-month {end_month} "
               f"--aws-account {aws_account} {force_flag} "
               f">> {project_root}/logs/backfill_$(date +\\%Y\\%m\\%d_\\%H\\%M).log 2>&1")

    # Create the cron job
    return create_cron_job(schedule, command, "NC Soccer Backfill (one-time)")

def ensure_log_directory(project_root):
    """Ensure the logs directory exists."""
    logs_dir = os.path.join(project_root, 'logs')
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
        print(f"Created logs directory: {logs_dir}")
    return logs_dir

def main():
    args = parse_args()
    project_root = get_project_root()

    # Ensure logs directory exists
    ensure_log_directory(project_root)

    # Setup requested cron jobs
    jobs_added = 0

    if args.daily:
        if setup_daily_scrape(args.daily_time, project_root, args.force_scrape, args.aws_account):
            jobs_added += 1

    if args.backfill:
        if setup_backfill_job(
            args.start_year, args.start_month,
            args.end_year, args.end_month,
            project_root, args.force_scrape, args.aws_account
        ):
            jobs_added += 1

    if jobs_added == 0:
        print("No cron jobs were added. Use --daily or --backfill options to set up jobs.")
        return 1

    print(f"Successfully added {jobs_added} cron job(s).")

    # Show the current crontab
    print("\nCurrent crontab:")
    subprocess.run('crontab -l', shell=True)

    return 0

if __name__ == '__main__':
    sys.exit(main())