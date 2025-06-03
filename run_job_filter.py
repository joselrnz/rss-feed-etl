#!/usr/bin/env python3
"""
Run the Job Filtering process.

This script filters job listings from StageData (after ETL) and saves
the filtered results to TexasJobs worksheet.
"""

import os
import sys
import argparse
import logging
import yaml
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import pytz

# Import from your src directory
from src.rss_feed_etl.utils.auth import get_google_sheets_client


def setup_logging(log_level=logging.INFO):
    """Set up logging configuration."""
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format)

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)


def load_config(config_path="config/config.yaml"):
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        return None


def load_environment():
    """Load environment variables."""
    env = os.environ.get("ENVIRONMENT", "development").lower()
    logging.info(f"🌍 Running in {env.upper()} environment")

    env_file = Path(f"config/.env.{env}")
    if env_file.exists():
        logging.info(f"📄 Loading environment from {env_file}")
        load_dotenv(env_file)
    else:
        logging.warning("⚠️ No .env file found. Using environment variables.")


def read_worksheet(client, spreadsheet_id, worksheet_name):
    """Read data from a Google Sheet worksheet."""
    try:
        sheet = client.open_by_key(spreadsheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)

        if df.empty:
            logging.warning(f"No records found in worksheet: {worksheet_name}")
        else:
            logging.info(f"📊 Read {len(df)} records from {worksheet_name}")

        return df
    except Exception as e:
        logging.error(f"Error reading worksheet {worksheet_name}: {e}")
        return pd.DataFrame()


def write_worksheet(client, df, spreadsheet_id, worksheet_name, clear_first=True):
    """Write data to a Google Sheet worksheet."""
    try:
        sheet = client.open_by_key(spreadsheet_id)

        # Check if worksheet exists, create if not
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except:
            logging.info(f"📝 Creating new worksheet: {worksheet_name}")
            worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

        # Clear the worksheet only if requested (for overwrite mode)
        if clear_first:
            worksheet.clear()
            logging.info(f"🗑️ Cleared existing data in {worksheet_name}")

        if df.empty:
            logging.warning(f"No data to write to {worksheet_name}")
            return True

        # Convert all columns to string
        for col in df.columns:
            df[col] = df[col].astype(str)

        # Update the worksheet
        worksheet.update([df.columns.tolist()] + df.values.tolist())
        logging.info(f"✅ Wrote {len(df)} records to {worksheet_name}")

        return True
    except Exception as e:
        logging.error(f"Error writing to worksheet {worksheet_name}: {e}")
        return False


def filter_by_date(df, config):
    """Filter data by published date."""
    filter_config = config.get("job_filter", {})
    date_filter_config = filter_config.get("date_filter", {})

    if not date_filter_config.get("enabled", True):
        logging.info("📅 Date filtering disabled")
        return df

    date_column = date_filter_config.get("column", "published")
    days_back = date_filter_config.get("days_back", 7)

    if date_column not in df.columns:
        logging.warning(f"📅 Date column '{date_column}' not found, skipping date filtering")
        return df

    if days_back <= 0:
        logging.info("📅 days_back = 0, including all data")
        return df

    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=days_back)
    logging.info(f"📅 Filtering for jobs published after: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")

    # Convert published column to datetime
    try:
        df_copy = df.copy()
        df_copy[date_column] = pd.to_datetime(df_copy[date_column], errors='coerce')

        # Filter by date
        date_mask = df_copy[date_column] >= cutoff_date
        filtered_df = df_copy[date_mask].copy()

        filtered_count = len(df) - len(filtered_df)
        logging.info(f"📅 Date filter removed {filtered_count} old records, {len(filtered_df)} remaining")

        return filtered_df

    except Exception as e:
        logging.warning(f"📅 Error filtering by date: {e}, skipping date filtering")
        return df


def filter_empty_content(df, config):
    """Filter out records with empty or missing content."""
    if df.empty:
        return df

    filter_config = config.get("job_filter", {})
    require_content_config = filter_config.get("require_content", {})

    if not require_content_config.get("enabled", True):
        logging.info("📝 Empty content filtering disabled")
        return df

    required_columns = require_content_config.get("columns", ["summary"])

    if not required_columns:
        logging.info("📝 No columns specified for content requirement")
        return df

    logging.info(f"📝 Filtering records with empty content in columns: {required_columns}")

    # Start with all rows
    mask = pd.Series(True, index=df.index)

    # Check each required column
    for column in required_columns:
        if column in df.columns:
            # Filter out rows where the column is empty, null, or whitespace-only
            column_mask = (
                df[column].notna() &  # Not null/NaN
                (df[column].astype(str).str.strip() != '') &  # Not empty string
                (df[column].astype(str).str.strip() != 'nan')  # Not string 'nan'
            )

            empty_count = (~column_mask).sum()
            if empty_count > 0:
                logging.info(f"    📝 Removed {empty_count} records with empty '{column}'")

            mask = mask & column_mask
        else:
            logging.warning(f"📝 Required column '{column}' not found in data")

    # Apply the mask
    filtered_df = df[mask].copy()

    total_removed = len(df) - len(filtered_df)
    if total_removed > 0:
        logging.info(f"📝 Removed {total_removed} records with empty content, {len(filtered_df)} remaining")

    return filtered_df


def filter_data(df, config):
    """Filter data based on keywords."""
    if df.empty:
        return df

    filter_config = config.get("job_filter", {})
    exclude_by_column = filter_config.get("exclude_by_column", {})
    case_sensitive = filter_config.get("case_sensitive", False)

    logging.info(f"🔍 Filtering {len(df)} records...")

    # Start with all rows
    mask = pd.Series(True, index=df.index)

    # Apply column-specific exclude filters
    for column, keywords in exclude_by_column.items():
        if column in df.columns and keywords:
            logging.info(f"  📋 Applying {len(keywords)} exclude filters to column: {column}")

            for keyword in keywords:
                keyword_mask = df[column].str.contains(keyword, case=case_sensitive, na=False)
                mask = mask & ~keyword_mask

                filtered_count = keyword_mask.sum()
                if filtered_count > 0:
                    logging.info(f"    ❌ '{keyword}' filtered out {filtered_count} records")

    # Apply the mask
    filtered_df = df[mask].copy()

    logging.info(f"🎯 Filtered out {len(df) - len(filtered_df)} records, {len(filtered_df)} remaining")
    return filtered_df


def add_as_of_dt(df, config):
    """Add AS_OF_DT column with current timestamp."""
    filter_config = config.get("job_filter", {})

    if not filter_config.get("add_as_of_dt", True):
        logging.info("⏰ AS_OF_DT column disabled")
        return df

    # Add AS_OF_DT column with current timestamp
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_copy = df.copy()
    df_copy['AS_OF_DT'] = current_time

    logging.info(f"⏰ Added AS_OF_DT column: {current_time}")
    return df_copy


def main():
    """Run the job filtering process."""
    parser = argparse.ArgumentParser(description="Filter jobs from StageData to TexasJobs")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--spreadsheet_id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--loading-mode", choices=["append", "overwrite"], help="Override loading mode from config")
    parser.add_argument("--days-back", type=int, help="Override days_back from config (0 = all data)")
    parser.add_argument("--no-as-of-dt", action="store_true", help="Disable AS_OF_DT column")

    args = parser.parse_args()

    # Set up logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level=log_level)

    # Load environment and config
    load_environment()
    config = load_config(args.config)
    if not config:
        return 1

    # Get configuration
    filter_config = config.get("job_filter", {})
    source_worksheet = filter_config.get("source_worksheet", "StageData")
    output_worksheet = filter_config.get("output_worksheet", "TexasJobs")

    # Apply command line overrides
    if args.loading_mode:
        filter_config["loading_mode"] = args.loading_mode
        logging.info(f"🔧 Override loading_mode: {args.loading_mode}")

    if args.days_back is not None:
        if "date_filter" not in filter_config:
            filter_config["date_filter"] = {}
        filter_config["date_filter"]["days_back"] = args.days_back
        logging.info(f"🔧 Override days_back: {args.days_back}")

    if args.no_as_of_dt:
        filter_config["add_as_of_dt"] = False
        logging.info(f"🔧 Override add_as_of_dt: False")

    # Get spreadsheet ID
    spreadsheet_id = args.spreadsheet_id or os.environ.get("GOOGLE_SPREADSHEET_ID", "")
    if not spreadsheet_id:
        logging.error("❌ Spreadsheet ID required")
        return 1

    # Get credentials
    creds_file = os.environ.get("GOOGLE_CREDS_FILE_PATH", "secrets/service_account.json")
    if not Path(creds_file).exists():
        logging.error(f"❌ Credentials file not found: {creds_file}")
        return 1

    start_time = datetime.now()
    logging.info("🚀 Starting job filtering process")
    logging.info(f"📊 Source: {source_worksheet} → Target: {output_worksheet}")

    if args.dry_run:
        logging.info("🧪 DRY RUN MODE: No changes will be made")

    try:
        # Get Google Sheets client
        client = get_google_sheets_client(creds_file)

        # Read data from StageData
        source_df = read_worksheet(client, spreadsheet_id, source_worksheet)
        if source_df.empty:
            logging.warning("❌ No data found in source worksheet")
            return 0

        # Step 1: Filter by date (if enabled)
        date_filtered_df = filter_by_date(source_df, config)
        if date_filtered_df.empty:
            logging.warning("❌ No data after date filtering")
            return 0

        # Step 2: Filter out empty content (if enabled)
        content_filtered_df = filter_empty_content(date_filtered_df, config)
        if content_filtered_df.empty:
            logging.warning("❌ No data after empty content filtering")
            return 0

        # Step 3: Filter by keywords
        keyword_filtered_df = filter_data(content_filtered_df, config)
        if keyword_filtered_df.empty:
            logging.warning("❌ No data after keyword filtering")
            return 0

        # Step 4: Add AS_OF_DT timestamp
        final_filtered_df = add_as_of_dt(keyword_filtered_df, config)

        # Step 5: Handle loading mode (append vs overwrite)
        loading_mode = filter_config.get("loading_mode", "append")
        logging.info(f"📝 Loading mode: {loading_mode}")

        if loading_mode == "append":
            try:
                existing_df = read_worksheet(client, spreadsheet_id, output_worksheet)
                if not existing_df.empty:
                    logging.info(f"📊 Found {len(existing_df)} existing records in {output_worksheet}")
                    logging.info(f"📊 Adding {len(final_filtered_df)} new filtered records")

                    # Combine new data with existing data
                    combined_df = pd.concat([final_filtered_df, existing_df], ignore_index=True)
                    logging.info(f"📊 Combined total: {len(combined_df)} records")

                    # Deduplicate by link
                    if "link" in combined_df.columns:
                        before_dedup = len(combined_df)
                        combined_df = combined_df.drop_duplicates(subset=["link"], keep="first")
                        after_dedup = len(combined_df)
                        duplicates_removed = before_dedup - after_dedup
                        logging.info(f"🔗 Removed {duplicates_removed} duplicates, {after_dedup} unique records remain")

                    final_df = combined_df
                else:
                    logging.info(f"📊 No existing data in {output_worksheet}, using filtered data only")
                    final_df = final_filtered_df
            except Exception as e:
                logging.info(f"📝 Error reading existing data ({e}), using filtered data only")
                final_df = final_filtered_df
        else:  # overwrite mode
            logging.info("📝 Overwrite mode: replacing all existing data")
            final_df = final_filtered_df

        # Write to output worksheet
        if not args.dry_run:
            # In append mode, we've already combined the data, so we always clear and write the full dataset
            # In overwrite mode, we also clear and write
            success = write_worksheet(client, final_df, spreadsheet_id, output_worksheet, clear_first=True)
            if not success:
                return 1
        else:
            logging.info(f"🧪 DRY RUN: Would write {len(final_df)} records")

        # Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        logging.info(f"\n📋 Job Filtering Summary:")
        logging.info(f"  ⏱️  Runtime: {elapsed:.2f} seconds")
        logging.info(f"  📥 Source records: {len(source_df)}")
        logging.info(f"  📅 Date filtered: {len(date_filtered_df)}")
        logging.info(f"  📝 Content filtered: {len(content_filtered_df)}")
        logging.info(f"  🎯 Keyword filtered: {len(keyword_filtered_df)}")
        logging.info(f"  📤 Final records: {len(final_df)}")
        logging.info(f"  📝 Loading mode: {loading_mode}")
        logging.info(f"✅ Job filtering completed successfully")

        return 0

    except Exception as e:
        logging.error(f"❌ Job filtering failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
