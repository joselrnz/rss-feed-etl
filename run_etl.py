#!/usr/bin/env python
"""
Run the RSS Feed ETL process.

This script extracts data from RSS feeds, transforms it, and loads it into Google Sheets.
It supports both merge_upsert and SCD2 patterns for tracking changes.
"""

import os
import sys
import argparse
import logging
import yaml
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from src.rss_feed_etl.core.etl import RSSFeedETL


# Configure logging
def setup_logging(log_level=logging.INFO, log_to_file=False):
    """Set up logging configuration."""
    log_format = "%(asctime)s - %(levelname)s - %(message)s"

    # Create logs directory if it doesn't exist and we're logging to file
    if log_to_file:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Create a log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"etl_{timestamp}.log"

        # Configure logging to file and console
        handlers = [
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]

        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=handlers
        )
    else:
        # Configure logging to console only
        logging.basicConfig(
            level=log_level,
            format=log_format
        )

    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("googleapiclient").setLevel(logging.WARNING)


def load_environment():
    """Load environment variables based on the current environment."""
    # Determine environment (default to development)
    env = os.environ.get("ENVIRONMENT", "development").lower()

    # Log the environment
    logging.info(f"🌍 Running in {env.upper()} environment")

    # Try environment-specific .env file first
    env_file = Path(f"config/.env.{env}")
    if env_file.exists():
        logging.info(f"📄 Loading environment from {env_file}")
        load_dotenv(env_file)
        return

    # Fall back to regular .env files
    for env_path in [Path("config/.env"), Path(".env")]:
        if env_path.exists():
            logging.info(f"📄 Loading environment from {env_path}")
            load_dotenv(env_path)
            return

    # If we get here, no .env file was found
    logging.warning("⚠️ No .env file found. Using environment variables or defaults.")


def load_config(config_path="config/config.yaml"):
    """Load configuration from YAML file."""
    config = {}
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f) or {}
            logging.info(f"Loaded configuration from {config_path}")
        else:
            logging.warning(f"Configuration file not found: {config_path}")
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
    return config


def main():
    """Run the RSS Feed ETL process."""
    # Parse command-line arguments for logging
    parser = argparse.ArgumentParser(description="Run the RSS Feed ETL process")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Log to file in addition to console"
    )

    # Parse just the logging arguments first
    log_args, _ = parser.parse_known_args()

    # Set up logging
    log_level = getattr(logging, log_args.log_level)
    setup_logging(log_level=log_level, log_to_file=log_args.log_to_file)

    # Load environment variables
    load_environment()

    # Parse ETL-specific command-line arguments
    parser = argparse.ArgumentParser(description="Run the RSS Feed ETL process")

    # Add logging arguments again (for help text)
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Log to file in addition to console"
    )

    # Add ETL-specific arguments
    parser.add_argument(
        "--config",
        help="Path to configuration file",
        default="config/config.yaml"
    )
    parser.add_argument(
        "--spreadsheet_id",
        help="Google Spreadsheet ID",
        default=os.environ.get("GOOGLE_SPREADSHEET_ID", "")
    )
    parser.add_argument(
        "--config_sheet",
        help="Name of the sheet containing feed configuration",
        default=os.environ.get("GOOGLE_CONFIG_SHEET", "JobFeedsConfig")
    )
    parser.add_argument(
        "--creds_file",
        help="Path to Google service account credentials file",
        default=os.environ.get("GOOGLE_CREDS_FILE_PATH", "secrets/service_account.json")
    )
    parser.add_argument(
        "--timezone",
        help="Timezone for date/time conversion",
        default=os.environ.get("TIMEZONE", "US/Central")
    )
    parser.add_argument(
        "--use_scd2",
        action="store_true",
        help="Use SCD2 pattern for tracking historical changes (legacy)"
    )
    parser.add_argument(
        "--loading_strategy",
        choices=["scd1", "scd2", "merge_upsert"],
        help="Data loading strategy: scd1 (simple overwrite), scd2 (full history), merge_upsert (legacy)"
    )
    parser.add_argument(
        "--target_worksheet",
        help="Override worksheet name for all feeds (e.g., 'StageData')"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without updating Google Sheets"
    )

    # Parse arguments
    args = parser.parse_args()

    # Validate required arguments
    if not args.spreadsheet_id:
        logging.error("ERROR: Spreadsheet ID is required. Provide it with --spreadsheet_id or set the GOOGLE_SPREADSHEET_ID environment variable.")
        sys.exit(1)

    # Check if credentials file exists
    creds_path = Path(args.creds_file)
    if not creds_path.exists():
        logging.error(f"ERROR: Credentials file not found: {args.creds_file}")
        logging.error("Please make sure your Google service account credentials file exists.")
        sys.exit(1)

    # Start the ETL process
    start_time = datetime.now()
    logging.info("Starting RSS Feed ETL process")
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Spreadsheet ID: {args.spreadsheet_id}")
    logging.info(f"Config Sheet: {args.config_sheet}")
    logging.info(f"Using {'SCD2' if args.use_scd2 else 'merge_upsert'} pattern")

    if args.dry_run:
        logging.info("DRY RUN MODE: No changes will be made to Google Sheets")

    try:
        # Load configuration
        config = load_config(args.config)
        etl_config = config.get("etl", {})

        # Determine loading strategy
        loading_strategy = (
            args.loading_strategy or
            etl_config.get("loading_strategy", "scd1")
        )

        # Handle legacy use_scd2 flag
        if args.use_scd2 or etl_config.get("use_scd2", False):
            loading_strategy = "scd2"

        # Get target worksheet from config or command line
        target_worksheet = args.target_worksheet or etl_config.get("target_worksheet")

        # Initialize the ETL process
        etl = RSSFeedETL(
            spreadsheet_id=args.spreadsheet_id,
            config_sheet=args.config_sheet,
            creds_file=args.creds_file,
            timezone=args.timezone,
            use_scd2=args.use_scd2,
            columns=etl_config.get("columns"),
            primary_key=etl_config.get("primary_key", "link"),
            loading_strategy=loading_strategy,
            target_worksheet=target_worksheet
        )

        # Run the ETL process
        results = etl.run(dry_run=args.dry_run)

        # Calculate summary statistics
        total_processed = sum(result.get("count", 0) for result in results)
        total_inserted = sum(result.get("inserted", 0) for result in results)
        total_updated = sum(result.get("updated", 0) for result in results)
        total_removed = sum(result.get("removed", 0) for result in results if "removed" in result)

        # Calculate elapsed time
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = elapsed_time.total_seconds()

        # Print summary
        logging.info("\nETL Summary:")
        logging.info(f"Total runtime: {elapsed_seconds:.2f} seconds")
        logging.info(f"Feeds processed: {len(results)}")
        logging.info(f"Entries processed: {total_processed}")
        logging.info(f"Inserted: {total_inserted}")
        logging.info(f"Updated: {total_updated}")

        if args.use_scd2:
            logging.info(f"Removed: {total_removed}")

        if args.dry_run:
            logging.info("DRY RUN COMPLETED: No changes were made to Google Sheets")
        else:
            logging.info("ETL process completed successfully")

        # Return success exit code
        return 0

    except KeyboardInterrupt:
        logging.warning("\nETL process interrupted by user")
        return 130  # Standard exit code for SIGINT

    except Exception as e:
        # Log the full exception with traceback
        logging.error(f"ETL process failed: {str(e)}")
        logging.debug("Exception details:", exc_info=True)

        # Return error exit code
        return 1


if __name__ == "__main__":
    sys.exit(main())
