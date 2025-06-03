"""RSS Feed ETL implementation."""

import feedparser
import pandas as pd
import gspread
import logging
import pytz
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..models.feeder import Feeder
from ..utils.auth import get_google_sheets_client
from ..utils.html_utils import create_html_cleaner
from .data_loader import DataLoader


class RSSFeedETL:
    """
    ETL process for RSS feeds with support for both merge_upsert and SCD2 patterns.

    This class handles the extraction, transformation, and loading of RSS feed data
    to Google Sheets, with the option to use either a simple merge_upsert approach
    or the SCD2 pattern for tracking historical changes.
    """

    def __init__(
        self,
        spreadsheet_id: str,
        config_sheet: str = "FeedConfig",
        creds_file: str = "secrets/service_account.json",
        timezone: str = "US/Central",
        use_scd2: bool = False,
        columns: Optional[List[str]] = None,
        primary_key: str = "link",
        loading_strategy: str = "scd1",
        target_worksheet: Optional[str] = None
    ):
        """
        Initialize the RSS Feed ETL.

        Args:
            spreadsheet_id: The ID of the Google Sheet
            config_sheet: The name of the sheet containing feed configuration
            creds_file: Path to the Google service account credentials file
            timezone: The timezone to use for date/time conversion
            use_scd2: Whether to use SCD2 pattern (legacy, overridden by loading_strategy)
            columns: List of columns to include in the output
            primary_key: Column to use as the primary key for merging
            loading_strategy: Data loading strategy ("scd1", "scd2", or "merge_upsert")
            target_worksheet: Override worksheet name for all feeds
        """
        self.spreadsheet_id = spreadsheet_id
        self.config_sheet = config_sheet
        self.creds_file = creds_file
        self.timezone = pytz.timezone(timezone)
        self.client = get_google_sheets_client(creds_file)
        self.html_cleaner = create_html_cleaner()
        self.target_worksheet = target_worksheet

        # Create data loader with appropriate strategy
        self.data_loader = DataLoader(
            primary_key=primary_key,
            columns=columns,
            use_scd2=use_scd2,
            loading_strategy=loading_strategy
        )

        # Define required columns for feed configuration
        self.required_columns = {
            "title", "reader", "time", "url", "worksheet_name"
        }

    def read_feeders(self) -> List[Feeder]:
        """
        Read feed configurations from the Google Sheet.

        Returns:
            List of Feeder objects

        Raises:
            ValueError: If required columns are missing
        """
        sheet = self.client.open_by_key(self.spreadsheet_id)
        worksheet = sheet.worksheet(self.config_sheet)
        df = pd.DataFrame(worksheet.get_all_records())

        # Normalize column names
        df.columns = [col.strip().lower() for col in df.columns]

        # Check for required columns
        missing = self.required_columns - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Create Feeder objects
        return [
            Feeder(
                title=row["title"].strip(),
                reader=row["reader"].strip(),
                time_window=row["time"].strip(),
                url=row["url"].strip(),
                worksheet_name=row["worksheet_name"].strip(),
                job_title=row.get("job_title", row["title"]).strip()  # Use title as fallback
            ) for _, row in df.iterrows()
        ]

    def parse_feed(self, feeder: Feeder) -> pd.DataFrame:
        """
        Parse an RSS feed and extract job entries.

        Args:
            feeder: Feeder configuration object

        Returns:
            DataFrame containing parsed feed entries
        """
        feed = feedparser.parse(feeder.url)
        records = []

        for entry in feed.entries:
            # Get published date
            published_raw = entry.get("published") or entry.get("updated") or entry.get("created")

            if published_raw:
                try:
                    published = pd.to_datetime(published_raw)
                    # Convert to specified timezone
                    if published.tzinfo is not None:
                        published = published.tz_convert(self.timezone)
                    else:
                        published = published.tz_localize("UTC").tz_convert(self.timezone)
                    published_str = published.strftime("%Y-%m-%d %H:%M:%S")
                except Exception as ex:
                    logging.warning(f"⚠️ Failed to convert date for {entry.get('title')}: {ex}")
                    published_str = ""
            else:
                logging.warning(f"⚠️ No published/updated/created date for {entry.get('title')}")
                published_str = datetime.now(pytz.utc).astimezone(self.timezone).strftime("%Y-%m-%d %H:%M:%S")

            # Clean HTML from summary
            summary_raw = self.html_cleaner.handle(entry.get("summary", "")).strip()
            summary_clean = re.sub(r"\s+", " ", summary_raw)

            # Create record
            records.append({
                "job_title": feeder.job_title,
                "link": entry.get("link", "").strip(),
                "entry_title": entry.get("title", "").strip(),
                "published": published_str,
                "feed_title": feeder.title,
                "reader": feeder.reader,
                "time_window": feeder.time_window,
                "summary": summary_clean,
                "notes": ""
            })

        df = pd.DataFrame(records)

        # Log sample of extracted data
        if not df.empty:
            logging.info(f"📥 Extracted {len(df)} entries from RSS feed:")
            for idx, row in df.head(2).iterrows():  # Show first 2 records
                job_title = row.get('job_title', 'N/A')
                entry_title = row.get('entry_title', 'N/A')[:50]
                link = row.get('link', 'N/A')[:60]
                logging.info(f"  {idx + 1}. {job_title} | {entry_title}... | {link}...")

        return df

    def process_feed(self, feeder: Feeder, dry_run: bool = False) -> Dict[str, Any]:
        """
        Process a single feed: extract, transform, and load.

        Args:
            feeder: Feeder configuration object
            dry_run: If True, don't actually update Google Sheets

        Returns:
            Dictionary with results
        """
        # Extract and transform
        df = self.parse_feed(feeder)

        if df.empty:
            return {
                "success": False,
                "message": f"No entries found for: {feeder.title}",
                "count": 0
            }

        if dry_run:
            # In dry run mode, simulate the results without updating Google Sheets
            logging.info(f"DRY RUN: Would process {len(df)} entries for {feeder.title}")

            # Return simulated results
            return {
                "success": True,
                "message": f"Processed {len(df)} entries (dry run)",
                "count": len(df),
                "inserted": len(df),  # Simulate all as new in dry run
                "updated": 0,
                "removed": 0,
                "dry_run": True
            }
        else:
            # Use target worksheet if specified, otherwise use feeder's worksheet
            worksheet_name = self.target_worksheet or feeder.worksheet_name

            # Load data to Google Sheets
            results = self.data_loader.load_to_sheet(
                client=self.client,
                new_df=df,
                spreadsheet_id=self.spreadsheet_id,
                worksheet_name=worksheet_name
            )

            return {
                "success": True,
                "message": f"Processed {len(df)} entries",
                "count": len(df),
                "inserted": results["inserted"],
                "updated": results["updated"],
                "removed": results.get("removed", 0),
                "dry_run": False
            }

    def run(self, dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Run the ETL process for all configured feeds.

        Args:
            dry_run: If True, don't actually update Google Sheets

        Returns:
            List of result dictionaries, one per feed
        """
        feeders = self.read_feeders()
        logging.info(f"Found {len(feeders)} configured feed(s).")

        if dry_run:
            logging.info("DRY RUN MODE: No changes will be made to Google Sheets")

        results = []

        for feeder in feeders:
            # Determine actual target worksheet
            actual_worksheet = self.target_worksheet or feeder.worksheet_name
            logging.info(f"\nProcessing: {feeder.title} | Target Sheet: {actual_worksheet}")

            try:
                # Pass the dry_run parameter to process_feed
                result = self.process_feed(feeder, dry_run=dry_run)

                if result["success"]:
                    if dry_run:
                        logging.info(
                            f"DRY RUN: Would process {result['count']} entries for {feeder.title}"
                        )
                    else:
                        logging.info(
                            f"SUCCESS: {result['message']}: "
                            f"Inserted: {result['inserted']}, "
                            f"Updated: {result['updated']}"
                            + (f", Removed: {result['removed']}" if "removed" in result else "")
                        )
                else:
                    logging.warning(f"WARNING: {result['message']}")

                results.append({
                    "feeder": feeder.title,
                    "worksheet": feeder.worksheet_name,
                    **result
                })
            except Exception as e:
                logging.error(f"❌ Error processing feed '{feeder.title}': {str(e)}")
                logging.debug("Exception details:", exc_info=True)
                results.append({
                    "feeder": feeder.title,
                    "worksheet": feeder.worksheet_name,
                    "success": False,
                    "message": f"Error: {str(e)}",
                    "count": 0,
                    "dry_run": dry_run
                })

        return results
