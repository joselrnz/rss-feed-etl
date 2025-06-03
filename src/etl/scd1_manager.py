import pandas as pd
from datetime import datetime, timezone
from typing import List, Tuple
import logging

# Use standard logging instead of custom logger to avoid import issues
logger = logging.getLogger(__name__)


def merge_scd1(new_df: pd.DataFrame, hist: pd.DataFrame) -> Tuple[pd.DataFrame, int, int]:
    """
    Performs a Slowly Changing Dimension Type 1 merge between new data and historical data.

    SCD1 simply overwrites old data with new data - no historical tracking.
    This is simpler and more storage-efficient than SCD2.

    Logic:
    - Records in new_df that match existing records in hist (by link) will UPDATE the historical record
    - Records in new_df that don't exist in hist will be INSERTED as new records
    - Records in hist that are not present in new_df will be PRESERVED (no deletion)

    Args:
        new_df: DataFrame containing the latest data (e.g., from RSS feed).
                Expected columns: 'link', 'entry_title', 'published', 'feed_title',
                'reader', 'time_window', 'summary', 'job_title', 'notes'.
        hist: DataFrame containing the historical data from the target sheet.

    Returns:
        A tuple of (merged DataFrame, inserted count, updated count).
        The merged DataFrame contains all historical records plus new/updated ones.
    """
    key = "link"

    # Define all columns expected in the final DataFrame
    expected_cols = [
        "job_title", "link", "entry_title", "published", "feed_title",
        "reader", "time_window", "summary", "notes"
    ]

    # Ensure new_df has all expected columns, adding empty if missing
    for col in expected_cols:
        if col not in new_df.columns:
            new_df[col] = ""
        # Convert to string for reliable comparison/merging
        new_df[col] = new_df[col].astype(str)

    # Ensure hist DataFrame has all expected columns, adding empty if missing
    for col in expected_cols:
        if col not in hist.columns:
            hist[col] = ""
            logger.warning(f"Added missing column '{col}' to history DataFrame.")
        # Convert to string for reliable comparison/merging
        hist[col] = hist[col].astype(str)

    # If history is empty, all records are new inserts
    if hist.empty:
        logger.info(f"SCD1 Merge: History is empty. Inserting {len(new_df)} new records.")
        result_df = new_df[expected_cols].copy()
        return result_df, len(new_df), 0

    # Merge new data with historical data to identify relationships
    merged = new_df[expected_cols].merge(
        hist[expected_cols],
        on=key,
        how="outer",
        indicator=True,
        suffixes=("", "_hist")
    )

    # Identify different types of records
    new_records = merged[merged["_merge"] == "left_only"]  # In new_df only
    existing_records = merged[merged["_merge"] == "both"]  # In both
    removed_records = merged[merged["_merge"] == "right_only"]  # In hist only

    logger.info(f"SCD1 Merge Analysis:")
    logger.info(f"  - New records to insert: {len(new_records)}")
    logger.info(f"  - Existing records to check for updates: {len(existing_records)}")
    logger.info(f"  - Historical records to preserve: {len(removed_records)}")

    # For SCD1, we preserve all historical data and add/update with new data
    # This handles:
    # - Inserts: New records that weren't in history are added
    # - Updates: Existing records get their latest values
    # - Preserves: Historical records not in new_df are kept (NO DELETION)

    if hist.empty:
        # No historical data, just use new data
        result_df = new_df[expected_cols].copy()
    else:
        # Start with all historical data (PRESERVE ALL)
        result_df = hist[expected_cols].copy()

        # Update existing records and add new ones
        new_links = set(new_df[key].astype(str))
        hist_links = set(hist[key].astype(str))

        # Update existing records (overwrite with new data)
        update_links = new_links & hist_links
        for link in update_links:
            new_record = new_df[new_df[key].astype(str) == link].iloc[0]
            # Update the record in result_df
            mask = result_df[key].astype(str) == link
            for col in new_df.columns:
                if col in result_df.columns:
                    result_df.loc[mask, col] = new_record[col]

        # Insert new records (append to existing data)
        insert_links = new_links - hist_links
        if insert_links:
            new_records = new_df[new_df[key].astype(str).isin(insert_links)]
            result_df = pd.concat([result_df, new_records[expected_cols]], ignore_index=True)

    # Preserve notes from historical data where possible
    # Notes are user-generated content that should be preserved across updates
    if not hist.empty and "notes" in hist.columns:
        # Create a mapping of link -> notes from historical data
        hist_notes = hist.set_index(key)["notes"].to_dict()

        # For each record in the result, preserve existing notes if they exist
        # and the new record doesn't already have notes
        for idx, row in result_df.iterrows():
            link_value = row[key]
            current_notes = str(row.get("notes", "")).strip()

            # If current record has no notes but history has notes for this link, preserve them
            if not current_notes and link_value in hist_notes:
                hist_note = str(hist_notes[link_value]).strip()
                if hist_note:
                    result_df.at[idx, "notes"] = hist_note
                    logger.debug(f"Preserved notes for link: {link_value}")

    # Calculate counts for reporting
    if hist.empty:
        inserted = len(result_df)
        updated = 0
    else:
        # Count actual changes by comparing with history
        hist_links = set(hist[key].astype(str))
        new_links = set(result_df[key].astype(str))

        inserted = len(new_links - hist_links)  # Links in new but not in history
        updated = len(new_links & hist_links)   # Links in both (potential updates)

    logger.info(f"SCD1 Merge Results:")
    logger.info(f"  - Total records in result: {len(result_df)}")
    logger.info(f"  - New records inserted: {inserted}")
    logger.info(f"  - Existing records updated: {updated}")

    # Log sample of data being processed
    if not result_df.empty:
        logger.info(f"📋 Sample of data in result:")
        for idx, row in result_df.head(3).iterrows():  # Show first 3 records
            job_title = row.get('job_title', 'N/A')
            entry_title = row.get('entry_title', 'N/A')[:50]  # Truncate long titles
            link = row.get('link', 'N/A')[:60]  # Truncate long URLs
            logger.info(f"  {idx + 1}. {job_title} | {entry_title}... | {link}...")

    # Sort by published date if possible, then by link for consistent ordering
    try:
        # Try to convert published to datetime for proper sorting
        result_df['published_dt'] = pd.to_datetime(result_df['published'], errors='coerce', utc=True)
        result_df = result_df.sort_values(['published_dt', key], ascending=[False, True])
        result_df = result_df.drop(columns=['published_dt'])
    except Exception as e:
        logger.warning(f"Could not sort by published date: {e}. Sorting by link only.")
        result_df = result_df.sort_values(key)

    # Ensure all columns are string type for writing to Google Sheets
    for col in result_df.columns:
        result_df[col] = result_df[col].astype(str)

    # Final validation - ensure we have the expected columns in the right order
    result_df = result_df[expected_cols]

    logger.info(f"SCD1 Merge complete. Final dataset has {len(result_df)} records.")
    return result_df, inserted, updated


def validate_scd1_data(df: pd.DataFrame) -> bool:
    """
    Validate that the DataFrame is suitable for SCD1 processing.

    Args:
        df: DataFrame to validate

    Returns:
        True if valid, False otherwise
    """
    # Only require the primary key column (link) - other columns are optional
    # The ETL process will ensure all expected columns exist before calling this
    required_cols = ["link"]

    # Check for required columns
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"SCD1 validation failed: Missing required columns: {missing_cols}")
        return False

    # Check for empty link column (primary key)
    if df.empty:
        logger.warning("SCD1 validation: DataFrame is empty")
        return True  # Empty DataFrame is valid (no data to process)

    if df["link"].isna().any() or (df["link"].astype(str).str.strip() == "").any():
        logger.error("SCD1 validation failed: Found empty or null values in 'link' column")
        return False

    # Check for duplicate links
    duplicate_links = df[df["link"].duplicated()]
    if not duplicate_links.empty:
        logger.warning(f"SCD1 validation: Found {len(duplicate_links)} duplicate links. Will keep last occurrence.")
        logger.debug(f"Duplicate links: {duplicate_links['link'].tolist()}")

    logger.info("SCD1 data validation passed.")
    return True


def deduplicate_by_link(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records based on the link column, keeping the last occurrence.

    Args:
        df: DataFrame to deduplicate

    Returns:
        DataFrame with duplicates removed
    """
    initial_count = len(df)

    # Keep the last occurrence of each link (most recent data)
    df_dedup = df.drop_duplicates(subset=["link"], keep="last")

    removed_count = initial_count - len(df_dedup)
    if removed_count > 0:
        logger.info(f"SCD1 deduplication: Removed {removed_count} duplicate records, kept {len(df_dedup)} unique records.")

    return df_dedup
