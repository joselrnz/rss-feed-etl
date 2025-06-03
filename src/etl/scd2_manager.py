import pandas as pd
from datetime import datetime, timezone
from typing import List

import logging
from src.utils.logging_utils import logger # Assuming you want to use the shared logger

def merge_scd2(new_df: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    """
    Performs a Slowly Changing Dimension Type 2 merge between new data and historical data.

    Compares new records against the current versions in history based on a 'link' key.
    - Records in new_df that match a current record in hist and have changes
      in key data fields will cause the historical record to be expired (current_flag=0, effective_end=now)
      and a new record inserted (current_flag=1, effective_start=now).
    - Records in new_df that do not match any current record in hist are inserted
      as new current records.
    - Records in hist that were current but are not present in new_df are expired
      (current_flag=0, effective_end=now).
    - Records in hist that were already expired remain unchanged.

    Assumes hist DataFrame contains SCD2 columns:
    'effective_start', 'effective_end', 'current_flag'.

    Args:
        new_df: DataFrame containing the latest data (e.g., from RSS feed).
                Expected columns: 'link', 'entry_title', 'published_raw', 'feed_title',
                'reader', 'time_window', 'summary'. (Using published_raw as that's what's in the sheet)
        hist: DataFrame containing the historical data from the target sheet,
              including SCD2 columns.

    Returns:
        A DataFrame representing the merged history (including both current and expired records).
        Includes columns: 'link', 'entry_title', 'published_raw', 'feed_title',
        'reader', 'time_window', 'summary', 'effective_start', 'effective_end',
        'current_flag', 'notes'.
    """
    now = datetime.now(timezone.utc)
    key = "link"

    # Define all columns expected in the final SCD2 DataFrame
    scd2_cols = [
        "link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary",
        "effective_start", "effective_end", "current_flag", "notes"
    ]

    # Ensure new_df has the standard content columns, adding empty if missing
    content_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary"]
    for col in content_cols:
        if col not in new_df.columns:
            new_df[col] = ""
        # Convert content columns to string for reliable comparison/merging
        new_df[col] = new_df[col].astype(str)


    # Ensure hist DataFrame has all SCD2 columns, adding empty/default values if missing
    for col in scd2_cols:
        if col not in hist.columns:
             # Use appropriate dtype for new columns
             if col in ["effective_start", "effective_end"]:
                  hist[col] = pd.NaT # Pandas Not-a-Time for datetime columns
                  hist[col] = hist[col].astype('datetime64[ns, UTC]') # Explicitly set datetime dtype with UTC
             elif col == "current_flag":
                  hist[col] = 0 # Default flag is 0 (not current)
                  hist[col] = hist[col].astype(int) # Ensure integer type
             else:
                  hist[col] = "" # Default to empty string for text columns
                  hist[col] = hist[col].astype(str) # Ensure string type


    # Filter history to get only the currently active records
    # Use .eq(1) or .astype(str).str.lower().eq('1') depending on how flags are stored
    # Assuming current_flag is stored as integer 1 or 0 in the sheet
    hist['current_flag'] = pd.to_numeric(hist['current_flag'], errors='coerce').fillna(0).astype(int)
    current_hist = hist[hist.current_flag == 1].copy()


    # Merge new data with current historical data based on the key ('link')
    merged = new_df[content_cols].merge(current_hist[scd2_cols], on=key, how="left", indicator=True, suffixes=("", "_hist"))

    # Identify rows that are in BOTH new and history, but have changes in content fields
    # Compare non-key, non-SCD2 columns
    comparison_cols = ["entry_title", "published_raw", "feed_title", "reader", "time_window", "summary"]
    changed_mask = pd.DataFrame(False, index=merged.index, columns=['changed'])

    # Check for changes only among rows that exist in both (indicator == 'both')
    # Use .fillna('') for robust comparison against empty strings
    both_mask = (merged["_merge"] == "both")
    if both_mask.any(): # Only check for changes if there are rows in both
        for col in comparison_cols:
            hist_col = f"{col}_hist"
            if col in merged.columns and hist_col in merged.columns:
                 # Compare values where the row is in both dataframes
                 changed_mask['changed'] |= (both_mask & (merged[col].fillna('') != merged[hist_col].fillna('')))
            # Note: If a historical column is missing from the merged df (shouldn't happen if hist is prepped),
            # it won't trigger a change here but the row will be treated as new if it's left_only.


    # Get the 'link' values for records that have changed
    changed_links = merged[changed_mask['changed']][key].tolist()

    logger.info(f"SCD2 Merge: Found {len(changed_links)} links with changes.")


    # 1. Invalidate old 'current' records that have changed or are now missing from new_df
    # Identify links that were current in history but are NOT in the new_df links
    removed_links = set(current_hist[key]) - set(new_df[key])
    logger.info(f"SCD2 Merge: Found {len(removed_links)} links removed from new data.")

    # Links to expire are those with changes OR those that were removed
    links_to_expire = set(changed_links).union(removed_links)

    if links_to_expire:
        logger.info(f"SCD2 Merge: Expiring {len(links_to_expire)} historical records.")
        # Find the specific rows in the *original* history DataFrame to update
        hist_rows_to_expire_mask = hist[key].isin(links_to_expire) & (hist.current_flag == 1)
        if hist_rows_to_expire_mask.any():
             # Update effective_end and current_flag
             hist.loc[hist_rows_to_expire_mask, "effective_end"] = now
             hist.loc[hist_rows_to_expire_mask, "current_flag"] = 0


    # 2. Prepare records to insert
    # These are the records that are new ('left_only' in the merge) or have changed ('both' in merge and part of changed_links)
    inserts_mask = (merged["_merge"] == "left_only") | (merged[key].isin(changed_links))
    inserts_df = merged[inserts_mask][content_cols].copy() # Select content columns from the new data


    # Add SCD2 columns to the inserts DataFrame
    inserts_df["effective_start"] = now
    inserts_df["effective_end"] = pd.NaT # Not-a-Time for current records
    inserts_df["effective_end"] = inserts_df["effective_end"].astype('datetime64[ns, UTC]') # Ensure dtype
    inserts_df["current_flag"] = 1
    # Preserve notes for *updated* records where possible?
    # In pure SCD2, new version starts with default state (empty notes).
    # If you wanted to carry notes forward, the logic would be more complex,
    # potentially merging notes from the *expired* version.
    # Sticking to pure SCD2: new records start with empty notes.
    inserts_df["notes"] = ""


    logger.info(f"SCD2 Merge: Inserting {len(inserts_df)} new current records.")


    # 3. Combine the (potentially updated) historical data with the new inserts
    # The original 'hist' DataFrame now contains the expired records.
    # Records from 'hist' that were already expired OR were not affected by the merge (i.e., current and unchanged)
    # are implicitly kept. We just need to add the new 'inserts_df'.

    # Exclude records from the original 'hist' that were just expired and replaced by an insert
    # (those in changed_links, but keep if they had previous non-current versions)
    # The approach of modifying 'hist' in place is simpler here.
    # The resulting 'hist' df after line ~60 already has the expirations applied.
    # We just need to concatenate the new inserts.

    result_df = pd.concat([hist, inserts_df], ignore_index=True)

    # Ensure final result has all expected SCD2 columns and correct types/order
    for col in scd2_cols:
         if col not in result_df.columns:
             if col in ["effective_start", "effective_end"]:
                  result_df[col] = pd.NaT
                  result_df[col] = result_df[col].astype('datetime64[ns, UTC]')
             elif col == "current_flag":
                  result_df[col] = 0
                  result_df[col] = result_df[col].astype(int)
             else:
                  result_df[col] = ""
                  result_df[col] = result_df[col].astype(str)


    result_df = result_df[scd2_cols].copy() # Select and reorder


    # Convert datetime columns to a sheet-friendly string format before returning
    # gspread's update handles datetime objects sometimes, but string is safer.
    # ISO format is good for machine readability.
    date_cols = ["effective_start", "effective_end"]
    for col in date_cols:
         if col in result_df.columns:
              # Convert NaT to empty string for the sheet
              result_df[col] = result_df[col].dt.strftime('%Y-%m-%d %H:%M:%S%z').replace('NaT', '') # Format with UTC offset


    # Ensure all columns are string type for writing to Google Sheets
    for col in result_df.columns:
         result_df[col] = result_df[col].astype(str)


    # Sort the final result for consistency: by link, then effective start date
    result_df['effective_start_dt'] = pd.to_datetime(result_df['effective_start'], errors='coerce', utc=True)
    result_df = result_df.sort_values([key, 'effective_start_dt'], ascending=[True, True]).drop(columns=['effective_start_dt'])


    logger.info(f"SCD2 Merge complete. Total records in history: {len(result_df)}.")
    return result_df