import feedparser
import pandas as pd
import gspread
from typing import List
import logging

# Import components from your src directory
from src.models.feeder import Feeder
from src.utils.auth import get_google_sheet_client
from src.utils.html_utils import clean_html_summary
from src.utils.logging_utils import logger

# --- Read feed config from spreadsheet tab ---
def read_feeders_from_config_worksheet(spreadsheet_id: str, worksheet_name: str = "Feeds") -> List[Feeder]:
    """
    Reads feeder configurations from a specified Google Sheet worksheet.

    Args:
        spreadsheet_id: The ID of the Google Spreadsheet.
        worksheet_name: The name of the worksheet containing feeder configs.

    Returns:
        A list of Feeder objects.

    Raises:
        ValueError: If required columns are missing in the config sheet.
        gspread.exceptions.SpreadsheetNotFound: If the spreadsheet is not found.
        gspread.exceptions.WorksheetNotFound: If the worksheet is not found.
    """
    client = get_google_sheet_client()

    try:
        sh = client.open_by_key(spreadsheet_id)
        ws = sh.worksheet(worksheet_name)
        df = pd.DataFrame(ws.get_all_records())
    except (gspread.exceptions.SpreadsheetNotFound, gspread.exceptions.WorksheetNotFound) as e:
         logger.error(f"Error accessing spreadsheet or worksheet: {e}")
         raise
    except Exception as e:
        logger.error(f"Error reading data from config sheet: {e}")
        raise


    logger.info(f"Columns in config sheet: {list(df.columns)}")
    df.columns = [col.strip().lower() for col in df.columns]

    required = {"title", "reader", "time", "url", "worksheet_name"}
    missing = required - set(df.columns)
    if missing:
        logger.error(f"Missing required columns in config sheet: {missing}")
        raise ValueError(f"❌ Missing required columns in config sheet: {missing}")

    feeders = []
    for _, r in df.iterrows():
        # Basic check for essential fields
        if pd.isna(r.get("url")) or not r["url"].strip():
             logger.warning(f"Skipping feeder with missing URL: {r.get('title', 'N/A')}")
             continue
        if pd.isna(r.get("worksheet_name")) or not r["worksheet_name"].strip():
            logger.warning(f"Skipping feeder '{r.get('title', 'N/A')}' with missing worksheet name.")
            continue


        feeders.append(
            Feeder(
                str(r.get("title", "")).strip(), # Ensure string
                str(r.get("reader", "")).strip(), # Ensure string
                str(r.get("time", "")).strip(),   # Ensure string
                str(r.get("url", "")).strip(),     # Ensure string
                str(r.get("worksheet_name", "")).strip() # Ensure string
            )
        )
    # Filter out any potentially empty feeders if needed, though the checks above handle this
    feeders = [f for f in feeders if f.url and f.worksheet_name]
    return feeders

# --- Parse RSS Feed ---
def parse_feed(f: Feeder) -> pd.DataFrame:
    """
    Parses a single RSS feed and returns a DataFrame of entries.

    Args:
        f: The Feeder configuration object.

    Returns:
        A pandas DataFrame containing the feed entries.
    """
    logger.info(f"Parsing feed from URL: {f.url}")
    try:
        feed = feedparser.parse(f.url)
    except Exception as e:
        logger.error(f"Error parsing feed URL {f.url}: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

    if feed.bozo:
         logger.warning(f"Feed '{f.title}' has parsing errors: {feed.bozo_exception}")


    records = []
    for e in feed.entries:
        summary = clean_html_summary(e.get("summary", "")).strip() # Clean after getting
        records.append({
            "link": str(e.get("link", "")).strip(), # Ensure string
            "entry_title": str(e.get("title", "")).strip(), # Ensure string
            # Using 'published_parsed' which is a consistent time structure
            # If not available, fallback to 'published' and try to parse later if needed
            "published_parsed": feedparser._parse_date(e.get("published", None)),
            "published_raw": str(e.get("published", "")).strip(), # Keep original raw string
            "feed_title": f.title,
            "reader": f.reader,
            "time_window": f.time_window,
            "summary": summary,
            "notes": "" # Initialize notes column
        })

    df = pd.DataFrame(records)

    if df.empty:
        return df

    # Convert published_parsed to datetime, handle errors
    df['published_datetime'] = pd.to_datetime(df['published_parsed'], errors='coerce')

    # Sort by date if possible
    if 'published_datetime' in df.columns and not df['published_datetime'].isnull().all():
        # Sort by datetime and then link (for stable sorting of identical dates)
        df = df.sort_values(['published_datetime', 'link'], ascending=[False, True]).drop(columns=['published_parsed'])
    else:
         logger.warning("Could not parse valid dates for sorting. Sorting by raw published string.")
         # If parsing failed or all dates are NaT, sort by the raw string if it exists
         if 'published_raw' in df.columns:
             df = df.sort_values('published_raw', ascending=False)


    # Reorder columns to match desired sheet order, including notes
    # Ensure all expected columns are present, adding if missing
    expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    # Select and reorder columns
    df = df[expected_cols]

    # Convert all columns to string to avoid mixed type issues when writing to sheets
    for col in df.columns:
         df[col] = df[col].astype(str)

    return df


# --- Read existing Google Sheet data ---
def read_history(ws) -> pd.DataFrame:
    """Reads all records from a Google Sheet worksheet into a DataFrame."""
    try:
        # Use get_all_values and convert to DataFrame to handle mixed types better
        # This also preserves potential empty cells as empty strings
        data = ws.get_all_values()
        if not data:
            logger.info("Worksheet is empty.")
            # Return a DataFrame with expected columns even if empty
            expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
            return pd.DataFrame(columns=expected_cols)


        headers = data[0]
        records = data[1:]
        df = pd.DataFrame(records, columns=headers)

        # Ensure all expected columns are present from the start for consistent merging
        expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""
                logger.warning(f"Added missing column '{col}' to history DataFrame.")

        # Select and reorder columns
        df = df[expected_cols].copy()

        # Convert all columns to string just in case
        for col in df.columns:
             df[col] = df[col].astype(str)

        logger.info(f"Read {len(df)} existing records from sheet.")
        return df
    except Exception as e:
        logger.error(f"Error reading history from worksheet: {e}")
        # Return an empty DataFrame with expected columns on error
        expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
        return pd.DataFrame(columns=expected_cols)


# --- Upsert Merge with notes preserved ---
def merge_upsert(new_df: pd.DataFrame, hist: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """
    Merges new data with historical data, preserving the 'notes' column
    from history for matching entries and identifying new/updated entries.

    Args:
        new_df: DataFrame containing new entries from the RSS feed.
        hist: DataFrame containing existing entries from the Google Sheet.

    Returns:
        A tuple containing:
            - The merged DataFrame (historical + new/updated).
            - The count of inserted rows.
            - The count of updated rows.
    """
    # Ensure consistent column types (string) for merging
    for col in new_df.columns:
        new_df[col] = new_df[col].astype(str)
    for col in hist.columns:
         hist[col] = hist[col].astype(str)


    if hist.empty:
        logger.info("History is empty. All new records will be inserted.")
        # Ensure new_df has all expected columns before returning
        expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
        for col in expected_cols:
            if col not in new_df.columns:
                new_df[col] = ""
        return new_df[expected_cols].copy(), len(new_df), 0 # Return a copy

    key = "link"
    # Ensure both dataframes have all necessary columns with empty strings if missing
    all_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]

    for col in all_cols:
        if col not in new_df.columns:
            new_df[col] = ""
        if col not in hist.columns:
            hist[col] = ""
            logger.warning(f"Added missing column '{col}' to history DataFrame before merge.")


    # Keep only the relevant columns and drop duplicates in history based on the key
    new_df_subset = new_df[all_cols].copy()
    hist_subset = hist[all_cols].drop_duplicates(subset=[key]).copy()

    # Perform a left merge to bring in history data
    # Using how="left" ensures all new_df rows are kept
    merged = new_df_subset.merge(hist_subset, on=key, how="left", indicator=True, suffixes=("", "_old"))

    # Identify rows that are new ('left_only')
    new_rows_mask = (merged["_merge"] == "left_only")

    # Identify rows that exist in both but have changes in non-notes fields
    comparison_cols = ["entry_title", "published_raw", "feed_title", "reader", "time_window", "summary"]
    changed_mask = pd.DataFrame(False, index=merged.index, columns=['changed'])

    # Check for changes only if the old column exists and the row isn't new
    for col in comparison_cols:
        old_col = f"{col}_old"
        if old_col in merged.columns:
             # Check for differences where the row is NOT new ('left_only')
             # Use .fillna('') to treat missing values consistently as empty strings for comparison
             changed_mask['changed'] |= (~new_rows_mask & (merged[col].fillna('') != merged[old_col].fillna('')))
        # Note: If an _old column doesn't exist, changes can't be detected for that column,
        # but new rows are handled by new_rows_mask.

    # Rows to include in the final merged DataFrame are:
    # 1. New rows (left_only)
    # 2. Historical rows that match a new row and have changes (identified by changed_mask)
    # 3. Historical rows that do NOT match any new row (these will be concatenated later)

    # Select the rows from the `merged` dataframe that are new or updated
    updated_or_new_in_merged = merged[new_rows_mask | changed_mask['changed']].copy()

    # Preserve notes from the old data for rows that had history
    if 'notes_old' in updated_or_new_in_merged.columns:
        # For new rows ('left_only'), 'notes_old' will be NaN, so combine_first will use the original 'notes' (which is "")
        # For updated rows, 'notes_old' will have the historical note, which we want to keep
        updated_or_new_in_merged['notes'] = updated_or_new_in_merged['notes_old'].combine_first(updated_or_new_in_merged['notes'])


    # Select only the final desired columns for the new/updated rows
    updated_or_new_df = updated_or_new_in_merged[all_cols]

    # Count insertions and updates based on the masks applied to the *merged* DataFrame
    inserted_count = new_rows_mask.sum()
    # Updated count is the number of rows that *changed* among those that were not new
    updated_count = changed_mask['changed'][~new_rows_mask].sum()


    # Get historical rows that were NOT in the new data at all (i.e., are neither new nor updated)
    # These are the historical rows whose 'link' is NOT present in the original new_df_subset
    unchanged_hist_df = hist_subset[~hist_subset[key].isin(new_df_subset[key])].copy()


    # Combine the unchanged historical data with the new/updated data
    combined_df = pd.concat([unchanged_hist_df, updated_or_new_df], ignore_index=True)

    # Attempt to sort by published date if possible
    try:
        # Convert published_raw to datetime for sorting, handling errors
        combined_df['published_datetime'] = pd.to_datetime(combined_df['published_raw'], errors='coerce', utc=True) # Assume UTC if no timezone info
        # Sort by datetime (NaT values will be sorted to the beginning or end depending on pandas version, usually end), then link
        combined_df = combined_df.sort_values(['published_datetime', 'link'], ascending=[False, True]).drop(columns=['published_datetime'])
    except Exception:
        logger.warning("Could not sort merged DataFrame by date; sorting by raw published string and link.")
        # Fallback to sorting by raw string and link
        combined_df = combined_df.sort_values(['published_raw', 'link'], ascending=[False, True])

    # Ensure all columns are string type before returning for sheet writing
    for col in combined_df.columns:
         combined_df[col] = combined_df[col].astype(str)


    logger.info(f"Merge result: Inserted={inserted_count}, Updated={updated_count}, Unchanged={len(unchanged_hist_df)}")

    return combined_df, inserted_count, updated_count

# --- Load to Google Sheet ---
def load_to_sheet(new_df: pd.DataFrame, spreadsheet_id: str, worksheet_name: str):
    """
    Loads a DataFrame into a Google Sheet worksheet, performing an upsert
    merge with existing data to preserve notes.

    Args:
        new_df: DataFrame containing the new data to load.
        spreadsheet_id: The ID of the Google Spreadsheet.
        worksheet_name: The name of the target worksheet.

    Returns:
        A tuple containing the number of inserted and updated rows.
    """
    client = get_google_sheet_client()

    try:
        sh = client.open_by_key(spreadsheet_id)
    except gspread.exceptions.SpreadsheetNotFound as e:
         logger.error(f"Spreadsheet not found with ID {spreadsheet_id}: {e}")
         raise
    except Exception as e:
        logger.error(f"Error opening spreadsheet {spreadsheet_id}: {e}")
        raise


    ws = None
    try:
        ws = sh.worksheet(worksheet_name)
        logger.info(f"Worksheet '{worksheet_name}' found.")
    except gspread.exceptions.WorksheetNotFound:
        logger.info(f"Worksheet '{worksheet_name}' not found — creating it.")
        # You might want to set initial rows/cols or other properties here
        try:
            ws = sh.add_worksheet(title=worksheet_name, rows="1000", cols="10") # Default size
            # Add headers immediately after creation
            initial_headers = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
            ws.update([initial_headers])
            logger.info(f"Worksheet '{worksheet_name}' created successfully with headers.")
        except Exception as e:
            logger.error(f"Error creating worksheet '{worksheet_name}': {e}")
            raise


    if ws is None:
        logger.error("Could not find or create worksheet.")
        return 0, 0

    hist_df = read_history(ws)
    if not hist_df.empty:
        full_df, inserted, updated = merge_upsert(new_df, hist_df)
    else:
        full_df = new_df.copy()
        # Ensure new_df has the 'notes' column even if empty initially
        if 'notes' not in full_df.columns:
            full_df['notes'] = ""
        # Ensure columns are in the expected order and are strings
        expected_cols = ["link", "entry_title", "published_raw", "feed_title", "reader", "time_window", "summary", "notes"]
        for col in expected_cols:
            if col not in full_df.columns:
                full_df[col] = ""
        full_df = full_df[expected_cols].astype(str)

        inserted, updated = len(new_df), 0
        logger.info(f"Loading {len(new_df)} new records into empty sheet.")


    if not full_df.empty:
        try:
            # Prepare data for update (headers + rows)
            # Ensure headers match DataFrame columns exactly
            data_to_update = [full_df.columns.tolist()] + full_df.values.tolist() # .astype(str) already done in merge/read

            # Clear existing data
            # Get the grid range dynamically based on current data size to avoid clearing unrelated cells
            # If headers are present, clear from A1 down to the last row of current data
            current_row_count = len(hist_df) + 1 if not hist_df.empty else 1 # +1 for header
            # If you want to clear the *entire* sheet contents including formatting, use clear().
            # Otherwise, use update with an empty range or calculate a range to clear just data.
            # For simplicity and ensuring old data is gone before writing, clear() is effective.
            # A potentially more optimized approach would be to get the bounds of the existing data
            # before clearing, but clearing is robust.
            ws.clear()
            logger.info(f"Cleared worksheet '{worksheet_name}'.")


            # Update with new data
            # Use update method that takes list of lists
            ws.update(data_to_update)
            logger.info(f"Updated worksheet '{worksheet_name}' with {len(full_df)} rows (including header).")

        except Exception as e:
            logger.error(f"Error updating worksheet '{worksheet_name}': {e}")
            raise
    else:
         logger.warning("No data to write to worksheet.")


    return inserted, updated