"""Data loading module with support for SCD1, SCD2, and merge_upsert patterns."""

import pandas as pd
import gspread
import logging
from datetime import datetime, timezone
from typing import Tuple, List, Optional, Dict, Any, Union

# Import SCD1 manager
try:
    from src.etl.scd1_manager import merge_scd1, validate_scd1_data, deduplicate_by_link
except ImportError:
    # Fallback for different import paths
    try:
        from etl.scd1_manager import merge_scd1, validate_scd1_data, deduplicate_by_link
    except ImportError:
        logging.warning("Could not import SCD1 manager. SCD1 functionality will be disabled.")


class DataLoader:
    """
    Data loader with support for SCD1, SCD2, and merge_upsert patterns.

    This class provides methods to load data to Google Sheets using:
    - SCD1: Simple overwrite (most efficient, no history)
    - SCD2: Full history tracking with effective dates
    - merge_upsert: Legacy approach (similar to SCD1)
    """

    def __init__(
        self,
        primary_key: str = "link",
        columns: Optional[List[str]] = None,
        use_scd2: bool = False,
        loading_strategy: str = "scd1"
    ):
        """
        Initialize the data loader.

        Args:
            primary_key: Column to use as the primary key for merging
            columns: List of columns to include in the output
            use_scd2: Whether to use SCD2 pattern (legacy, overridden by loading_strategy)
            loading_strategy: Data loading strategy ("scd1", "scd2", or "merge_upsert")
        """
        self.primary_key = primary_key
        self.columns = columns or [
            "job_title",
            "link",
            "entry_title",
            "published",
            "feed_title",
            "reader",
            "time_window",
            "summary",
            "notes"
        ]

        # Determine loading strategy
        if loading_strategy.lower() == "scd2" or use_scd2:
            self.loading_strategy = "scd2"
            self.use_scd2 = True
        elif loading_strategy.lower() == "scd1":
            self.loading_strategy = "scd1"
            self.use_scd2 = False
        else:
            self.loading_strategy = "merge_upsert"
            self.use_scd2 = False

        # Add SCD2 columns if using SCD2 pattern
        if self.use_scd2 and "effective_start" not in self.columns:
            self.columns.extend(["effective_start", "effective_end", "current_flag"])

    def _read_worksheet(
        self,
        client: gspread.Client,
        spreadsheet_id: str,
        worksheet_name: str
    ) -> Tuple[pd.DataFrame, gspread.Worksheet]:
        """
        Read data from a Google Sheet worksheet.

        Args:
            client: Authenticated gspread client
            spreadsheet_id: ID of the Google Sheet
            worksheet_name: Name of the worksheet

        Returns:
            Tuple of (DataFrame, Worksheet)
        """
        sheet = client.open_by_key(spreadsheet_id)

        try:
            ws = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logging.info(f"📝 Worksheet '{worksheet_name}' not found. Creating it.")
            ws = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

            # Add headers if it's a new worksheet
            ws.update([self.columns])
            return pd.DataFrame(columns=self.columns), ws

        # Read data
        data = ws.get_all_records(empty2zero=False)
        df = pd.DataFrame(data)

        # Convert date columns if using SCD2
        if self.use_scd2:
            for date_col in ["effective_start", "effective_end"]:
                if date_col in df.columns:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        return df, ws

    def _merge_upsert(
        self,
        new_df: pd.DataFrame,
        old_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        Merge new data with old data using a simple upsert approach.

        This method:
        - Preserves the notes column
        - Updates records that have changed
        - Keeps track of inserted and updated counts
        - PRESERVES ALL HISTORICAL DATA

        Args:
            new_df: DataFrame with new data
            old_df: DataFrame with existing data

        Returns:
            Tuple of (merged DataFrame, inserted count, updated count)
        """
        # Ensure all columns exist in both dataframes
        for col in self.columns:
            if col not in new_df.columns:
                new_df[col] = ""
            if col not in old_df.columns:
                old_df[col] = ""

        # Filter to required columns
        new_df = new_df[self.columns]
        old_df = old_df[self.columns].drop_duplicates(subset=[self.primary_key])

        # Merge dataframes - use outer merge to keep ALL records from both dataframes
        merged = new_df.merge(
            old_df,
            on=self.primary_key,
            how="outer",  # Changed from "left" to "outer" to keep all records
            indicator=True,
            suffixes=("", "_old")
        )

        # Identify new, existing, and old-only records
        is_new = merged["_merge"] == "left_only"  # New records
        is_both = merged["_merge"] == "both"      # Records in both new and old
        is_old_only = merged["_merge"] == "right_only"  # Records only in old data

        # Columns to compare for changes (exclude notes and primary key)
        compare_cols = [col for col in self.columns
                       if col != "notes" and col != self.primary_key]

        # Process records that exist in both datasets
        both_records = merged[is_both].copy()

        # Check which records have changed
        is_changed = both_records[compare_cols].ne(
            both_records[[f"{col}_old" for col in compare_cols]]
        ).any(axis=1)

        # For changed records, update with new data but preserve notes
        if "notes" in merged.columns and "notes_old" in merged.columns:
            # Preserve notes from old records for records that exist in both datasets
            both_records.loc[is_changed, "notes"] = both_records.loc[is_changed, "notes_old"].combine_first(both_records.loc[is_changed, "notes"])

        # Process new records (left_only)
        new_records = merged[is_new].copy()

        # Process old-only records (right_only)
        old_only_records = merged[is_old_only].copy()

        # For old-only records, we need to use the "_old" columns
        for col in self.columns:
            if col != self.primary_key and f"{col}_old" in old_only_records.columns:
                old_only_records[col] = old_only_records[f"{col}_old"]

        # Combine all records
        result_parts = [
            both_records[self.columns],  # Records in both datasets (with updates applied)
            new_records[self.columns],   # New records
            old_only_records[self.columns]  # Records only in old data
        ]

        final_df = pd.concat(result_parts, ignore_index=True)

        # Sort by published date if it exists
        if "published" in final_df.columns:
            final_df["published"] = pd.to_datetime(final_df["published"], errors="coerce")
            final_df = final_df.sort_values("published", ascending=False)  # Changed to descending order

        # Calculate counts
        inserted = is_new.sum()
        updated = is_changed.sum()

        return final_df, inserted, updated

    def _merge_scd2(
        self,
        new_df: pd.DataFrame,
        hist_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int, int, int]:
        """
        Merge new data with historical data using SCD2 pattern.

        This method:
        - Tracks historical changes with effective dates
        - Maintains a current_flag to identify the latest version
        - Preserves notes when updating records

        Args:
            new_df: DataFrame with new data
            hist_df: DataFrame with historical data

        Returns:
            Tuple of (merged DataFrame, inserted count, updated count, removed count)
        """
        now = datetime.now(timezone.utc)

        # Ensure all columns exist in both dataframes
        scd2_columns = self.columns + ["effective_start", "effective_end", "current_flag"]
        for col in scd2_columns:
            if col not in new_df.columns and col not in ["effective_start", "effective_end", "current_flag"]:
                new_df[col] = ""
            if col not in hist_df.columns:
                if col == "effective_start":
                    hist_df[col] = pd.NaT
                elif col == "effective_end":
                    hist_df[col] = pd.NaT
                elif col == "current_flag":
                    hist_df[col] = 1
                else:
                    hist_df[col] = ""

        # Get only current records from historical data
        current = hist_df[hist_df.current_flag == 1]

        # Merge new data with current historical data
        merged = new_df.merge(
            current,
            on=self.primary_key,
            how="left",
            indicator=True,
            suffixes=("", "_hist")
        )

        # Columns to compare for changes (exclude notes and primary key)
        compare_cols = [col for col in self.columns
                       if col != "notes" and col != self.primary_key]

        # Find records that have changed
        changed = merged[
            (merged["_merge"] == "both") &
            merged[compare_cols].ne(
                merged[[f"{col}_hist" for col in compare_cols]]
            ).any(axis=1)
        ][self.primary_key].tolist()

        # Update historical records: set effective_end and current_flag for changed records
        hist_df.loc[
            hist_df[self.primary_key].isin(changed) & (hist_df.current_flag == 1),
            ["effective_end", "current_flag"]
        ] = [now, 0]

        # Create new records for inserts and changes
        inserts = merged[(merged["_merge"] == "left_only") |
                         (merged[self.primary_key].isin(changed))].copy()

        # Keep only the columns from new_df
        insert_columns = [col for col in new_df.columns if col in inserts.columns]
        inserts = inserts[insert_columns].copy()

        # Add SCD2 columns
        inserts["effective_start"] = now
        inserts["effective_end"] = pd.NaT
        inserts["current_flag"] = 1

        # Preserve notes if they exist
        if "notes" in merged.columns and "notes_hist" in merged.columns:
            notes_idx = merged[merged[self.primary_key].isin(changed)].index
            inserts.loc[inserts.index.isin(notes_idx), "notes"] = merged.loc[notes_idx, "notes_hist"].values

        # Identify records that have been removed
        removed = set(current[self.primary_key]) - set(new_df[self.primary_key])
        removed_count = len(removed)

        if removed:
            hist_df.loc[
                hist_df[self.primary_key].isin(removed) & (hist_df.current_flag == 1),
                ["effective_end", "current_flag"]
            ] = [now, 0]

        # Combine historical and new records
        result = pd.concat([hist_df, inserts], ignore_index=True)

        # Sort by primary key and effective start date
        result = result.sort_values([self.primary_key, "effective_start"])

        # Calculate counts
        inserted = len(inserts[~inserts[self.primary_key].isin(changed)])
        updated = len(changed)

        return result, inserted, updated, removed_count

    def _merge_scd1(
        self,
        new_df: pd.DataFrame,
        hist_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, int, int]:
        """
        Merge new data with historical data using SCD1 pattern.

        This method uses the SCD1 manager to perform simple overwrite merging.

        Args:
            new_df: DataFrame with new data
            hist_df: DataFrame with historical data

        Returns:
            Tuple of (merged DataFrame, inserted count, updated count)
        """
        try:
            # Validate new data
            if not validate_scd1_data(new_df):
                logging.error("SCD1 validation failed for new data")
                return hist_df, 0, 0

            # Deduplicate new data
            new_df_clean = deduplicate_by_link(new_df)

            # Perform SCD1 merge
            result_df, inserted, updated = merge_scd1(new_df_clean, hist_df)

            return result_df, inserted, updated

        except Exception as e:
            logging.error(f"Error in SCD1 merge: {e}")
            logging.debug("Exception details:", exc_info=True)
            # Return original data on error
            return hist_df, 0, 0

    def load_to_sheet(
        self,
        client: gspread.Client,
        new_df: pd.DataFrame,
        spreadsheet_id: str,
        worksheet_name: str
    ) -> Dict[str, Any]:
        """
        Load data to a Google Sheet worksheet.

        Args:
            client: Authenticated gspread client
            new_df: DataFrame with new data
            spreadsheet_id: ID of the Google Sheet
            worksheet_name: Name of the worksheet

        Returns:
            Dictionary with results (inserted, updated, removed counts)
        """
        # Read existing data
        hist_df, ws = self._read_worksheet(client, spreadsheet_id, worksheet_name)

        results = {
            "inserted": 0,
            "updated": 0,
            "removed": 0
        }

        # Apply appropriate merge strategy
        if self.loading_strategy == "scd2":
            if not hist_df.empty:
                final_df, inserted, updated, removed = self._merge_scd2(new_df, hist_df)
                results["inserted"] = inserted
                results["updated"] = updated
                results["removed"] = removed
            else:
                # For new worksheets with SCD2, add SCD2 columns
                now = datetime.now(timezone.utc)
                new_df["effective_start"] = now
                new_df["effective_end"] = pd.NaT
                new_df["current_flag"] = 1
                final_df = new_df
                results["inserted"] = len(new_df)
        elif self.loading_strategy == "scd1":
            # Use SCD1
            if not hist_df.empty:
                final_df, inserted, updated = self._merge_scd1(new_df, hist_df)
                results["inserted"] = inserted
                results["updated"] = updated
            else:
                final_df = new_df
                results["inserted"] = len(new_df)
        else:
            # Use merge_upsert (legacy)
            if not hist_df.empty:
                final_df, inserted, updated = self._merge_upsert(new_df, hist_df)
                results["inserted"] = inserted
                results["updated"] = updated
            else:
                final_df = new_df
                results["inserted"] = len(new_df)

        # Format dates for Google Sheets
        if not final_df.empty:
            date_columns = ["published"]
            if self.use_scd2:
                date_columns.extend(["effective_start", "effective_end"])

            for date_col in date_columns:
                if date_col in final_df.columns:
                    final_df[date_col] = pd.to_datetime(final_df[date_col], errors="coerce")
                    final_df[date_col] = final_df[date_col].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

            # Update the worksheet
            ws.clear()
            ws.update([final_df.columns.tolist()] + final_df.astype(str).values.tolist())

        return results
