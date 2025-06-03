"""Tests for the DataLoader class."""

import unittest
import pandas as pd
from datetime import datetime, timezone
from src.rss_feed_etl.core.data_loader import DataLoader


class TestDataLoader(unittest.TestCase):
    """Test cases for the DataLoader class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.upsert_loader = DataLoader(use_scd2=False)
        self.scd2_loader = DataLoader(use_scd2=True)
        
        # Create test data
        self.new_data = pd.DataFrame({
            "link": ["link1", "link2", "link3"],
            "entry_title": ["Job 1", "Job 2", "Job 3"],
            "published": ["2023-01-01", "2023-01-02", "2023-01-03"],
            "feed_title": ["Feed 1", "Feed 2", "Feed 3"],
            "reader": ["Reader 1", "Reader 2", "Reader 3"],
            "time_window": ["daily", "weekly", "monthly"],
            "summary": ["Summary 1", "Summary 2", "Summary 3"],
            "job_title": ["Developer", "Designer", "Manager"],
            "notes": ["", "", ""]
        })
        
        self.old_data = pd.DataFrame({
            "link": ["link1", "link2", "link4"],
            "entry_title": ["Job 1", "Old Job 2", "Job 4"],
            "published": ["2023-01-01", "2023-01-02", "2023-01-04"],
            "feed_title": ["Feed 1", "Feed 2", "Feed 4"],
            "reader": ["Reader 1", "Reader 2", "Reader 4"],
            "time_window": ["daily", "weekly", "monthly"],
            "summary": ["Summary 1", "Old Summary 2", "Summary 4"],
            "job_title": ["Developer", "Old Designer", "Analyst"],
            "notes": ["Note 1", "Note 2", "Note 4"]
        })
        
        # Add SCD2 columns to old data for SCD2 tests
        self.old_data_scd2 = self.old_data.copy()
        now = datetime.now(timezone.utc)
        self.old_data_scd2["effective_start"] = now
        self.old_data_scd2["effective_end"] = pd.NaT
        self.old_data_scd2["current_flag"] = 1
    
    def test_merge_upsert(self):
        """Test the merge_upsert method."""
        final_df, inserted, updated = self.upsert_loader._merge_upsert(self.new_data, self.old_data)
        
        # Check counts
        self.assertEqual(inserted, 1)  # link3 is new
        self.assertEqual(updated, 1)   # link2 is updated
        
        # Check total records
        self.assertEqual(len(final_df), 4)  # link1, link2, link3, link4
        
        # Check that notes are preserved
        link2_row = final_df[final_df["link"] == "link2"]
        self.assertEqual(link2_row["notes"].values[0], "Note 2")
        
        # Check that changes are applied
        self.assertEqual(link2_row["entry_title"].values[0], "Job 2")
        self.assertEqual(link2_row["summary"].values[0], "Summary 2")
    
    def test_merge_scd2(self):
        """Test the merge_scd2 method."""
        final_df, inserted, updated, removed = self.scd2_loader._merge_scd2(self.new_data, self.old_data_scd2)
        
        # Check counts
        self.assertEqual(inserted, 1)  # link3 is new
        self.assertEqual(updated, 1)   # link2 is updated
        self.assertEqual(removed, 1)   # link4 is removed
        
        # Check total records
        self.assertEqual(len(final_df), 5)  # link1, link2 (old), link2 (new), link3, link4
        
        # Check that we have 2 versions of link2
        link2_rows = final_df[final_df["link"] == "link2"]
        self.assertEqual(len(link2_rows), 2)
        
        # Check that one version is current and one is historical
        self.assertEqual(sum(link2_rows["current_flag"]), 1)
        
        # Check that the current version has the new data
        current_link2 = link2_rows[link2_rows["current_flag"] == 1]
        self.assertEqual(current_link2["entry_title"].values[0], "Job 2")
        self.assertEqual(current_link2["summary"].values[0], "Summary 2")
        
        # Check that notes are preserved
        self.assertEqual(current_link2["notes"].values[0], "Note 2")
        
        # Check that removed records are marked as not current
        link4_row = final_df[final_df["link"] == "link4"]
        self.assertEqual(link4_row["current_flag"].values[0], 0)
        self.assertTrue(pd.notna(link4_row["effective_end"].values[0]))


if __name__ == "__main__":
    unittest.main()
