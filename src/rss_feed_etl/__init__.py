"""
RSS Feed ETL - Extract, Transform, and Load RSS feeds to Google Sheets.

This package provides functionality to:
1. Extract data from RSS feeds
2. Transform the data into a structured format
3. Load the data into Google Sheets using either:
   - Simple merge_upsert approach (default)
   - SCD2 (Slowly Changing Dimension Type 2) pattern for tracking historical changes
"""

__version__ = "1.0.0"

from .core.etl import RSSFeedETL
from .core.data_loader import DataLoader
from .models.feeder import Feeder

__all__ = ["RSSFeedETL", "DataLoader", "Feeder"]
