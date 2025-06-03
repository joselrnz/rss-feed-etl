"""Feeder model for RSS feed configuration."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Feeder:
    """Configuration for an RSS feed source.
    
    Attributes:
        title: The title of the feed
        reader: The person or entity reading the feed
        time_window: The time window for the feed (e.g., "daily", "weekly")
        url: The URL of the RSS feed
        worksheet_name: The name of the worksheet to store the feed data
        job_title: The job title associated with this feed
    """
    title: str
    reader: str
    time_window: str
    url: str
    worksheet_name: str
    job_title: str
