"""HTML processing utilities."""

import re
import html2text
from bs4 import BeautifulSoup


def create_html_cleaner() -> html2text.HTML2Text:
    """Create and configure an HTML to text converter.
    
    Returns:
        Configured HTML2Text instance
    """
    cleaner = html2text.HTML2Text()
    cleaner.ignore_links = False
    cleaner.ignore_images = True
    cleaner.ignore_emphasis = True
    cleaner.body_width = 0
    return cleaner


def clean_html_to_text(html_content: str, cleaner: html2text.HTML2Text = None) -> str:
    """Convert HTML content to clean text.
    
    Args:
        html_content: HTML content to clean
        cleaner: Optional HTML2Text instance (will create one if not provided)
        
    Returns:
        Cleaned text as a single line
    """
    if cleaner is None:
        cleaner = create_html_cleaner()
        
    raw_text = cleaner.handle(html_content).strip()
    # Replace multiple whitespace characters with a single space
    return re.sub(r"\s+", " ", raw_text)
