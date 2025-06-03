import html2text
import re

def clean_html_summary(html_content: str) -> str:
    """Cleans HTML content, typically from an RSS summary."""
    if not html_content:
        return ""

    html_cleaner = html2text.HTML2Text()
    html_cleaner.ignore_links = False
    html_cleaner.ignore_images = True
    html_cleaner.ignore_emphasis = True
    html_cleaner.body_width = 0 # Prevent wrapping

    raw_text = html_cleaner.handle(html_content).strip()
    # Collapse multiple spaces/newlines into single spaces
    single_line_text = re.sub(r"\s+", " ", raw_text)
    return single_line_text