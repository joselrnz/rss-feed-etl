from dataclasses import dataclass

@dataclass
class Feeder:
    """Represents configuration for a single RSS feed."""
    title: str
    reader: str
    time_window: str
    url: str
    worksheet_name: str