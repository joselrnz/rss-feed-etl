"""Authentication utilities for Google Sheets."""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import List


def get_google_sheets_client(creds_file: str, scopes: List[str] = None) -> gspread.Client:
    """Authenticate with Google Sheets API and return a client.
    
    Args:
        creds_file: Path to the service account JSON file
        scopes: OAuth scopes to request (default: sheets and drive)
        
    Returns:
        An authenticated gspread client
    """
    if scopes is None:
        scopes = [
            "https://spreadsheets.google.com/feeds", 
            "https://www.googleapis.com/auth/drive"
        ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scopes)
    return gspread.authorize(creds)
