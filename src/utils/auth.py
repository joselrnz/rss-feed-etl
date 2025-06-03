import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import logging

from dotenv import load_dotenv
load_dotenv() # Load environment variables from .env

logger = logging.getLogger(__name__)

def get_google_sheet_client():
    """Authenticates with Google Sheets and returns a client."""
    creds_file = os.getenv("GOOGLE_CREDS_FILE_PATH", "secrets/service_account.json")
    if not os.path.exists(creds_file):
        logger.error(f"Google Sheets credential file not found: {creds_file}")
        raise FileNotFoundError(f"Google Sheets credential file not found: {creds_file}")

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        logger.info("Successfully authenticated with Google Sheets.")
        return client
    except Exception as e:
        logger.error(f"Error authenticating with Google Sheets: {e}")
        raise