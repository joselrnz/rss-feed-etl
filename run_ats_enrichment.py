#!/usr/bin/env python
"""
ATS Enrichment Script

This script analyzes job descriptions against a resume using AI and/or spaCy to determine match percentages
and identify matched/missing skills. It's designed to work with the RSS Feed ETL pipeline to provide
automated job matching capabilities.

Workflow:
1. Reads data from the Google Sheet or a CSV file
2. Filters records from the last specified hours (default: 24) based on the published column
3. Adds a new column as_of_dt with the current timestamp
4. For each recent record:
   - Runs the ATS matcher using the job entry and the specified resume
   - Appends additional columns: match_percentage, missing_skills, matched_skills
5. Writes the enriched result back to the Google Sheet or a CSV file

Features:
- Supports both SCD2 and merge_upsert logic for data loading
- Uses AI (via OpenRouter API) for sophisticated match percentage calculation
- Uses spaCy for skills extraction and as a fallback for match percentage
- Supports batch processing for efficient API usage
- Handles token limits automatically by splitting batches when needed
- Provides detailed logging and error handling
- Configurable via command-line arguments and config file

Usage:
    python run_ats_enrichment.py [options]

Options:
    --spreadsheet_id ID        Google Spreadsheet ID
    --resume PATH              Path to resume file (PDF or DOCX)
    --hours N                  Number of hours to look back for recent entries
    --model MODEL              AI model to use (e.g., "openai/gpt-3.5-turbo")
    --batch-size N             Maximum number of job descriptions per batch
    --limit N                  Process only N records (for testing)
    --percentage-only          Only calculate match percentage, ignore skills
    --save-to-csv              Save results to CSV instead of Google Sheets
    --output-csv PATH          Path to output CSV file
    --dry-run                  Perform a dry run without updating Google Sheets
    --verbose                  Enable verbose output
    --log-to-file              Log to file in addition to console

Environment Variables:
    ENVIRONMENT                Set to "production" for production mode
    GOOGLE_SPREADSHEET_ID      Google Spreadsheet ID
    GOOGLE_CREDS_FILE_PATH     Path to Google service account credentials
    OPENROUTER_API_KEY         OpenRouter API key

Author: Jose Lorenzo Rodriguez
Version: 1.0.0
"""

import os
import sys
import argparse
import logging
import json
import time
import tempfile
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from dotenv import load_dotenv

# Try to import spaCy-related modules
try:
    from src.rss_feed_etl.utils.skills_extractor import SkillsExtractor, SPACY_AVAILABLE
except ImportError:
    SPACY_AVAILABLE = False

# Import the ETL components
from src.rss_feed_etl import RSSFeedETL
from src.rss_feed_etl.utils.auth import get_google_sheets_client
from src.rss_feed_etl.utils.file_utils import read_resume

# Default configuration
DEFAULT_CONFIG = {
    "ats_enrichment": {
        "worksheet_name": "RawJobsPerHour",
        "enriched_suffix": "_Enriched",
        "resume_path": "resume.pdf",
        "hours_lookback": 24,
        "api_url": "https://openrouter.ai/api/v1/chat/completions",
        "model": "openai/gpt-3.5-turbo",
        "max_retries": 3,
        "retry_delay": 2,
        "batch_size": 5,
        "use_batch": True,
        "use_scd2": False,
        "description_column": "summary",
        "use_ai_for_percentage": True,
        "spacy_model": "en_core_web_sm",
        "skills_file": "skills.json",
        "percentage_only": False,
        "save_to_csv": False,
        "output_csv": "ats_results.csv",
        "system_prompt": "You are an ATS (Applicant Tracking System) expert. Analyze the resume and job description for matching skills, experience, and qualifications.",
        "user_prompt": "Resume:\n{resume_text}\n\nJob Description:\n{job_description}\n\nAnalyze the match between this resume and job description as an ATS would. Return a JSON object with the following fields:\n1. match_percentage: A number between 0-100 representing the overall match\n2. missing_skills: An array of skills mentioned in the job description that are not found in the resume\n3. matched_skills: An array of skills that are found in both the resume and job description\n\nReturn ONLY the JSON object, nothing else."
    }
}


def load_config(config_path="config/config.yaml"):
    """Load configuration from YAML file."""
    config = DEFAULT_CONFIG.copy()

    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                yaml_config = yaml.safe_load(f)

            if yaml_config and "ats_enrichment" in yaml_config:
                config["ats_enrichment"].update(yaml_config["ats_enrichment"])
                logging.info(f"Loaded configuration from {config_path}")
        else:
            logging.warning(f"Configuration file not found: {config_path}")
            logging.info("Using default configuration")
    except Exception as e:
        logging.error(f"Error loading configuration: {e}")
        logging.info("Using default configuration")

    return config


def setup_logging(log_level=logging.INFO, log_to_file=True):
    """Set up logging configuration."""
    # Create a more detailed formatter for debugging
    debug_format = "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s"
    # Create a simpler formatter for console output
    console_format = "%(asctime)s - %(levelname)s - %(message)s"

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with simpler format and proper encoding
    try:
        # Use utf-8 encoding for console output to handle non-ASCII characters
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(console_format, datefmt="%Y-%m-%d %H:%M:%S"))
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)
    except Exception as e:
        # Fallback to a safer approach if there are encoding issues
        import codecs
        # Force UTF-8 encoding for stdout
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(console_format, datefmt="%Y-%m-%d %H:%M:%S"))
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)

    # Create logs directory if it doesn't exist and we're logging to file
    if log_to_file:
        try:
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)

            # Create a log file with timestamp for regular logs
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = log_dir / f"ats_enrichment_{timestamp}.log"

            # Create file handler for regular logs
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(console_format, datefmt="%Y-%m-%d %H:%M:%S"))
            file_handler.setLevel(log_level)
            root_logger.addHandler(file_handler)

            # Create a separate debug log file
            debug_file = log_dir / f"ats_enrichment_debug_{timestamp}.log"
            debug_handler = logging.FileHandler(debug_file)
            debug_handler.setFormatter(logging.Formatter(debug_format, datefmt="%Y-%m-%d %H:%M:%S"))
            debug_handler.setLevel(logging.DEBUG)  # Always log debug to this file
            root_logger.addHandler(debug_handler)

            logging.info(f"Logging to {log_file} and {debug_file}")
        except Exception as e:
            logging.warning(f"Could not set up file logging: {e}")

    # Suppress excessive logging from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)


def load_environment():
    """Load environment variables based on the current environment."""
    # Determine environment (default to development)
    env = os.environ.get("ENVIRONMENT", "development").lower()

    # Log the environment
    logging.info(f"Running in {env.upper()} environment")

    # Try environment-specific .env file first
    env_file = Path(f"config/.env.{env}")
    if env_file.exists():
        logging.info(f"Loading environment from {env_file}")
        load_dotenv(env_file)
        return

    # Fall back to regular .env files
    for env_path in [Path("config/.env"), Path(".env")]:
        if env_path.exists():
            logging.info(f"Loading environment from {env_path}")
            load_dotenv(env_path)
            return

    # If we get here, no .env file was found
    logging.warning("No .env file found. Using environment variables or defaults.")


def call_openrouter_api(messages, api_key, model, config):
    """Make API call to OpenRouter with retry logic."""
    if not api_key:
        logging.error("API key is missing. Please provide a valid API key.")
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "model": model,
        "messages": messages
    }

    api_url = config["ats_enrichment"]["api_url"]
    max_retries = config["ats_enrichment"]["max_retries"]
    retry_delay = config["ats_enrichment"]["retry_delay"]

    logging.debug(f"Making API call to {api_url} with model {model}")

    for attempt in range(max_retries):
        try:
            logging.debug(f"API call attempt {attempt + 1}/{max_retries}")
            response = requests.post(api_url, headers=headers, json=data, timeout=30)

            logging.debug(f"API response status code: {response.status_code}")

            if response.status_code == 200:
                response_json = response.json()

                # Validate response structure
                if 'choices' not in response_json:
                    logging.error(f"API response missing 'choices' key: {response_json}")
                    time.sleep(retry_delay)
                    continue

                if not response_json['choices'] or 'message' not in response_json['choices'][0]:
                    logging.error(f"API response has invalid 'choices' structure: {response_json}")
                    time.sleep(retry_delay)
                    continue

                return response_json
            elif response.status_code == 401:  # Unauthorized
                logging.error("API key is invalid or expired")
                return None  # No point in retrying with the same invalid key
            elif response.status_code == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                logging.warning(f"Rate limit exceeded. Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
            else:
                logging.error(f"API request failed with status code {response.status_code}")
                logging.error(f"Response: {response.text}")
                time.sleep(retry_delay)  # Wait before retry
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            time.sleep(retry_delay)  # Wait before retry
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse API response as JSON: {e}")
            time.sleep(retry_delay)
        except Exception as e:
            logging.error(f"Unexpected error during API call: {e}")
            logging.debug("Exception details:", exc_info=True)
            time.sleep(retry_delay)

    logging.error(f"Failed after {max_retries} attempts")
    return None


def analyze_job_batch(resume_text, job_descriptions, api_key, model, config):
    """
    Analyze match percentages between a resume and multiple job descriptions in a batch.

    Args:
        resume_text: Text content of the resume
        job_descriptions: List of dictionaries with 'id', 'title', and 'description' keys
        api_key: OpenRouter API key
        model: Model to use for AI analysis
        config: Configuration dictionary

    Returns:
        Dictionary mapping job IDs to match data (with match_percentage only)
    """
    if not job_descriptions:
        return {}

    # Get batch size from config
    batch_size = config["ats_enrichment"]["batch_size"]

    # Process job descriptions in batches
    results = {}

    total_batches = (len(job_descriptions) + batch_size - 1) // batch_size
    logging.info(f"Processing {len(job_descriptions)} jobs in {total_batches} batches (batch size: {batch_size})")

    for i in range(0, len(job_descriptions), batch_size):
        batch = job_descriptions[i:i+batch_size]
        current_batch = i//batch_size + 1
        logging.info(f"Processing batch {current_batch}/{total_batches} with {len(batch)} jobs")

        # Format job descriptions for the batch
        formatted_jobs = []
        for idx, job in enumerate(batch):
            job_id = job.get('id')
            job_title = job.get('title', f"Job {job_id}")
            job_desc = job.get('description', '')

            # Log job description length for debugging
            if len(job_desc) > 5000:
                logging.debug(f"Long job description for {job_title}: {len(job_desc)} characters")

            formatted_jobs.append(f"JOB #{idx+1}:\nTitle: {job_title}\nDescription: {job_desc}")

        all_jobs_text = "\n\n".join(formatted_jobs)

        # Create the prompt for match percentage only
        messages = [
            {
                "role": "system",
                "content": "You are an ATS (Applicant Tracking System) expert. Analyze the resume against multiple job descriptions and provide match percentages."
            },
            {
                "role": "user",
                "content": f"Resume:\n{resume_text}\n\nJob Descriptions:\n{all_jobs_text}\n\nFor each job, calculate the percentage match between this resume and the job description as an ATS would. Return the results in JSON format as a list of objects with job_number and match_percentage, like this: [{{'job_number': 1, 'match_percentage': 85}}, {{'job_number': 2, 'match_percentage': 72}}]"
            }
        ]

        # Make the API call
        result = call_openrouter_api(messages, api_key, model, config)

        if not result:
            # If API call fails, return empty results for this batch
            for job in batch:
                job_id = job.get('id')
                results[job_id] = {
                    "match_percentage": 0
                }
            continue

        content = result['choices'][0]['message']['content'].strip()

        # Try to parse JSON response
        try:
            # Find JSON in the response (it might be embedded in text)
            json_start = content.find('[')
            json_end = content.rfind(']') + 1

            if json_start >= 0 and json_end > json_start:
                json_str = content[json_start:json_end]
                batch_results = json.loads(json_str)

                # Map results to job IDs
                for idx, match_data in enumerate(batch_results):
                    if idx < len(batch):  # Ensure we don't go out of bounds
                        job_id = batch[idx].get('id')

                        results[job_id] = {
                            "match_percentage": match_data.get("match_percentage", 0)
                        }
        except json.JSONDecodeError:
            # Safely log error with non-ASCII content
            try:
                # Replace non-ASCII characters with their Unicode escape sequences
                safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
                logging.error(f"Failed to parse JSON response: {safe_content}")
            except Exception as e:
                logging.error(f"Failed to parse JSON response (content logging failed: {e})")
            # If parsing fails, return empty results for this batch
            for job in batch:
                job_id = job.get('id')
                results[job_id] = {
                    "match_percentage": 0
                }

    return results


def analyze_job_match(resume_text, job_description, api_key, model, config):
    """
    Analyze the match percentage between a resume and job description using AI.

    Returns:
        Dictionary with match_percentage (only)
    """
    # Create a simplified prompt that only asks for match percentage
    messages = [
        {
            "role": "system",
            "content": "You are an ATS (Applicant Tracking System) expert. Your task is to analyze a resume against a job description and provide a match percentage as a single number."
        },
        {
            "role": "user",
            "content": f"Resume:\n{resume_text}\n\nJob Description:\n{job_description}\n\nThe resume is for a Data Engineer with skills in Python, SQL, ETL, and Big Data technologies.\n\nCalculate the percentage match between this resume and job description as an ATS would.\n\nYou MUST respond with ONLY a single number between 0 and 100 representing the match percentage. For example: '75' or '75%'.\n\nDo NOT include any explanations, tables, or additional text. Your entire response must be just the percentage number."
        }
    ]

    result = call_openrouter_api(messages, api_key, model, config)

    if not result:
        logging.warning("API call returned no result")
        return {
            "match_percentage": 0
        }

    try:
        if 'choices' not in result or not result['choices'] or 'message' not in result['choices'][0]:
            logging.error(f"Invalid API response structure: {result}")
            return {
                "match_percentage": 0
            }

        content = result['choices'][0]['message']['content'].strip()
        # Safely log content, handling non-ASCII characters
        try:
            # Replace non-ASCII characters with their Unicode escape sequences
            safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
            logging.debug(f"API response content: {safe_content}")
        except Exception as e:
            logging.debug(f"Could not log API response content: {e}")

        # Try to extract the percentage using regex first
        import re
        percentage_pattern = re.compile(r'(\d+)%?')
        matches = percentage_pattern.findall(content)

        if matches:
            # Use the first number that looks like a percentage
            for match in matches:
                try:
                    match_percentage = float(match)
                    if 0 <= match_percentage <= 100:  # Validate percentage is in valid range
                        logging.debug(f"Extracted match percentage {match_percentage}% using regex")
                        return {
                            "match_percentage": match_percentage
                        }
                    else:
                        logging.debug(f"Skipping invalid percentage value {match_percentage} (not in range 0-100)")
                except ValueError:
                    logging.debug(f"Could not convert to float: {match}")

        # If regex didn't work, try the old method
        numbers = ''.join(filter(lambda x: x.isdigit() or x == '.', content))
        if not numbers:
            # Safely log warning with non-ASCII content
            try:
                # Replace non-ASCII characters with their Unicode escape sequences
                safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
                logging.warning(f"No numbers found in API response: {safe_content}")
            except Exception as e:
                logging.warning(f"No numbers found in API response (content logging failed: {e})")
            return {
                "match_percentage": 0
            }

        try:
            match_percentage = float(numbers)
            if 0 <= match_percentage <= 100:  # Validate percentage is in valid range
                logging.debug(f"Extracted match percentage {match_percentage}% using filter method")
                return {
                    "match_percentage": match_percentage
                }
            else:
                logging.warning(f"Invalid percentage value {match_percentage} (not in range 0-100)")
                return {
                    "match_percentage": 0
                }
        except ValueError:
            logging.error(f"Could not convert extracted numbers to float: {numbers}")
            return {
                "match_percentage": 0
            }
    except Exception as e:
        logging.error(f"Error processing API response: {e}")
        logging.debug("Exception details:", exc_info=True)
        return {
            "match_percentage": 0
        }


def get_recent_job_entries(spreadsheet_id, config):
    """
    Get job entries from the last specified hours.

    Args:
        spreadsheet_id: Google Spreadsheet ID
        config: Configuration dictionary

    Returns:
        DataFrame containing recent job entries
    """
    worksheet_name = config["ats_enrichment"]["worksheet_name"]
    hours = config["ats_enrichment"]["hours_lookback"]
    # Get Google Sheets client
    creds_file = os.environ.get("GOOGLE_CREDS_FILE_PATH", "secrets/service_account.json")
    client = get_google_sheets_client(creds_file)

    # Open the spreadsheet and worksheet
    sheet = client.open_by_key(spreadsheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    # Get all records
    records = worksheet.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        logging.warning(f"No records found in worksheet: {worksheet_name}")
        return df

    # Convert published column to datetime
    if "published" in df.columns:
        df["published"] = pd.to_datetime(df["published"], errors="coerce")

        # Filter for records in the last specified hours
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_df = df[df["published"] >= cutoff_time]

        logging.info(f"Found {len(recent_df)} records from the last {hours} hours")
        return recent_df
    else:
        logging.warning("No 'published' column found in the data")
        return pd.DataFrame()


def enrich_job_entries(job_entries, api_key, config):
    """
    Enrich job entries with ATS match information.

    Args:
        job_entries: DataFrame containing job entries
        api_key: OpenRouter API key
        config: Configuration dictionary

    Returns:
        DataFrame with enriched job entries
    """
    resume_path = config["ats_enrichment"]["resume_path"]
    model = config["ats_enrichment"]["model"]
    use_batch = config["ats_enrichment"].get("use_batch", True)  # Default to batch processing
    description_column = config["ats_enrichment"].get("description_column", "summary")  # Default to "summary"
    use_ai_for_percentage = config["ats_enrichment"].get("use_ai_for_percentage", True)  # Default to using AI for percentage
    percentage_only = config["ats_enrichment"].get("percentage_only", False)  # Default to extracting skills

    # Initialize spaCy skills extractor if not in percentage-only mode
    skills_extractor = None
    if not percentage_only and SPACY_AVAILABLE:
        spacy_model = config["ats_enrichment"].get("spacy_model", "en_core_web_sm")
        skills_file = config["ats_enrichment"].get("skills_file", "skills.json")
        skills_extractor = SkillsExtractor(skills_file, spacy_model)
        logging.info(f"Using spaCy for skills extraction with model: {spacy_model}")
        if skills_file:
            logging.info(f"Using skills file: {skills_file}")
    elif percentage_only:
        logging.info("Running in percentage-only mode. Skills extraction is disabled.")
    elif not SPACY_AVAILABLE:
        logging.warning("spaCy is not available. Skills extraction will be limited.")

    if job_entries.empty:
        return job_entries

    # Read resume
    try:
        resume_text = read_resume(resume_path)
        if not resume_text or len(resume_text.strip()) == 0:
            logging.warning("Extracted empty text from resume")
            return job_entries

        # Log resume length for debugging
        logging.debug(f"Resume length: {len(resume_text)} characters")
    except Exception as e:
        logging.error(f"Error reading resume: {e}")
        return job_entries

    # Add as_of_dt column with current timestamp
    enriched_df = job_entries.copy()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    enriched_df["as_of_dt"] = current_time

    # Initialize new columns
    enriched_df["match_percentage"] = 0.0
    if not percentage_only:
        enriched_df["missing_skills"] = None
        enriched_df["matched_skills"] = None

    # Determine whether to use batch processing or individual processing
    if use_batch and len(enriched_df) > 1:
        logging.info(f"Using batch processing for {len(enriched_df)} job entries")

        # Prepare job descriptions for batch processing
        job_descriptions = []
        for idx, row in enriched_df.iterrows():
            job_description = row.get(description_column, "")
            job_title = row.get("job_title", f"Job {idx}")

            if not job_description:
                logging.warning(f"Empty job description for {job_title}")
                continue

            job_descriptions.append({
                'id': idx,
                'title': job_title,
                'description': job_description
            })

        # Process job descriptions in batches
        batch_results = {}

        # First, use spaCy for skills extraction (if not in percentage-only mode)
        if not percentage_only and skills_extractor:
            # Process each job individually with spaCy for skills
            for job in job_descriptions:
                job_id = job.get('id')
                job_desc = job.get('description', '')
                spacy_results = skills_extractor.analyze_match(resume_text, job_desc)

                # Initialize result with spaCy data
                batch_results[job_id] = {
                    "match_percentage": 0,  # Will be updated by AI if enabled
                    "missing_skills": spacy_results.get("missing_skills", []),
                    "matched_skills": spacy_results.get("matched_skills", [])
                }
        else:
            # Initialize empty results if in percentage-only mode or spaCy is not available
            for job in job_descriptions:
                job_id = job.get('id')
                if percentage_only:
                    batch_results[job_id] = {
                        "match_percentage": 0
                    }
                else:
                    batch_results[job_id] = {
                        "match_percentage": 0,
                        "missing_skills": [],
                        "matched_skills": []
                    }

        # Then, use AI for match percentage if enabled
        if use_ai_for_percentage:
            # Create a simplified prompt that only asks for match percentages
            formatted_jobs = []
            for idx, job in enumerate(job_descriptions):
                job_id = job.get('id')
                job_title = job.get('title', f"Job {job_id}")
                job_desc = job.get('description', '')

                # Log job description length for debugging
                if len(job_desc) > 5000:
                    logging.debug(f"Long job description for {job_title}: {len(job_desc)} characters")

                formatted_jobs.append(f"JOB #{idx+1}:\nTitle: {job_title}\nDescription: {job_desc}")

            all_jobs_text = "\n\n".join(formatted_jobs)

            # Estimate token count (rough approximation: 1 token ≈ 4 characters)
            prompt_text = f"Resume:\n{resume_text}\n\nJob Descriptions:\n{all_jobs_text}"
            estimated_tokens = len(prompt_text) // 4
            max_tokens = 60000  # Leave some buffer below the 64k limit

            if estimated_tokens > max_tokens:
                logging.warning(f"Estimated token count ({estimated_tokens}) exceeds limit ({max_tokens}). Processing smaller batch.")
                # Process a smaller batch
                if len(job_descriptions) > 1:
                    # Split the job_descriptions in half and process recursively
                    mid = len(job_descriptions) // 2
                    first_half = job_descriptions[:mid]
                    second_half = job_descriptions[mid:]

                    logging.info(f"Splitting batch of {len(job_descriptions)} into two batches of {len(first_half)} and {len(second_half)}")

                    # Process each half
                    first_results = analyze_job_batch(resume_text, first_half, api_key, model, config)
                    second_results = analyze_job_batch(resume_text, second_half, api_key, model, config)

                    # Combine results
                    for job_id, result in first_results.items():
                        batch_results[job_id] = result
                    for job_id, result in second_results.items():
                        batch_results[job_id] = result

                    # Skip the API call for this batch since we've processed it in smaller chunks
                    return batch_results
                else:
                    # If we can't split further, process the single job individually
                    logging.warning("Cannot split batch further. Processing single job individually.")

                    # Get the job details
                    job = job_descriptions[0]
                    job_id = job.get('id')
                    job_title = job.get('title', f"Job {job_id}")
                    job_desc = job.get('description', '')

                    # Use the individual job processing approach
                    use_ai_for_percentage = config["ats_enrichment"].get("use_ai_for_percentage", True)
                    if use_ai_for_percentage:
                        # Create a simplified prompt for just this one job
                        single_job_messages = [
                            {"role": "system", "content": "You are an ATS (Applicant Tracking System) expert. Your task is to analyze a resume against a job description and provide a match percentage as a single number."},
                            {"role": "user", "content": f"Resume:\n{resume_text}\n\nJob Description:\nTitle: {job_title}\n{job_desc}\n\nThe resume is for a Data Engineer with skills in Python, SQL, ETL, and Big Data technologies.\n\nCalculate the percentage match between this resume and job description as an ATS would.\n\nYou MUST respond with ONLY a single number between 0 and 100 representing the match percentage. For example: '75' or '75%'.\n\nDo NOT include any explanations, tables, or additional text. Your entire response must be just the percentage number."}
                        ]

                        # Make the API call for just this one job
                        single_result = call_openrouter_api(single_job_messages, api_key, model, config)

                        if single_result and 'choices' in single_result and single_result['choices'] and 'message' in single_result['choices'][0]:
                            try:
                                content = single_result['choices'][0]['message']['content'].strip()

                                # Try to extract the percentage using regex first
                                import re
                                percentage_pattern = re.compile(r'(\d+)%?')
                                matches = percentage_pattern.findall(content)

                                if matches:
                                    # Use the first number that looks like a percentage
                                    for match in matches:
                                        try:
                                            match_percentage = float(match)
                                            if 0 <= match_percentage <= 100:  # Validate percentage is in valid range
                                                batch_results[job_id]["match_percentage"] = match_percentage
                                                logging.debug(f"Extracted match percentage {match_percentage}% using regex")
                                                break
                                            else:
                                                logging.debug(f"Skipping invalid percentage value {match_percentage} (not in range 0-100)")
                                        except ValueError:
                                            logging.debug(f"Could not convert to float: {match}")

                                # If regex didn't work, try the old method
                                if batch_results[job_id]["match_percentage"] == 0:
                                    numbers = ''.join(filter(lambda x: x.isdigit() or x == '.', content))
                                    if numbers:
                                        try:
                                            match_percentage = float(numbers)
                                            if 0 <= match_percentage <= 100:
                                                batch_results[job_id]["match_percentage"] = match_percentage
                                                logging.debug(f"Extracted match percentage {match_percentage}% using filter method")
                                        except ValueError:
                                            logging.error(f"Could not convert extracted numbers to float: {numbers}")
                            except Exception as e:
                                logging.error(f"Error processing API response for individual job: {e}")

                    # Return the results without making the batch API call
                    return batch_results

            # Create the prompt for match percentage only
            messages = [
                {"role": "system", "content": "You are an ATS (Applicant Tracking System) expert. Your task is to analyze a resume against multiple job descriptions and provide match percentages in a specific JSON format."},
                {"role": "user", "content": f"Resume:\n{resume_text}\n\nJob Descriptions:\n{all_jobs_text}\n\nThe resume is for a Data Engineer with skills in Python, SQL, ETL, and Big Data technologies.\n\nFor each job, calculate the percentage match between this resume and the job description as an ATS would.\n\nYou MUST respond with ONLY a JSON array in the following format:\n[{{'job_number': 1, 'match_percentage': 85}}, {{'job_number': 2, 'match_percentage': 72}}]\n\nDo NOT include any explanations, tables, or additional text. Your entire response must be valid JSON that starts with '[' and ends with ']'. Each job_number should correspond to the job number in the input (starting from 1), and each match_percentage should be a number between 0 and 100."}
            ]

            # Make the API call
            result = call_openrouter_api(messages, api_key, model, config)

            if result and 'choices' in result and result['choices'] and 'message' in result['choices'][0]:
                try:
                    content = result['choices'][0]['message']['content'].strip()
                    # Safely log content, handling non-ASCII characters
                    try:
                        # Try to log the first 100 characters
                        preview = content[:100]
                        # Replace non-ASCII characters with their Unicode escape sequences if needed
                        safe_preview = preview.encode('ascii', 'backslashreplace').decode('ascii')
                        logging.debug(f"API response content preview: {safe_preview}...")
                    except Exception as e:
                        logging.debug(f"Could not log API response content: {e}")

                    # Try to parse JSON response
                    try:
                        # First, try to extract percentages directly from the response using regex
                        import re

                        # Look for patterns like "job_number: 1, match_percentage: 63%" or similar
                        percentage_pattern = re.compile(r'job_number:?\s*(\d+)[,\s]*match_percentage:?\s*(\d+)')
                        matches = percentage_pattern.findall(content)

                        if matches:
                            logging.debug(f"Found {len(matches)} job matches using regex")
                            # Create results from regex matches
                            for match in matches:
                                job_num = int(match[0]) - 1  # Convert to 0-based index
                                if job_num < len(job_descriptions):
                                    job_id = job_descriptions[job_num].get('id')
                                    if job_id in batch_results:
                                        try:
                                            match_percentage = int(match[1])
                                            batch_results[job_id]["match_percentage"] = match_percentage
                                            logging.debug(f"Set job {job_id} match to {match_percentage}% using regex")
                                        except ValueError:
                                            logging.warning(f"Could not convert match percentage to int: {match[1]}")
                        else:
                            # Try to find JSON array in the response
                            json_start = content.find('[')
                            json_end = content.rfind(']') + 1

                            if json_start >= 0 and json_end > json_start:
                                json_str = content[json_start:json_end]

                                # Clean up the JSON string to handle common formatting issues
                                json_str = json_str.replace("'", '"')  # Replace single quotes with double quotes
                                json_str = re.sub(r'(\w+):', r'"\1":', json_str)  # Add quotes around keys
                                json_str = re.sub(r':\s*(\d+)%', r': \1', json_str)  # Remove % signs from values

                                try:
                                    ai_results = json.loads(json_str)

                                    # Update match percentages in batch_results
                                    for idx, ai_result in enumerate(ai_results):
                                        if idx < len(job_descriptions):  # Ensure we don't go out of bounds
                                            job_id = job_descriptions[idx].get('id')
                                            if job_id in batch_results:
                                                match_percentage = ai_result.get("match_percentage", 0)
                                                # Handle case where match_percentage might be a string with % sign
                                                if isinstance(match_percentage, str):
                                                    match_percentage = match_percentage.rstrip('%')
                                                    try:
                                                        match_percentage = float(match_percentage)
                                                    except ValueError:
                                                        match_percentage = 0
                                                batch_results[job_id]["match_percentage"] = match_percentage
                                                logging.debug(f"Set job {job_id} match to {match_percentage}% using JSON")
                                except json.JSONDecodeError as e:
                                    logging.error(f"Failed to parse cleaned JSON: {e}")
                                    logging.debug(f"Cleaned JSON string: {json_str}")

                                    # Try one more approach - extract percentages from the JSON string
                                    percentage_pattern = re.compile(r'"job_number":\s*(\d+).*?"match_percentage":\s*(\d+)')
                                    matches = percentage_pattern.findall(json_str)

                                    if matches:
                                        logging.debug(f"Found {len(matches)} job matches using JSON regex")
                                        # Create results from regex matches
                                        for match in matches:
                                            job_num = int(match[0]) - 1  # Convert to 0-based index
                                            if job_num < len(job_descriptions):
                                                job_id = job_descriptions[job_num].get('id')
                                                if job_id in batch_results:
                                                    try:
                                                        match_percentage = int(match[1])
                                                        batch_results[job_id]["match_percentage"] = match_percentage
                                                        logging.debug(f"Set job {job_id} match to {match_percentage}% using JSON regex")
                                                    except ValueError:
                                                        logging.warning(f"Could not convert match percentage to int: {match[1]}")
                            else:
                                # Safely log warning with non-ASCII content
                                try:
                                    # Replace non-ASCII characters with their Unicode escape sequences
                                    safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
                                    logging.warning(f"Could not find JSON array in response: {safe_content}")
                                except Exception as e:
                                    logging.warning(f"Could not find JSON array in response (content logging failed: {e})")
                    except Exception as e:
                        # Safely log error with non-ASCII content
                        logging.error(f"Error processing API response: {e}")
                        try:
                            # Replace non-ASCII characters with their Unicode escape sequences
                            safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
                            logging.debug(f"Response content: {safe_content}")
                        except Exception as e:
                            logging.error(f"Failed to log response content: {e}")
                except Exception as e:
                    logging.error(f"Error processing API response: {e}")
                    logging.debug("Exception details:", exc_info=True)
            else:
                logging.warning("API call failed or returned invalid response structure")

        # If no match percentages were set, calculate them using spaCy results
        for job_id, result in batch_results.items():
            if result["match_percentage"] == 0 and skills_extractor:
                # Find the job in job_descriptions
                job_desc = ""
                for job in job_descriptions:
                    if job.get('id') == job_id:
                        job_desc = job.get('description', '')
                        break

                # Calculate match percentage using spaCy
                if job_desc:
                    spacy_results = skills_extractor.analyze_match(resume_text, job_desc)
                    result["match_percentage"] = spacy_results.get("match_percentage", 0)

        # Update DataFrame with batch results
        for idx, row in enriched_df.iterrows():
            if idx in batch_results:
                match_data = batch_results[idx]

                enriched_df.at[idx, "match_percentage"] = match_data["match_percentage"]
                if not percentage_only:
                    enriched_df.at[idx, "missing_skills"] = json.dumps(match_data.get("missing_skills", []))
                    enriched_df.at[idx, "matched_skills"] = json.dumps(match_data.get("matched_skills", []))

                logging.info(f"  Job: {row.get('job_title', f'Job {idx}')} - Match: {match_data['match_percentage']}%")
    else:
        # Process each job entry individually
        logging.info("Using individual processing for job entries")

        for idx, row in enriched_df.iterrows():
            job_description = row.get(description_column, "")
            job_title = row.get("job_title", f"Job {idx}")

            logging.info(f"Processing job: {job_title}")

            if not job_description:
                logging.warning(f"Empty job description for {job_title}")
                continue

            # Initialize match data
            match_data = {
                "match_percentage": 0
            }

            # Add skills fields if not in percentage-only mode
            if not percentage_only:
                match_data["missing_skills"] = []
                match_data["matched_skills"] = []

            # Use spaCy for skills extraction if not in percentage-only mode
            if not percentage_only and skills_extractor:
                spacy_results = skills_extractor.analyze_match(resume_text, job_description)
                match_data["missing_skills"] = spacy_results.get("missing_skills", [])
                match_data["matched_skills"] = spacy_results.get("matched_skills", [])

                # Use spaCy for match percentage as fallback
                match_data["match_percentage"] = spacy_results.get("match_percentage", 0)

            # Then, use AI for match percentage if enabled
            if use_ai_for_percentage:
                # Create a simplified prompt that only asks for match percentage
                messages = [
                    {"role": "system", "content": "You are an ATS (Applicant Tracking System) expert. Your task is to analyze a resume against a job description and provide a match percentage as a single number."},
                    {"role": "user", "content": f"Resume:\n{resume_text}\n\nJob Description:\n{job_description}\n\nThe resume is for a Data Engineer with skills in Python, SQL, ETL, and Big Data technologies.\n\nCalculate the percentage match between this resume and job description as an ATS would.\n\nYou MUST respond with ONLY a single number between 0 and 100 representing the match percentage. For example: '75' or '75%'.\n\nDo NOT include any explanations, tables, or additional text. Your entire response must be just the percentage number."}
                ]

                # Make the API call
                result = call_openrouter_api(messages, api_key, model, config)

                if result and 'choices' in result and result['choices'] and 'message' in result['choices'][0]:
                    try:
                        content = result['choices'][0]['message']['content'].strip()
                        # Safely log content, handling non-ASCII characters
                        try:
                            # Replace non-ASCII characters with their Unicode escape sequences
                            safe_content = content.encode('ascii', 'backslashreplace').decode('ascii')
                            logging.debug(f"API response for job {job_title}: {safe_content}")
                        except Exception as e:
                            logging.debug(f"Could not log API response for job {job_title}: {e}")

                        # Try to extract the percentage using regex first
                        import re
                        percentage_pattern = re.compile(r'(\d+)%?')
                        matches = percentage_pattern.findall(content)

                        if matches:
                            # Use the first number that looks like a percentage
                            for match in matches:
                                try:
                                    match_percentage = float(match)
                                    if 0 <= match_percentage <= 100:  # Validate percentage is in valid range
                                        match_data["match_percentage"] = match_percentage
                                        logging.debug(f"Extracted match percentage {match_percentage}% using regex")
                                        break
                                    else:
                                        logging.debug(f"Skipping invalid percentage value {match_percentage} (not in range 0-100)")
                                except ValueError:
                                    logging.debug(f"Could not convert to float: {match}")

                        # If regex didn't work, try the old method
                        if match_data["match_percentage"] == 0:
                            # Extract just the percentage number
                            numbers = ''.join(filter(lambda x: x.isdigit() or x == '.', content))
                            if numbers:
                                try:
                                    match_percentage = float(numbers)
                                    if 0 <= match_percentage <= 100:  # Validate percentage is in valid range
                                        match_data["match_percentage"] = match_percentage
                                        logging.debug(f"Extracted match percentage {match_percentage}% using filter method")
                                    else:
                                        logging.warning(f"Invalid percentage value {match_percentage} (not in range 0-100)")
                                except ValueError:
                                    logging.error(f"Could not convert extracted numbers to float: {numbers}")
                            else:
                                logging.warning(f"No numbers found in API response: {content}")
                    except Exception as e:
                        logging.error(f"Error processing API response: {e}")
                        logging.debug("Exception details:", exc_info=True)
                else:
                    logging.warning(f"API call failed for job {job_title} or returned invalid response structure")

            # Update DataFrame with match data
            enriched_df.at[idx, "match_percentage"] = match_data["match_percentage"]
            if not percentage_only:
                enriched_df.at[idx, "missing_skills"] = json.dumps(match_data["missing_skills"])
                enriched_df.at[idx, "matched_skills"] = json.dumps(match_data["matched_skills"])

            logging.info(f"  Match: {match_data['match_percentage']}%")
            if not percentage_only:
                logging.info(f"  Missing skills: {match_data.get('missing_skills', [])}")
                logging.info(f"  Matched skills: {match_data.get('matched_skills', [])}")

    # Log the type of object being returned
    logging.debug(f"enrich_job_entries returning object of type: {type(enriched_df)}")
    if isinstance(enriched_df, pd.DataFrame):
        logging.debug(f"DataFrame shape: {enriched_df.shape}")
    elif isinstance(enriched_df, dict):
        logging.debug(f"Dictionary with {len(enriched_df)} keys")

    return enriched_df


def write_enriched_data(enriched_df, spreadsheet_id, worksheet_name, config):
    """
    Write enriched data back to Google Sheets or CSV.

    Args:
        enriched_df: DataFrame with enriched job entries
        spreadsheet_id: Google Spreadsheet ID
        worksheet_name: Name of the worksheet
        config: Configuration dictionary

    Returns:
        True if successful, False otherwise
    """
    use_scd2 = config["ats_enrichment"]["use_scd2"]
    enriched_suffix = config["ats_enrichment"]["enriched_suffix"]
    save_to_csv = config["ats_enrichment"].get("save_to_csv", False)
    output_csv = config["ats_enrichment"].get("output_csv", "ats_results.csv")

    # Check if there's data to write
    if isinstance(enriched_df, pd.DataFrame):
        if enriched_df.empty:
            logging.warning("No enriched data to write (DataFrame is empty)")
            return False
    elif not enriched_df:  # If it's a dict or other container, check if it's empty
        logging.warning("No enriched data to write (container is empty)")
        return False

    try:
        if save_to_csv:
            # Save to CSV file
            if isinstance(enriched_df, pd.DataFrame):
                # If it's already a DataFrame, save it directly
                enriched_df.to_csv(output_csv, index=False)
            else:
                # If it's a dictionary or other structure, convert to DataFrame first
                try:
                    # Try to convert to DataFrame
                    if isinstance(enriched_df, dict):
                        # If it's a dict of dicts, convert to DataFrame
                        df = pd.DataFrame.from_dict(enriched_df, orient='index')
                    else:
                        # Otherwise, try to create a DataFrame from it
                        df = pd.DataFrame(enriched_df)
                    df.to_csv(output_csv, index=False)
                except Exception as e:
                    logging.error(f"Error converting data to DataFrame: {e}")
                    # Fallback: write as JSON
                    with open(output_csv, 'w') as f:
                        json.dump(enriched_df, f, indent=2)
                    logging.warning(f"Data written as JSON instead of CSV due to conversion error")

            logging.info(f"Enriched data written to CSV file: {output_csv}")
            return True
        else:
            # Save to Google Sheets
            # Get Google Sheets client
            creds_file = os.environ.get("GOOGLE_CREDS_FILE_PATH", "secrets/service_account.json")
            client = get_google_sheets_client(creds_file)

            # Convert to DataFrame if needed
            if not isinstance(enriched_df, pd.DataFrame):
                try:
                    # Try to convert to DataFrame
                    if isinstance(enriched_df, dict):
                        # If it's a dict of dicts, convert to DataFrame
                        enriched_df = pd.DataFrame.from_dict(enriched_df, orient='index')
                    else:
                        # Otherwise, try to create a DataFrame from it
                        enriched_df = pd.DataFrame(enriched_df)
                except Exception as e:
                    logging.error(f"Error converting data to DataFrame for Google Sheets: {e}")
                    return False

            # Create a temporary CSV file
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
                temp_path = temp_file.name
                enriched_df.to_csv(temp_path, index=False)

            # Initialize ETL with appropriate strategy
            etl = RSSFeedETL(
                spreadsheet_id=spreadsheet_id,
                creds_file=creds_file,
                use_scd2=use_scd2
            )

            # Load the enriched data to the sheet
            results = etl.data_loader.load_to_sheet(
                client=client,
                new_df=enriched_df,
                spreadsheet_id=spreadsheet_id,
                worksheet_name=f"{worksheet_name}{enriched_suffix}"
            )

            # Clean up temporary file
            os.unlink(temp_path)

            logging.info(f"Enriched data written to {worksheet_name}{enriched_suffix}")
            logging.info(f"Inserted: {results['inserted']}, Updated: {results['updated']}")
            if use_scd2:
                logging.info(f"Removed: {results['removed']}")

            return True
    except Exception as e:
        logging.error(f"Error writing enriched data: {e}")
        logging.debug("Exception details:", exc_info=True)
        return False


def main():
    """Run the ATS enrichment process."""
    # Parse command-line arguments for logging
    parser = argparse.ArgumentParser(description="ATS Enrichment Process")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output (same as --log-level DEBUG)"
    )
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Log to file in addition to console"
    )

    # Parse just the logging arguments first
    log_args, _ = parser.parse_known_args()

    # Set up logging
    if log_args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = getattr(logging, log_args.log_level)
    setup_logging(log_level=log_level, log_to_file=log_args.log_to_file)

    # Log system information for debugging
    if log_level == logging.DEBUG:
        logging.debug(f"Python version: {sys.version}")
        logging.debug(f"Operating system: {os.name} - {sys.platform}")
        logging.debug(f"Current working directory: {os.getcwd()}")
        logging.debug(f"Command line arguments: {sys.argv}")
        logging.debug(f"Environment variables: ENVIRONMENT={os.environ.get('ENVIRONMENT', 'Not set')}")

    # Load environment variables
    load_environment()

    # Parse all arguments again (now that environment variables are loaded)
    parser = argparse.ArgumentParser(description="ATS Enrichment Process")

    # Add logging arguments again (for help text)
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output (same as --log-level DEBUG)"
    )
    parser.add_argument(
        "--log-to-file",
        action="store_true",
        help="Log to file in addition to console"
    )

    # Add ATS enrichment arguments
    parser.add_argument(
        "--config",
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--spreadsheet_id",
        help="Google Spreadsheet ID (overrides config file)",
        default=os.environ.get("GOOGLE_SPREADSHEET_ID", "")
    )
    parser.add_argument(
        "--worksheet_name",
        help="Name of the worksheet containing job entries (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--resume",
        help="Path to resume file (PDF or DOCX) (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--hours",
        type=int,
        help="Number of hours to look back for recent job entries (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--api_key",
        help="OpenRouter API key (overrides config file)",
        default=os.environ.get("OPENROUTER_API_KEY", "")
    )
    parser.add_argument(
        "--model",
        help="Model to use for AI analysis (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Maximum number of job descriptions per batch (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--use-batch",
        action="store_true",
        help="Use batch processing for job descriptions (overrides config file)"
    )
    parser.add_argument(
        "--no-batch",
        action="store_true",
        help="Disable batch processing for job descriptions (overrides config file)"
    )
    parser.add_argument(
        "--description-column",
        help="Column name containing job descriptions (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--save-to-csv",
        action="store_true",
        help="Save results to CSV instead of Google Sheets (overrides config file)"
    )
    parser.add_argument(
        "--output-csv",
        help="Output CSV file name (overrides config file)",
        default=None
    )

    parser.add_argument(
        "--use-ai-for-percentage",
        action="store_true",
        help="Use AI for calculating match percentage (overrides config file)"
    )
    parser.add_argument(
        "--no-ai-for-percentage",
        action="store_true",
        help="Disable AI for calculating match percentage (overrides config file)"
    )
    parser.add_argument(
        "--percentage-only",
        action="store_true",
        help="Only calculate match percentage and ignore skills extraction (overrides config file)"
    )
    parser.add_argument(
        "--spacy-model",
        help="spaCy model to use for skills extraction (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--skills-file",
        help="Path to JSON file containing skills list (overrides config file)",
        default=None
    )
    parser.add_argument(
        "--use_scd2",
        action="store_true",
        help="Use SCD2 pattern for tracking historical changes (overrides config file)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without updating Google Sheets"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of records to process (for testing)",
        default=None
    )

    # Parse all arguments
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Override configuration with command-line arguments
    if args.worksheet_name:
        config["ats_enrichment"]["worksheet_name"] = args.worksheet_name

    if args.resume:
        config["ats_enrichment"]["resume_path"] = args.resume

    if args.hours:
        config["ats_enrichment"]["hours_lookback"] = args.hours

    if args.model:
        config["ats_enrichment"]["model"] = args.model

    if args.batch_size:
        config["ats_enrichment"]["batch_size"] = args.batch_size

    # Handle batch processing options
    if args.use_batch:
        config["ats_enrichment"]["use_batch"] = True
    elif args.no_batch:
        config["ats_enrichment"]["use_batch"] = False

    # Handle description column option
    if args.description_column:
        config["ats_enrichment"]["description_column"] = args.description_column

    # Handle CSV output options
    if args.save_to_csv:
        config["ats_enrichment"]["save_to_csv"] = True
    if args.output_csv:
        config["ats_enrichment"]["output_csv"] = args.output_csv

    # Handle AI options
    if args.use_ai_for_percentage:
        config["ats_enrichment"]["use_ai_for_percentage"] = True
    elif args.no_ai_for_percentage:
        config["ats_enrichment"]["use_ai_for_percentage"] = False

    # Handle percentage-only option
    if args.percentage_only:
        config["ats_enrichment"]["percentage_only"] = True
    if args.spacy_model:
        config["ats_enrichment"]["spacy_model"] = args.spacy_model
    if args.skills_file:
        config["ats_enrichment"]["skills_file"] = args.skills_file

    if args.use_scd2:
        config["ats_enrichment"]["use_scd2"] = True

    # Validate required arguments
    if not args.spreadsheet_id:
        logging.error("ERROR: Spreadsheet ID is required. Provide it with --spreadsheet_id or set the GOOGLE_SPREADSHEET_ID environment variable.")
        return 1

    # Check if API key is required (only if use_ai_for_percentage is True)
    use_ai_for_percentage = config["ats_enrichment"].get("use_ai_for_percentage", True)
    if use_ai_for_percentage:
        # Check API key
        if not args.api_key:
            logging.error("ERROR: OpenRouter API key is required when using AI for percentage calculation.")
            logging.error("Provide it with --api_key or set the OPENROUTER_API_KEY environment variable.")
            logging.error("Alternatively, use --no-ai-for-percentage to use spaCy for percentage calculation instead.")
            return 1

        # Check API URL
        api_url = config["ats_enrichment"].get("api_url", "")
        if not api_url:
            logging.error("ERROR: API URL is missing in the configuration.")
            logging.error("Please check your config.yaml file and ensure the api_url is set correctly.")
            return 1

        # Check model
        model = config["ats_enrichment"].get("model", "")
        if not model:
            logging.error("ERROR: AI model is missing in the configuration.")
            logging.error("Please check your config.yaml file and ensure the model is set correctly.")
            return 1

    # Check if resume file exists
    resume_path = Path(config["ats_enrichment"]["resume_path"])
    if not resume_path.exists():
        logging.error(f"ERROR: Resume file not found: {resume_path}")
        return 1

    # Start the enrichment process
    start_time = datetime.now()
    logging.info("Starting ATS enrichment process")
    logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Spreadsheet ID: {args.spreadsheet_id}")
    logging.info(f"Worksheet: {config['ats_enrichment']['worksheet_name']}")
    logging.info(f"Resume: {config['ats_enrichment']['resume_path']}")
    logging.info(f"Looking back {config['ats_enrichment']['hours_lookback']} hours")
    logging.info(f"Using {'SCD2' if config['ats_enrichment']['use_scd2'] else 'merge_upsert'} pattern")

    if args.dry_run:
        logging.info("DRY RUN MODE: No changes will be made to Google Sheets")

    try:
        # Wrap the entire process in a try-except block to catch any unexpected errors
        try:
            # Get recent job entries
            job_entries = get_recent_job_entries(
                spreadsheet_id=args.spreadsheet_id,
                config=config
            )

            if job_entries.empty:
                logging.warning("No recent job entries found")
                return 0

            # Apply limit if specified
            original_count = len(job_entries)
            if args.limit and args.limit > 0 and args.limit < len(job_entries):
                job_entries = job_entries.head(args.limit)
                logging.info(f"Limiting processing to {args.limit} records out of {original_count} available records")

            # Enrich job entries with ATS match information
            enriched_df = enrich_job_entries(
                job_entries=job_entries,
                api_key=args.api_key,
                config=config
            )

            # Debug logging for the returned object
            logging.debug(f"Main function received object of type: {type(enriched_df)}")
            if isinstance(enriched_df, pd.DataFrame):
                logging.debug(f"DataFrame shape: {enriched_df.shape}")
            elif isinstance(enriched_df, dict):
                logging.debug(f"Dictionary with {len(enriched_df)} keys")

            # Write enriched data back to Google Sheets
            if not args.dry_run:
                success = write_enriched_data(
                    enriched_df=enriched_df,
                    spreadsheet_id=args.spreadsheet_id,
                    worksheet_name=config["ats_enrichment"]["worksheet_name"],
                    config=config
                )

                if not success:
                    logging.error("Failed to write enriched data")
                    return 1
            else:
                logging.info("DRY RUN: Would write enriched data to Google Sheets")

            # Calculate elapsed time
            end_time = datetime.now()
            elapsed_time = end_time - start_time
            elapsed_seconds = elapsed_time.total_seconds()

            # Print summary
            logging.info("\nATS Enrichment Summary:")
            logging.info(f"Total runtime: {elapsed_seconds:.2f} seconds")
            if args.limit and args.limit > 0 and original_count > len(enriched_df):
                logging.info(f"Entries processed: {len(enriched_df)} (limited from {original_count} available)")
            else:
                logging.info(f"Entries processed: {len(enriched_df)}")

            if args.dry_run:
                logging.info("DRY RUN COMPLETED: No changes were made to Google Sheets")
            else:
                logging.info("ATS enrichment process completed successfully")

            return 0
        except Exception as e:
            # Log the full exception with traceback
            logging.error(f"ATS enrichment process failed: {str(e)}")
            logging.debug("Exception details:", exc_info=True)

            # Return error exit code
            return 1

    except KeyboardInterrupt:
        logging.warning("\nATS enrichment process interrupted by user")
        return 130  # Standard exit code for SIGINT


if __name__ == "__main__":
    sys.exit(main())
