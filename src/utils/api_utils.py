import requests
import json
import time
import os
import logging

from src.utils.logging_utils import logger

# Default API URL for OpenRouter
DEFAULT_API_URL = "https://openrouter.ai/api/v1/chat/completions"

def call_openrouter_api(messages, api_key, model="openai/gpt-3.5-turbo", api_url=None, retries=3, retry_delay=5):
    """Make API call to OpenRouter with retry logic."""
    if api_key is None or api_key == "YOUR_OPENROUTER_API_KEY":
        logger.error("OpenRouter API key not provided.")
        return None

    url = api_url if api_url else DEFAULT_API_URL

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "model": model,
        "messages": messages
    }

    for attempt in range(retries):
        try:
            logger.debug(f"Attempt {attempt + 1} of {retries} calling API: {url}")
            response = requests.post(url, headers=headers, json=data, timeout=60) # Increased timeout

            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            if response.status_code == 200:
                logger.debug("API call successful.")
                return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(f"API request failed (attempt {attempt + 1}/{retries}): {e}")
            if response is not None and response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', retry_delay))
                logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds before retry...")
                time.sleep(retry_after)
            elif attempt < retries - 1:
                logger.warning(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
            else:
                logger.error(f"API request failed after {retries} attempts.")

    logger.error(f"Failed after {retries} attempts")
    return None