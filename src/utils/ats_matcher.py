import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import json
import re # Import regex for parsing AI response
from typing import List, Dict, Any

import logging
from src.utils.logging_utils import logger
from src.utils.api_utils import call_openrouter_api # Import the API utility
from src.utils.file_utils import read_resume, read_job_descriptions_from_csv # Import file utilities

# Configuration defaults (can be overridden by config file/args)
DEFAULT_MAX_DESCRIPTIONS_PER_BATCH = 10
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 5
DEFAULT_AI_MODEL = "openai/gpt-3.5-turbo"
DEFAULT_JOB_DESC_COLUMN = "summary"

def calculate_similarity_with_tfidf(resume_text: str, job_description: str) -> float:
    """Calculate similarity between resume and job description using TF-IDF and cosine similarity."""
    if not resume_text or not job_description:
        logger.warning("Empty text for TF-IDF similarity calculation.")
        return 0.0

    # Use try-except for robustness
    try:
        vectorizer = TfidfVectorizer(stop_words='english')
        # Handle case where text might be too short after stop word removal
        if not resume_text.strip() or not job_description.strip():
             return 0.0
        tfidf_matrix = vectorizer.fit_transform([resume_text, job_description])
        cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
        return round(cosine_sim[0][0] * 100, 2)  # Convert to percentage and round
    except Exception as e:
        logger.error(f"Error calculating TF-IDF similarity: {e}")
        return 0.0

# --- AI Matching Functions with Skill Analysis ---

def calculate_similarity_and_skills_with_ai(resume_text: str, job_description: str, api_key: str, model: str) -> tuple[float, List[str], List[str]]:
    """
    Calculate similarity and identify missing/matched skills using OpenRouter AI API.

    Args:
        resume_text: The text content of the resume.
        job_description: The text content of the job description.
        api_key: The OpenRouter API key.
        model: The AI model to use (e.g., "openai/gpt-3.5-turbo").

    Returns:
        A tuple: (match_percentage, missing_skills_list, matched_skills_list)
        Returns (0.0, [], []) on failure or inability to parse.
    """
    if not resume_text or not job_description:
         logger.warning("Empty text provided for AI similarity calculation.")
         return 0.0, [], []

    # Craft the prompt to ask for percentage, missing skills, and matched skills in a parseable format (JSON)
    messages = [
        {"role": "system", "content": """You are an ATS (Applicant Tracking System) expert. Analyze the provided resume against the job description.
        Identify the percentage match based on skills, experience, and qualifications.
        Also, identify key skills or requirements mentioned in the job description that appear to be missing or less prominent in the resume. Focus on 5-10 key missing skills if possible.
        Finally, identify key skills from the job description that *do* appear to be well-matched or present in the resume. Focus on 5-10 key matched skills if possible.
        Return the results in a JSON object with the following keys:
        'match_percentage': (integer or float between 0 and 100)
        'missing_skills': (list of strings, skills from job description not strongly in resume)
        'matched_skills': (list of strings, skills from job description found in resume)
        Ensure the response is ONLY the JSON object, nothing else before or after."""},
        {"role": "user", "content": f"Resume:\n{resume_text}\n\nJob Description:\n{job_description}\n\nProvide the ATS match analysis as a JSON object."}
    ]

    result = call_openrouter_api(messages, api_key, model)

    if result:
        content = result['choices'][0]['message']['content'].strip()
        logger.debug(f"AI Raw Response: {content}")

        # Attempt to parse the JSON response
        try:
            # Use regex to find the JSON object in case there's surrounding text
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)

                percentage = float(data.get('match_percentage', 0))
                missing_skills = data.get('missing_skills', [])
                matched_skills = data.get('matched_skills', [])

                # Ensure skills are lists of strings and clean them
                missing_skills = [str(s).strip() for s in missing_skills if isinstance(s, (str, int, float)) and str(s).strip()]
                matched_skills = [str(s).strip() for s in matched_skills if isinstance(s, (str, int, float)) and str(s).strip()]

                logger.debug(f"Parsed AI Response: Percentage={percentage}, Missing={missing_skills}, Matched={matched_skills}")
                return round(percentage, 2), missing_skills, matched_skills

            else:
                logger.warning(f"AI response did not contain a valid JSON object: {content}")
                # Fallback: try to extract percentage if JSON parsing fails
                numbers = ''.join(filter(lambda x: x.isdigit() or x == '.', content))
                try:
                    fallback_percentage = float(numbers)
                    logger.warning(f"Falling back to extracting percentage only: {fallback_percentage}%")
                    return round(fallback_percentage, 2), [], []
                except ValueError:
                    logger.error(f"Could not extract percentage or parse JSON from API response: {content}")
                    return 0.0, [], []

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from AI response: {e}\nContent: {content}")
            # Fallback: try to extract percentage if JSON parsing fails
            numbers = ''.join(filter(lambda x: x.isdigit() or x == '.', content))
            try:
                fallback_percentage = float(numbers)
                logger.warning(f"Falling back to extracting percentage only: {fallback_percentage}%")
                return round(fallback_percentage, 2), [], []
            except ValueError:
                 logger.error(f"Could not extract percentage or parse JSON from API response: {content}")
                 return 0.0, [], []

        except Exception as e:
            logger.error(f"An unexpected error occurred while processing AI response: {e}\nContent: {content}")
            return 0.0, [], []


    return 0.0, [], [] # Return 0% and empty lists if API call fails


def batch_calculate_similarity_and_skills_with_ai(resume_text: str, job_descriptions: List[str], api_key: str, model: str, batch_size: int = DEFAULT_MAX_DESCRIPTIONS_PER_BATCH) -> Dict[int, Dict[str, Any]]:
    """
    Calculate similarity and identify missing/matched skills for multiple job descriptions in batches using OpenRouter AI API.

    Args:
        resume_text: The text content of the resume.
        job_descriptions: A list of job description text strings.
        api_key: The OpenRouter API key.
        model: The AI model to use.
        batch_size: The maximum number of descriptions per API call batch.

    Returns:
        A dictionary where keys are original indices from the input list and values are dictionaries
        containing 'match_percentage', 'missing_skills', and 'matched_skills'.
        Returns an empty dictionary on failure.
    """
    all_batch_results_dict = {}

    if not job_descriptions:
        logger.info("No job descriptions provided for batch AI processing.")
        return {}

    # Process job descriptions in batches
    for batch_start in range(0, len(job_descriptions), batch_size):
        batch_end = min(batch_start + batch_size, len(job_descriptions))
        batch_descs = job_descriptions[batch_start:batch_end]

        logger.info(f"Processing batch {batch_start//batch_size + 1}: jobs {batch_start+1}-{batch_end}")

        # Prepare job descriptions for batch processing with clear separators and numbering
        formatted_jobs = []
        for i, desc in enumerate(batch_descs):
             # Add truncation warning if needed, but let the API handle token limits primarily
             # Truncate very long descriptions might be necessary for smaller models or tight budgets
             # if len(desc) > 2000: # Example truncation length
             #     desc = desc[:2000] + "... [truncated]"
             formatted_jobs.append(f"--- JOB #{batch_start+i+1} ---\n{desc.strip()}")

        all_jobs_text = "\n\n".join(formatted_jobs)

        # Craft the batch prompt
        messages = [
            {"role": "system", "content": """You are an ATS (Applicant Tracking System) expert. Analyze the provided resume against multiple job descriptions.
            For EACH job description provided, calculate the percentage match based on skills, experience, and qualifications.
            Also, for EACH job, identify key skills or requirements mentioned in that specific job description that appear to be missing or less prominent in the resume. Focus on 5-10 key missing skills per job.
            Finally, for EACH job, identify key skills from that specific job description that *do* appear to be well-matched or present in the resume. Focus on 5-10 key matched skills per job.
            Return the results in a single JSON object. The keys of the JSON object should be the job numbers (e.g., "1", "2", "3") corresponding to the "--- JOB #X ---" markers.
            The value for each job number key should be an object with the following keys:
            'match_percentage': (integer or float between 0 and 100)
            'missing_skills': (list of strings, skills from job description not strongly in resume)
            'matched_skills': (list of strings, skills from job description found in resume)
            Ensure the response is ONLY the JSON object, nothing else before or after. Example structure:
            {
              "1": {"match_percentage": 85.0, "missing_skills": ["SQL"], "matched_skills": ["Python", "API"]},
              "2": {"match_percentage": 72.5, "missing_skills": ["Java"], "matched_skills": ["Cloud"]}
            }
            Handle cases where skills lists might be empty by providing an empty list [].
            If a job cannot be processed, omit its job number key from the JSON."""},
            {"role": "user", "content": f"Resume:\n{resume_text}\n\nJob Descriptions:\n{all_jobs_text}\n\nProvide the ATS match analysis for each job as a single JSON object."}
        ]

        result = call_openrouter_api(messages, api_key, model)

        if result:
            content = result['choices'][0]['message']['content'].strip()
            logger.debug(f"AI Batch Raw Response: {content}")

            # Attempt to parse the JSON response - expect an object now, not a list
            try:
                # Use regex to find the JSON object {}
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    data = json.loads(json_str)

                    # Iterate through the keys (job numbers as strings) in the parsed JSON
                    for job_num_str, job_data in data.items():
                        try:
                            job_num = int(job_num_str)
                            # Convert job number back to the original index in the full list
                            original_idx = (batch_start) + (job_num - 1)

                            # Check if original_idx is within the bounds of the original job_descriptions list
                            if 0 <= original_idx < len(job_descriptions):
                                percentage = float(job_data.get('match_percentage', 0))
                                missing_skills = job_data.get('missing_skills', [])
                                matched_skills = job_data.get('matched_skills', [])

                                # Ensure skills are lists of strings and clean them
                                missing_skills = [str(s).strip() for s in missing_skills if isinstance(s, (str, int, float)) and str(s).strip()]
                                matched_skills = [str(s).strip() for s in matched_skills if isinstance(s, (str, int, float)) and str(s).strip()]


                                all_batch_results_dict[original_idx] = {
                                    'match_percentage': round(percentage, 2),
                                    'missing_skills': missing_skills,
                                    'matched_skills': matched_skills
                                }
                                logger.debug(f"Parsed batch result for job {job_num}: {percentage}%")
                            else:
                                logger.warning(f"AI returned job number {job_num} which is outside the expected range for this batch (indices {batch_start} to {batch_end-1}). Skipping.")


                        except ValueError as e:
                             logger.warning(f"Could not parse job number '{job_num_str}' or data structure in batch response: {e}. Data: {job_data}")
                        except Exception as e:
                            logger.error(f"Error processing data for job number '{job_num_str}' in batch: {e}. Data: {job_data}")


                else:
                    logger.warning(f"AI batch response did not contain a valid JSON object: {content}")
                    # No fallback for batch if JSON fails, too complex to parse text reliably

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse batch JSON from AI response: {e}\nContent: {content}")
            except Exception as e:
                 logger.error(f"An unexpected error occurred while processing AI batch response: {e}\nContent: {content}")


        else:
            logger.error(f"API call failed for batch starting at index {batch_start}. Skipping batch.")


    return all_batch_results_dict