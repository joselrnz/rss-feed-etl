import os
import pandas as pd
import PyPDF2
import docx # Make sure you have python-docx installed
import logging

from src.utils.logging_utils import logger

def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file."""
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                # Add a check if extract_text returns None or is empty
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        logger.info(f"Extracted text from PDF: {pdf_path}")
    except FileNotFoundError:
        logger.error(f"PDF file not found: {pdf_path}")
        raise
    except Exception as e:
        logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
        raise
    return text


def extract_text_from_docx(docx_path):
    """Extract text content from a DOCX file."""
    text = ""
    try:
        # Explicitly import Document from python-docx
        from docx import Document
        doc = Document(docx_path)
        for para in doc.paragraphs:
            text += para.text + "\n"
        logger.info(f"Extracted text from DOCX: {docx_path}")
    except FileNotFoundError:
        logger.error(f"DOCX file not found: {docx_path}")
        raise
    except Exception as e:
        logger.error(f"Error extracting text from DOCX {docx_path}: {e}")
        raise
    return text

def read_resume(resume_path):
    """Read and extract text from a resume file (PDF or DOCX)."""
    if not os.path.exists(resume_path):
        logger.error(f"Resume file not found: {resume_path}")
        raise FileNotFoundError(f"Resume file not found: {resume_path}")

    if resume_path.lower().endswith('.pdf'):
        return extract_text_from_pdf(resume_path)
    elif resume_path.lower().endswith('.docx'):
        return extract_text_from_docx(resume_path)
    else:
        logger.error(f"Unsupported resume format for file: {resume_path}")
        raise ValueError("Unsupported resume format. Please provide a PDF or DOCX file.")

def read_job_descriptions_from_csv(csv_path: str, description_column: str = "description") -> pd.DataFrame:
    """Read job descriptions from a CSV file."""
    try:
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(csv_path)

        if description_column not in df.columns:
            available_columns = ", ".join(df.columns)
            logger.error(f"Column '{description_column}' not found in CSV file. Available columns: {available_columns}")
            raise ValueError(f"Column '{description_column}' not found in CSV file. Available columns: {available_columns}")

        # Remove rows with empty descriptions
        initial_count = len(df)
        df = df[df[description_column].notna() & (df[description_column].astype(str).str.strip() != "")]
        if len(df) < initial_count:
            logger.warning(f"Removed {initial_count - len(df)} rows with empty descriptions.")

        logger.info(f"Read {len(df)} job descriptions from {csv_path}.")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {e}")
        raise