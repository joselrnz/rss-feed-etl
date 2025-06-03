"""File handling utilities."""

import os
import PyPDF2
from docx import Document


def extract_text_from_pdf(pdf_path):
    """Extract text content from a PDF file."""
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                text += page.extract_text() + "\n"
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
    return text


def extract_text_from_docx(docx_path):
    """Extract text content from a DOCX file."""
    text = ""
    try:
        doc = Document(docx_path)
        for para in doc.paragraphs:
            text += para.text + "\n"
    except Exception as e:
        print(f"Error extracting text from DOCX: {e}")
    return text


def read_resume(resume_path):
    """Read and extract text from a resume file (PDF or DOCX)."""
    if not os.path.exists(resume_path):
        raise FileNotFoundError(f"Resume file not found: {resume_path}")
       
    if resume_path.lower().endswith('.pdf'):
        return extract_text_from_pdf(resume_path)
    elif resume_path.lower().endswith('.docx'):
        return extract_text_from_docx(resume_path)
    else:
        raise ValueError("Unsupported resume format. Please provide a PDF or DOCX file.")
