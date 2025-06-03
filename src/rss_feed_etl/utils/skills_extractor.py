"""Skills extraction utilities using spaCy."""

import logging
import re
import json
from pathlib import Path
from typing import List, Dict, Set, Tuple, Optional, Union

try:
    import spacy
    from spacy.matcher import PhraseMatcher
    from spacy.tokens import Doc
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False


class SkillsExtractor:
    """Extract skills from text using spaCy."""
    
    def __init__(self, skills_file: Optional[str] = None, model: str = "en_core_web_sm"):
        """
        Initialize the skills extractor.
        
        Args:
            skills_file: Path to a JSON file containing a list of skills
            model: spaCy model to use
        """
        self.skills_file = skills_file
        self.model_name = model
        self.nlp = None
        self.matcher = None
        self.skills_list = []
        
        if not SPACY_AVAILABLE:
            logging.warning("spaCy is not installed. Skills extraction will not be available.")
            return
        
        try:
            # Load spaCy model
            self.nlp = spacy.load(model)
            logging.info(f"Loaded spaCy model: {model}")
            
            # Initialize matcher
            self.matcher = PhraseMatcher(self.nlp.vocab, attr="LOWER")
            
            # Load skills list
            if skills_file:
                self._load_skills(skills_file)
            else:
                # Use default skills list
                self._load_default_skills()
                
        except Exception as e:
            logging.error(f"Error initializing SkillsExtractor: {e}")
            self.nlp = None
            self.matcher = None
    
    def _load_skills(self, skills_file: str) -> None:
        """Load skills from a JSON file."""
        try:
            skills_path = Path(skills_file)
            if not skills_path.exists():
                logging.warning(f"Skills file not found: {skills_file}")
                self._load_default_skills()
                return
            
            with open(skills_path, 'r', encoding='utf-8') as f:
                skills_data = json.load(f)
            
            if isinstance(skills_data, list):
                self.skills_list = skills_data
            elif isinstance(skills_data, dict) and 'skills' in skills_data:
                self.skills_list = skills_data['skills']
            else:
                logging.warning(f"Invalid skills format in {skills_file}")
                self._load_default_skills()
                return
            
            # Create patterns
            patterns = [self.nlp.make_doc(skill.lower()) for skill in self.skills_list]
            self.matcher.add("SKILLS", patterns)
            logging.info(f"Loaded {len(self.skills_list)} skills from {skills_file}")
            
        except Exception as e:
            logging.error(f"Error loading skills: {e}")
            self._load_default_skills()
    
    def _load_default_skills(self) -> None:
        """Load a default list of common technical skills."""
        # Common technical skills
        self.skills_list = [
            "Python", "JavaScript", "Java", "C++", "C#", "Ruby", "PHP", "Swift", "Kotlin", "Go",
            "SQL", "MySQL", "PostgreSQL", "MongoDB", "Oracle", "SQLite", "Redis", "Cassandra",
            "HTML", "CSS", "React", "Angular", "Vue.js", "Node.js", "Express", "Django", "Flask",
            "Spring", "ASP.NET", "Ruby on Rails", "Laravel", "Symfony",
            "AWS", "Azure", "Google Cloud", "Heroku", "Docker", "Kubernetes", "Terraform",
            "Git", "GitHub", "GitLab", "Bitbucket", "CI/CD", "Jenkins", "Travis CI", "CircleCI",
            "Machine Learning", "Deep Learning", "TensorFlow", "PyTorch", "Scikit-learn", "NLP",
            "Data Analysis", "Data Science", "Big Data", "Hadoop", "Spark", "Tableau", "Power BI",
            "Agile", "Scrum", "Kanban", "Jira", "Confluence", "Trello", "Asana",
            "REST API", "GraphQL", "SOAP", "Microservices", "Serverless", "WebSockets",
            "Linux", "Unix", "Windows", "macOS", "Bash", "PowerShell", "Shell Scripting",
            "Networking", "TCP/IP", "HTTP", "HTTPS", "DNS", "Load Balancing", "Firewall",
            "Security", "Encryption", "Authentication", "Authorization", "OAuth", "JWT",
            "Testing", "Unit Testing", "Integration Testing", "E2E Testing", "TDD", "BDD",
            "Selenium", "Jest", "Mocha", "Chai", "Cypress", "JUnit", "PyTest",
            "Responsive Design", "Mobile Development", "iOS", "Android", "React Native", "Flutter",
            "UI/UX", "Figma", "Sketch", "Adobe XD", "Photoshop", "Illustrator",
            "Project Management", "Product Management", "Technical Writing", "Documentation"
        ]
        
        # Create patterns
        patterns = [self.nlp.make_doc(skill.lower()) for skill in self.skills_list]
        self.matcher.add("SKILLS", patterns)
        logging.info(f"Loaded {len(self.skills_list)} default skills")
    
    def extract_skills(self, text: str) -> List[str]:
        """
        Extract skills from text.
        
        Args:
            text: Text to extract skills from
            
        Returns:
            List of skills found in the text
        """
        if not SPACY_AVAILABLE or not self.nlp or not self.matcher:
            logging.warning("Skills extraction is not available")
            return []
        
        try:
            # Process text
            doc = self.nlp(text)
            
            # Find matches
            matches = self.matcher(doc)
            
            # Extract skills
            found_skills = set()
            for match_id, start, end in matches:
                span = doc[start:end]
                skill = span.text
                found_skills.add(skill)
            
            return list(found_skills)
            
        except Exception as e:
            logging.error(f"Error extracting skills: {e}")
            return []
    
    def analyze_match(self, resume_text: str, job_description: str) -> Dict:
        """
        Analyze the match between a resume and job description.
        
        Args:
            resume_text: Text content of the resume
            job_description: Text content of the job description
            
        Returns:
            Dictionary with match_percentage, missing_skills, and matched_skills
        """
        if not SPACY_AVAILABLE or not self.nlp or not self.matcher:
            logging.warning("Skills extraction is not available")
            return {
                "match_percentage": 0,
                "missing_skills": [],
                "matched_skills": []
            }
        
        try:
            # Extract skills from resume and job description
            resume_skills = set(self.extract_skills(resume_text))
            job_skills = set(self.extract_skills(job_description))
            
            # Calculate matched and missing skills
            matched_skills = resume_skills.intersection(job_skills)
            missing_skills = job_skills - resume_skills
            
            # Calculate match percentage
            if not job_skills:
                match_percentage = 0
            else:
                match_percentage = (len(matched_skills) / len(job_skills)) * 100
            
            return {
                "match_percentage": round(match_percentage, 2),
                "missing_skills": list(missing_skills),
                "matched_skills": list(matched_skills)
            }
            
        except Exception as e:
            logging.error(f"Error analyzing match: {e}")
            return {
                "match_percentage": 0,
                "missing_skills": [],
                "matched_skills": []
            }
