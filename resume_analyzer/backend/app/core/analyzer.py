import logging
from typing import Any, Dict, List

from .gemini_client import call_gemini
from ..models.local_model import analyze_resume_local


logger = logging.getLogger(__name__)

EXPECTED_KEYS = {"skills", "experience", "summary", "strengths", "suggestions"}

# Common stop words to filter out
STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
    'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'be', 'been',
    'have', 'has', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'your', 'our', 'we', 'you', 'they', 'them', 'that', 'this', 'these',
    'about', 'more', 'other', 'some', 'any', 'all', 'each', 'no', 'not',
    'can', 'may', 'must', 'need', 'use', 'used', 'using', 'uses', 'include',
    'includes', 'experience', 'years', 'knowledge', 'proficiency', 'strong',
    'preferred', 'required', 'working', 'work', 'able', 'ability', 'skills'
}


def _is_valid_analysis(result: Dict[str, Any]) -> bool:
    return isinstance(result, dict) and EXPECTED_KEYS.issubset(result.keys())


def _extract_meaningful_words(text: str) -> set:
    """Extract meaningful words from text (length >= 3, not stop words)."""
    words = set()
    for word in text.lower().split():
        # Clean punctuation from word
        cleaned = ''.join(c for c in word if c.isalnum())
        if len(cleaned) >= 3 and cleaned not in STOP_WORDS:
            words.add(cleaned)
    return words


def _compute_match_score(resume_text: str, job_description: str) -> int:
    """Compute simple keyword overlap match score between resume and job description."""
    if not job_description or not resume_text:
        return 0
    
    resume_lower = resume_text.lower()
    jd_lower = job_description.lower()
    
    # Extract words from job description (skip common words)
    jd_words = set(word for word in jd_lower.split() if len(word) > 2 and word not in STOP_WORDS)
    matched = sum(1 for word in jd_words if word in resume_lower)
    
    # Calculate score as percentage of matched keywords (capped at 100)
    score = int((matched / len(jd_words) * 100)) if jd_words else 0
    return min(score, 100)


def _compute_skill_matches(resume_text: str, job_description: str) -> tuple:
    """
    Compute matched and missing skills between resume and job description.
    Returns (matched_skills, missing_skills) as lists of strings, limited to top 10 each.
    """
    if not job_description or not resume_text:
        return [], []
    
    resume_words = _extract_meaningful_words(resume_text)
    jd_words = _extract_meaningful_words(job_description)
    
    # Find matched and missing skills
    matched = list(resume_words & jd_words)  # Intersection
    missing = list(jd_words - resume_words)  # Words in JD but not in resume
    
    # Sort and limit to top 10 to keep UI clean
    matched = sorted(matched)[:10]
    missing = sorted(missing)[:10]
    
    return matched, missing


def analyze_resume_hybrid(resume_text: str, job_description: str = "") -> Dict[str, Any]:
    """Use Gemini first and fall back to local analyzer when Gemini fails."""
    try:
        gemini_result = call_gemini(resume_text=resume_text, job_description=job_description)
        if not _is_valid_analysis(gemini_result):
            raise ValueError("Gemini response did not match the expected schema")

        logger.info("Resume analysis completed using Gemini")
        
        # Add match score and skills if job description is provided
        if job_description.strip():
            match_score = _compute_match_score(resume_text, job_description)
            matched_skills, missing_skills = _compute_skill_matches(resume_text, job_description)
            
            print(f"COMPUTED match_score (Gemini): {match_score}")
            print(f"COMPUTED matched_skills: {matched_skills}")
            print(f"COMPUTED missing_skills: {missing_skills}")
            
            gemini_result["match_score"] = match_score
            gemini_result["matched_skills"] = matched_skills
            gemini_result["missing_skills"] = missing_skills
        
        return {
            "source": "gemini",
            "data": gemini_result,
        }
    except Exception as exc:
        logger.exception("Gemini failed, switching to local analyzer: %s", exc)
        local_result = analyze_resume_local(resume_text=resume_text, job_description=job_description)
        logger.info("Resume analysis completed using local fallback")
        
        # Add match score and skills if job description is provided
        if job_description.strip():
            match_score = _compute_match_score(resume_text, job_description)
            matched_skills, missing_skills = _compute_skill_matches(resume_text, job_description)
            
            print(f"COMPUTED match_score (Local): {match_score}")
            print(f"COMPUTED matched_skills: {matched_skills}")
            print(f"COMPUTED missing_skills: {missing_skills}")
            
            local_result["match_score"] = match_score
            local_result["matched_skills"] = matched_skills
            local_result["missing_skills"] = missing_skills
        
        return {
            "source": "local",
            "data": local_result,
        }
