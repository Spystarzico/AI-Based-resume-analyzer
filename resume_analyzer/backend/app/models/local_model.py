import re
from typing import Any, Dict, List


ALL_SKILLS = [
    "python", "javascript", "typescript", "java", "c++", "c#", "go", "rust", "sql",
    "react", "vue", "angular", "node.js", "django", "flask", "fastapi", "spring",
    "aws", "gcp", "azure", "docker", "kubernetes", "terraform", "linux",
    "postgresql", "mongodb", "redis", "mysql", "elasticsearch", "kafka",
    "spark", "hadoop", "airflow", "pandas", "numpy", "scikit-learn", "tensorflow",
    "git", "jenkins", "ci/cd", "rest api", "graphql", "microservices",
    "machine learning", "deep learning", "nlp", "data engineering", "etl",
]


def _format_skill(skill: str) -> str:
    if skill in {"aws", "gcp", "sql", "nlp", "etl"}:
        return skill.upper()
    if skill == "ci/cd":
        return "CI/CD"
    return skill.capitalize()


def _extract_education(resume_text: str) -> List[str]:
    education: List[str] = []
    if re.search(r"b\.?tech|b\.?e\.|bachelor", resume_text, re.IGNORECASE):
        education.append("Bachelor's Degree")
    if re.search(r"m\.?tech|m\.?e\.|master", resume_text, re.IGNORECASE):
        education.append("Master's Degree")
    if re.search(r"ph\.?d", resume_text, re.IGNORECASE):
        education.append("PhD")

    if re.search(r"gpa|cgpa|percentage", resume_text, re.IGNORECASE):
        gpa = re.search(r"(?:gpa|cgpa)[:\s]*(\d+\.?\d*)", resume_text, re.IGNORECASE)
        if gpa:
            education.append(f"GPA: {gpa.group(1)}")

    if not education:
        education.append("Education details not found")
    return education


def _experience_level(years: int) -> str:
    if years >= 8:
        return "Principal/Staff"
    if years >= 5:
        return "Senior"
    if years >= 3:
        return "Mid-Level"
    if years >= 1:
        return "Junior"
    return "Entry Level"


def analyze_resume_local(resume_text: str, job_description: str = "") -> Dict[str, Any]:
    text = resume_text.lower()

    found_skills = [skill for skill in ALL_SKILLS if skill in text]

    exp_match = re.search(r"(\d+)\+?\s*years?", resume_text, re.IGNORECASE)
    years = int(exp_match.group(1)) if exp_match else 0
    level = _experience_level(years)

    education = _extract_education(resume_text)

    skill_score = min(len(found_skills) * 5, 35)
    exp_score = min(years * 4, 30)
    edu_score = 15 if education and education[0] != "Education details not found" else 6

    section_patterns = [
        re.compile(r"summary|objective", re.IGNORECASE),
        re.compile(r"experience", re.IGNORECASE),
        re.compile(r"education", re.IGNORECASE),
        re.compile(r"skills", re.IGNORECASE),
    ]
    format_score = sum(1 for pattern in section_patterns if pattern.search(resume_text)) * 5
    overall_score = min(skill_score + exp_score + edu_score + format_score + 10, 99)

    has_summary = bool(re.search(r"summary|objective|profile", resume_text, re.IGNORECASE))
    has_metrics = bool(re.search(r"\d+%|\d+x|\$\d+", resume_text, re.IGNORECASE))
    has_certs = bool(re.search(r"certif|aws|gcp|azure|pmp|scrum", resume_text, re.IGNORECASE))
    has_links = bool(re.search(r"github|linkedin|portfolio", resume_text, re.IGNORECASE))

    suggestions: List[str] = []
    if not has_summary:
        suggestions.append("Add a professional summary at the top of your resume")
    if not has_metrics:
        suggestions.append('Add quantifiable achievements (e.g. "improved performance by 40%")')
    if not has_links:
        suggestions.append("Include GitHub/LinkedIn profile links")
    if not has_certs:
        suggestions.append("Add relevant certifications (AWS, GCP, etc.)")
    if len(found_skills) < 5:
        suggestions.append("Expand your skills section with more technical keywords")
    if not suggestions:
        suggestions.append("Consider adding a projects section with links")
    suggestions.append("Tailor your resume keywords to match each job description")

    strengths: List[str] = []
    if len(found_skills) >= 8:
        strengths.append(f"Strong technical profile with {len(found_skills)} identified skills")
    if years >= 3:
        strengths.append(f"{years}+ years of professional experience")
    if has_metrics:
        strengths.append("Good use of quantifiable achievements and metrics")
    if has_certs:
        strengths.append("Has relevant industry certifications")
    if has_links:
        strengths.append("Includes professional online presence")
    if not strengths:
        strengths.append("Resume has clear structure and formatting")

    keywords = [_format_skill(skill) for skill in found_skills[:8]]

    top_skills = ", ".join(found_skills[:3]) if found_skills else "software development"
    experience_summary = (
        f"{level} professional with {f'{years}+ years of experience' if years > 0 else 'entry-level experience'} "
        f"and strengths in {top_skills}."
    )

    summary = (
        f"This resume shows a {level.lower()} profile with {len(found_skills)} technical skills identified. "
        f"{'It includes measurable achievements and strong impact signals.' if has_metrics else 'It would benefit from measurable achievements and clearer impact statements.'} "
        f"{'Relevant certifications strengthen the profile.' if has_certs else 'Adding certifications could improve credibility.'}"
    )

    return {
        "skills": [_format_skill(skill) for skill in found_skills],
        "experience": experience_summary,
        "summary": summary,
        "strengths": strengths,
        "suggestions": suggestions[:5],
    }
