from __future__ import annotations

from typing import Iterable


def score_skill_overlap(resume_skills: Iterable[str], job_skills: Iterable[str]) -> int:
    """Compute a simple 0-100 overlap score between resume and JD skill sets."""
    resume_set = {s.strip().lower() for s in resume_skills if s and s.strip()}
    job_set = {s.strip().lower() for s in job_skills if s and s.strip()}
    if not job_set:
        return 0
    overlap = len(resume_set.intersection(job_set))
    return round((overlap / len(job_set)) * 100)
