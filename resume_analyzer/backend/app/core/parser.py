from __future__ import annotations

import re
from typing import Dict, List


def extract_sections(text: str) -> Dict[str, List[str]]:
    """Extract coarse resume sections used by analysis endpoints."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    sections = {
        "experience": [],
        "education": [],
        "skills": [],
    }

    current = "experience"
    for line in lines:
        upper = line.upper()
        if "EDUCATION" in upper:
            current = "education"
            continue
        if "SKILL" in upper:
            current = "skills"
            continue
        sections[current].append(line)

    return sections


def find_keywords(text: str) -> List[str]:
    """Extract alphanumeric keywords as a simple parser baseline."""
    tokens = re.findall(r"[A-Za-z0-9+#.]{3,}", text)
    return sorted(set(token.lower() for token in tokens))
