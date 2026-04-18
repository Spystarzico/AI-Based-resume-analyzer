import importlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from typing import Any, Dict


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
ENV_PATH = os.path.join(BACKEND_DIR, ".env")

dotenv_module = importlib.import_module("dotenv")
dotenv_module.load_dotenv(ENV_PATH)


def _clean_json_text(raw_text: str) -> str:
    cleaned = raw_text.strip().replace("```json", "").replace("```", "").strip()
    if cleaned.startswith("{") and cleaned.endswith("}"):
        return cleaned

    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        return match.group(0)
    return cleaned


def call_gemini(resume_text: str, job_description: str = "") -> Dict[str, Any]:
    """Call Gemini and return structured resume analysis JSON."""
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is missing in backend/.env")

    # Demo fallback toggle:
    # Uncomment the next line to force the local analyzer path during evaluation.
    # raise RuntimeError("Forced Gemini failure for local fallback testing")

    genai = importlib.import_module("google.generativeai")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-pro")

    has_jd = bool(job_description.strip())
    prompt = f"""
You are an expert ATS and hiring analyst.
Analyze the resume and return ONLY valid JSON. Do not include markdown, code fences, or commentary.

Use this exact schema:
{{
  "skills": ["skill1", "skill2"],
  "experience": "short experience summary",
  "summary": "short overall summary",
  "strengths": ["strength1", "strength2"],
  "suggestions": ["suggestion1", "suggestion2"]
}}

Rules:
- Only include keys from the schema above.
- Keep values concise and evidence-based.
- If the job description is missing, do not add job-matching fields.
- If the resume is weak or sparse, still return valid JSON.

Resume:
{resume_text}

Job Description:
{job_description if has_jd else "Not provided"}
""".strip()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(model.generate_content, prompt)
            response = future.result(timeout=8)

        raw_text = getattr(response, "text", None)
        if not raw_text:
            raise RuntimeError("Gemini returned an empty response")

        parsed = json.loads(_clean_json_text(raw_text))
        if not isinstance(parsed, dict):
            raise RuntimeError("Gemini response is not a valid JSON object")
        return parsed
    except TimeoutError as exc:
        raise RuntimeError("Gemini request timed out") from exc
    except Exception as exc:
        raise RuntimeError(f"Gemini analysis failed: {exc}") from exc
