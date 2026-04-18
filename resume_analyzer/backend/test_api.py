"""Simple API test script for the FastAPI Resume Analyzer.

How to run:
1. Start the backend server first:
   uvicorn app.main:app --reload --port 8000
2. Run this script from the backend folder:
   python test_api.py

Expected output:
- HTTP status code
- JSON response from /analyze
- The response should look like:
  {
    "source": "gemini" or "local",
    "data": {
      "skills": [...],
      "experience": "...",
      "summary": "...",
      "strengths": [...],
      "suggestions": [...]
    }
  }

Fallback test:
- Open backend/app/core/gemini_client.py
- Uncomment the line that raises:
  RuntimeError("Forced Gemini failure for local fallback testing")
- Run this script again
- The response source should change to "local"
"""

import json
from typing import Any, Dict

import requests

API_URL = "http://127.0.0.1:8000/analyze"

SAMPLE_PAYLOAD: Dict[str, Any] = {
    "resume_text": """
John Doe
Data Engineer
5+ years of experience in data engineering and analytics.

Skills: Python, SQL, Machine Learning, Spark, Kafka, Airflow, Docker, AWS

Experience:
- Built ETL pipelines using Python and SQL
- Improved job processing performance by 40%
- Worked with Spark and Kafka for batch and streaming data workflows

Education:
B.Tech in Computer Science, CGPA 8.7

Projects:
- Built a machine learning model for resume classification
- Developed dashboards and automation workflows

Certifications:
AWS Certified Cloud Practitioner
GitHub: github.com/johndoe
LinkedIn: linkedin.com/in/johndoe
""".strip(),
    "job_description": "Looking for a Data Engineer with Python, SQL, Machine Learning, Spark, Kafka, and AWS experience.",
}


def main() -> None:
    response = requests.post(API_URL, json=SAMPLE_PAYLOAD, timeout=60)
    print(f"Status Code: {response.status_code}")

    try:
        print("JSON Response:")
        print(json.dumps(response.json(), indent=2))
    except ValueError:
        print("Response was not valid JSON:")
        print(response.text)


if __name__ == "__main__":
    main()
