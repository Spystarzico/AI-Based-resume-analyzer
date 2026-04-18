# AI Resume Analyzer

A beginner-friendly full-stack project that analyzes resumes using AI.

It uses Gemini as the primary model and automatically falls back to a local rule-based analyzer if Gemini is unavailable, times out, or returns invalid output.

## What This Project Does

- Accepts `resume_text` and optional `job_description`
- Extracts skills, experience, strengths, and improvement suggestions
- Computes job match insights:
  - `match_score`
  - `matched_skills`
  - `missing_skills`
- Returns a structured JSON response for the React dashboard

## Key Features

- Hybrid AI flow:
  - Primary: Gemini API
  - Fallback: local Python analyzer
- FastAPI backend with clear JSON responses
- React + Vite + TypeScript frontend dashboard
- Job matching against a target role description
- Defensive backend behavior (fallback on failure)

## Tech Stack

- Frontend: React, Vite, TypeScript, Tailwind
- Backend: FastAPI, Python, Uvicorn
- AI: Gemini API (`google-generativeai`)
- Fallback: Rule-based Python model
- Data/infra in repo: Airflow DAGs, SQLite, Docker Compose

## Architecture Overview

```text
User Input (Resume + Optional Job Description)
                    |
                    v
             React Frontend
                    |
                    v
          FastAPI POST /analyze
                    |
        +-----------+-----------+
        |                       |
        v                       v
  Gemini API (primary)   Local fallback model
        |                       |
        +-----------+-----------+
                    |
                    v
        Structured JSON response (data)
                    |
                    v
          Resume Analyzer Dashboard
```

## Project Structure

```text
resume_analyzer/
├── backend/
│   ├── app/
│   │   ├── core/
│   │   │   ├── analyzer.py
│   │   │   └── gemini_client.py
│   │   ├── models/
│   │   │   └── local_model.py
│   │   └── main.py
│   ├── requirements.txt
│   └── test_api.py
├── frontend/
├── airflow/
├── docker-compose.yml
└── README.md
```

## Setup Instructions

### 1. Clone and Open Project

```bash
git clone <your-repo-url>
cd resume_analyzer
```

### 2. Backend Setup (FastAPI)

```bash
cd backend
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Create `backend/.env`:

```env
GEMINI_API_KEY=your_gemini_api_key_here
```

Run backend:

```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

API docs:

```text
http://127.0.0.1:8000/docs
```

### 3. Frontend Setup (React + Vite)

In a new terminal:

```bash
cd frontend
npm install
npm run dev
```

Vite app usually runs at:

```text
http://127.0.0.1:5173
```

Important: Make sure the frontend API URL matches the backend port you run (for example, `8000` or `8001`).

## API Endpoint

### POST `/analyze`

Analyzes a resume with optional job-description matching.

Request body:

```json
{
  "resume_text": "string",
  "job_description": "string (optional)"
}
```

## Example Input

```json
{
  "resume_text": "Python developer with 4 years of experience in FastAPI, SQL, Docker, and AWS.",
  "job_description": "Looking for a backend engineer with Python, FastAPI, PostgreSQL, Docker, and Kubernetes."
}
```

## Example Output

```json
{
  "source": "gemini",
  "data": {
    "skills": ["Python", "FastAPI", "SQL", "Docker", "AWS"],
    "experience": {
      "years": 4,
      "level": "Mid",
      "details": ["4 years of backend development"]
    },
    "summary": "Strong backend profile with practical API and cloud experience.",
    "strengths": [
      "Good Python and API fundamentals",
      "Hands-on DevOps tooling exposure"
    ],
    "suggestions": [
      "Add measurable project outcomes",
      "Highlight system design impact"
    ],
    "match_score": 72,
    "matched_skills": ["docker", "fastapi", "python"],
    "missing_skills": ["kubernetes", "postgresql"]
  }
}
```
- If Gemini fails, the backend still returns a valid response using the local fallback model.

## License

For academic, learning, and portfolio use.
