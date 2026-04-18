"""
FastAPI Backend for AI Resume Analyzer
Connects the React frontend to all 30 Data Engineering Python task scripts.

Run with:
    uvicorn app.main:app --reload --port 8000
"""

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from pydantic import BaseModel
from typing import Optional, List
import importlib.util
import sys
import os
import io
import logging
import traceback
import time
from collections import defaultdict, deque
from threading import Lock
from pathlib import Path
from datetime import datetime
import subprocess
import tempfile
from dotenv import load_dotenv
from .core.analyzer import analyze_resume_hybrid
from .db.database import DB_PATH, get_db_connection

BACKEND_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BACKEND_DIR / ".env")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="AI Resume Analyzer - Data Engineering Backend",
    description="FastAPI backend connecting React UI to all 30 Data Engineering task scripts",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Paths ─────────────────────────────────────────────────────────────────────
PYTHON_TASKS_DIR = Path(__file__).parent / "core" / "python_tasks"
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 5
_request_history = defaultdict(deque)
_rate_limit_lock = Lock()

# ── Database Setup (Task 6 & 7 - SQL) ────────────────────────────────────────
def init_database():
    """Initialize SQLite database for storing resume analysis results (Tasks 6-9)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fact table (Task 9 - Data Warehousing / Star Schema)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_resume_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id TEXT UNIQUE NOT NULL,
            overall_score INTEGER,
            job_match_score INTEGER,
            experience_years INTEGER,
            experience_level TEXT,
            skills_count INTEGER,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Dimension: Skills (Task 9 - Dimension Table)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_skills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id TEXT,
            skill_name TEXT,
            skill_category TEXT,
            is_matched INTEGER DEFAULT 0
        )
    """)

    # Dimension: Suggestions (Task 9)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id TEXT,
            suggestion TEXT,
            suggestion_type TEXT DEFAULT 'improvement'
        )
    """)

    # Task execution log (Task 1 - Logging, Task 23 - Airflow monitoring)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS task_execution_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_number INTEGER,
            task_name TEXT,
            status TEXT,
            execution_time_ms INTEGER,
            output_summary TEXT,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


def _get_client_key(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_rate_limit(request: Request) -> None:
    client_key = _get_client_key(request)
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS

    with _rate_limit_lock:
        request_times = _request_history[client_key]
        while request_times and request_times[0] < window_start:
            request_times.popleft()

        if len(request_times) >= RATE_LIMIT_MAX_REQUESTS:
            logger.warning("Rate limit triggered for client=%s", client_key)
            raise HTTPException(
                status_code=429,
                detail="Rate limit exceeded. Please try again in a minute.",
            )

        request_times.append(now)
        logger.info("Incoming /analyze request from client=%s", client_key)



init_database()


# ── Pydantic Models ───────────────────────────────────────────────────────────
class ResumeAnalysisRequest(BaseModel):
    resume_text: str
    job_description: Optional[str] = ""
    analysis_id: Optional[str] = None


class TaskRunRequest(BaseModel):
    task_number: int
    parameters: Optional[dict] = {}


class AnalysisResult(BaseModel):
    analysis_id: str
    overall_score: int
    skills: List[str]
    experience: dict
    education: List[str]
    keywords: List[str]
    suggestions: List[str]
    strengths: List[str]
    job_match: Optional[dict] = None
    ai_summary: str
    pipeline_steps: List[dict]  # Shows which DE tasks ran


# ── Helper: Load and run a task module ───────────────────────────────────────
def load_task_module(task_number: int):
    """Dynamically load a Python task file (Tasks 1-30)."""
    task_files = {
        1: "task01_linux_filesystem.py",
        2: "task02_networking.py",
        3: "task03_python_basics.py",
        4: "task04_advanced_python.py",
        5: "task05_pandas_numpy.py",
        6: "task06_sql_basics.py",
        7: "task07_advanced_sql.py",
        8: "task08_database_concepts.py",
        9: "task09_data_warehousing.py",
        10: "task10_etl_vs_elt.py",
        11: "task11_data_ingestion.py",
        12: "task12_hadoop.py",
        13: "task13_hdfs_architecture.py",
        14: "task14_spark_basics.py",
        15: "task15_spark_dataframes.py",
        16: "task16_spark_sql.py",
        17: "task17_pyspark_advanced.py",
        18: "task18_streaming_concepts.py",
        19: "task19_kafka_basics.py",
        20: "task20_kafka_advanced.py",
        21: "task21_structured_streaming.py",
        22: "task22_airflow_basics.py",
        23: "task23_airflow_advanced.py",
        24: "task24_cloud_basics.py",
        25: "task25_cloud_storage.py",
        26: "task26_cloud_compute.py",
        27: "task27_data_warehouse_cloud.py",
        28: "task28_lakehouse.py",
        29: "task29_data_quality.py",
        30: "task30_final_project.py",
    }

    filename = task_files.get(task_number)
    if not filename:
        raise ValueError(f"Task {task_number} not found")

    filepath = PYTHON_TASKS_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Task file not found: {filepath}")

    spec = importlib.util.spec_from_file_location(f"task{task_number:02d}", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def log_task_execution(task_number: int, task_name: str, status: str, exec_ms: int, summary: str):
    """Log task execution to DB (Task 1 - Logging, Task 6 - SQL)."""
    try:
        conn = get_db_connection()
        conn.execute(
            "INSERT INTO task_execution_log (task_number, task_name, status, execution_time_ms, output_summary) VALUES (?,?,?,?,?)",
            (task_number, task_name, status, exec_ms, summary)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed to log task execution: %s", e)


def store_analysis_result(analysis_id: str, result: dict):
    """Store analysis result in star schema DB (Tasks 6, 7, 9)."""
    try:
        conn = get_db_connection()
        # Fact table
        conn.execute("""
            INSERT OR REPLACE INTO fact_resume_analysis
            (analysis_id, overall_score, job_match_score, experience_years, experience_level, skills_count)
            VALUES (?,?,?,?,?,?)
        """, (
            analysis_id,
            result.get("overallScore", 0),
            result.get("jobMatch", {}).get("score", 0) if result.get("jobMatch") else 0,
            result.get("experience", {}).get("years", 0),
            result.get("experience", {}).get("level", "Unknown"),
            len(result.get("skills", []))
        ))

        # Skills dimension
        for skill in result.get("skills", []):
            conn.execute(
                "INSERT INTO dim_skills (analysis_id, skill_name, skill_category) VALUES (?,?,?)",
                (analysis_id, skill, "technical")
            )

        # Matched skills
        if result.get("jobMatch"):
            for skill in result["jobMatch"].get("matchedSkills", []):
                conn.execute(
                    "INSERT INTO dim_skills (analysis_id, skill_name, skill_category, is_matched) VALUES (?,?,?,1)",
                    (analysis_id, skill, "matched")
                )

        # Suggestions dimension
        for suggestion in result.get("suggestions", []):
            conn.execute(
                "INSERT INTO dim_suggestions (analysis_id, suggestion) VALUES (?,?)",
                (analysis_id, suggestion)
            )

        conn.commit()
        conn.close()
        logger.info("Stored analysis result for %s", analysis_id)
    except Exception as e:
        logger.error("Failed to store analysis: %s", e)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "AI Resume Analyzer Backend",
        "tasks_available": 30,
        "python_tasks_path": str(PYTHON_TASKS_DIR),
        "database": DB_PATH,
        "docs": "/docs"
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    task_count = len(list(PYTHON_TASKS_DIR.glob("task*.py"))) if PYTHON_TASKS_DIR.exists() else 0
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "tasks_found": task_count,
        "database_exists": os.path.exists(DB_PATH)
    }


# ── Task 3, 4: Python pipeline run ───────────────────────────────────────────
@app.post("/api/tasks/{task_number}/run")
def run_task(task_number: int, request: TaskRunRequest):
    """
    Run any of the 30 Python task scripts dynamically.
    Demonstrates Tasks 2 (Networking/API), 3 (Python), 4 (Advanced Python).
    """
    if task_number < 1 or task_number > 30:
        raise HTTPException(status_code=400, detail="Task number must be 1-30")

    start = datetime.now()
    try:
        module = load_task_module(task_number)
        # Try to call a standard entry point if it exists
        result_data = {}
        if hasattr(module, 'main'):
            captured = io.StringIO()
            sys.stdout = captured
            module.main()
            sys.stdout = sys.__stdout__
            result_data["output"] = captured.getvalue()[:2000]  # Cap output
        elif hasattr(module, 'run'):
            result_data["output"] = str(module.run(**request.parameters))
        else:
            # List available functions
            funcs = [f for f in dir(module) if not f.startswith('_') and callable(getattr(module, f))]
            result_data["available_functions"] = funcs
            result_data["output"] = f"Task {task_number} loaded. Call specific functions: {funcs[:5]}"

        exec_ms = int((datetime.now() - start).total_seconds() * 1000)
        log_task_execution(task_number, f"task{task_number:02d}", "success", exec_ms, str(result_data)[:200])

        return {
            "task_number": task_number,
            "status": "success",
            "execution_time_ms": exec_ms,
            "result": result_data
        }

    except Exception as e:
        sys.stdout = sys.__stdout__
        exec_ms = int((datetime.now() - start).total_seconds() * 1000)
        log_task_execution(task_number, f"task{task_number:02d}", "error", exec_ms, str(e)[:200])
        raise HTTPException(status_code=500, detail=f"Task {task_number} error: {str(e)}")


# ── Task 11: Data Ingestion ───────────────────────────────────────────────────
@app.post("/api/ingest/resume")
async def ingest_resume(file: UploadFile = File(...)):
    """
    Batch ingestion endpoint for resume files (Task 11 - Data Ingestion).
    Accepts PDF/DOCX/TXT and extracts text.
    """
    content = await file.read()
    filename = file.filename or "unknown"

    try:
        # TXT files: direct decode
        if filename.endswith('.txt'):
            text = content.decode('utf-8', errors='ignore')
        # For PDF/DOCX: try to extract if libraries available, else return raw
        elif filename.endswith('.pdf'):
            try:
                import pdfplumber
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            except ImportError:
                text = content.decode('utf-8', errors='ignore')
        elif filename.endswith('.docx'):
            try:
                from docx import Document
                doc = Document(io.BytesIO(content))
                text = "\n".join(p.text for p in doc.paragraphs)
            except ImportError:
                text = content.decode('utf-8', errors='ignore')
        else:
            text = content.decode('utf-8', errors='ignore')

        log_task_execution(11, "data_ingestion", "success", 0, f"Ingested {filename}, {len(text)} chars")
        return {
            "filename": filename,
            "size_bytes": len(content),
            "extracted_text": text,
            "char_count": len(text),
            "task": "Task 11 - Data Ingestion"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


# ── Task 6, 7: SQL Queries ────────────────────────────────────────────────────
@app.get("/api/analytics/resumes")
def get_resume_analytics():
    """
    SQL analytics on stored resumes (Tasks 6 & 7 - Basic + Advanced SQL).
    Uses window functions, GROUP BY, ORDER BY.
    """
    conn = get_db_connection()

    # Task 6: Basic SQL
    basic_query = conn.execute("""
        SELECT experience_level, COUNT(*) as count, AVG(overall_score) as avg_score
        FROM fact_resume_analysis
        GROUP BY experience_level
        ORDER BY avg_score DESC
    """).fetchall()

    # Task 7: Advanced SQL with window functions
    advanced_query = conn.execute("""
        SELECT
            analysis_id,
            overall_score,
            experience_level,
            RANK() OVER (ORDER BY overall_score DESC) as score_rank,
            AVG(overall_score) OVER (PARTITION BY experience_level) as level_avg
        FROM fact_resume_analysis
        ORDER BY overall_score DESC
        LIMIT 20
    """).fetchall()

    # Top skills
    skills_query = conn.execute("""
        SELECT skill_name, COUNT(*) as frequency
        FROM dim_skills
        GROUP BY skill_name
        ORDER BY frequency DESC
        LIMIT 15
    """).fetchall()

    conn.close()

    return {
        "by_experience_level": [dict(r) for r in basic_query],
        "ranked_resumes": [dict(r) for r in advanced_query],
        "top_skills": [dict(r) for r in skills_query],
        "sql_tasks_demonstrated": ["Task 6 - GROUP BY, ORDER BY", "Task 7 - Window Functions RANK(), AVG() OVER"]
    }


# ── Task 9: Data Warehousing ──────────────────────────────────────────────────
@app.get("/api/warehouse/star-schema")
def get_star_schema_info():
    """
    Returns the star schema design (Task 9 - Data Warehousing).
    """
    return {
        "schema_type": "Star Schema",
        "fact_table": {
            "name": "fact_resume_analysis",
            "columns": ["analysis_id", "overall_score", "job_match_score", "experience_years", "experience_level", "skills_count", "analyzed_at"],
            "description": "Central fact table storing all resume analysis metrics"
        },
        "dimension_tables": [
            {"name": "dim_skills", "columns": ["skill_name", "skill_category", "is_matched"], "description": "Skills extracted per resume"},
            {"name": "dim_suggestions", "columns": ["suggestion", "suggestion_type"], "description": "Improvement suggestions per resume"},
        ],
        "task": "Task 9 - Data Warehousing Star Schema"
    }


# ── Task 10: ETL Pipeline ─────────────────────────────────────────────────────
@app.post("/api/pipeline/etl")
def run_etl_pipeline(request: ResumeAnalysisRequest):
    """
    ETL Pipeline for resume processing (Task 10 - ETL vs ELT).
    Extract → Transform → Load into SQLite.
    """
    analysis_id = request.analysis_id or f"resume_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    pipeline_log = []

    # EXTRACT
    start = datetime.now()
    raw_text = request.resume_text
    pipeline_log.append({
        "step": "EXTRACT",
        "task": "Task 10 - ETL",
        "status": "success",
        "detail": f"Extracted {len(raw_text)} characters from resume text"
    })

    # TRANSFORM (Task 3, 4 - Python processing)
    transformed = {
        "analysis_id": analysis_id,
        "word_count": len(raw_text.split()),
        "line_count": len(raw_text.splitlines()),
        "char_count": len(raw_text),
        "has_email": "@" in raw_text,
        "has_phone": any(c.isdigit() for c in raw_text),
        "sections_found": [s for s in ["EXPERIENCE", "EDUCATION", "SKILLS", "SUMMARY", "CERTIFICATIONS"] if s in raw_text.upper()],
    }
    pipeline_log.append({
        "step": "TRANSFORM",
        "task": "Task 4 - Advanced Python",
        "status": "success",
        "detail": f"Transformed: {transformed['word_count']} words, {len(transformed['sections_found'])} sections found"
    })

    # LOAD (Task 6 - SQL)
    conn = get_db_connection()
    conn.execute("""
        INSERT OR REPLACE INTO fact_resume_analysis
        (analysis_id, overall_score, experience_years, experience_level, skills_count)
        VALUES (?,0,0,'Unknown',0)
    """, (analysis_id,))
    conn.commit()
    conn.close()

    pipeline_log.append({
        "step": "LOAD",
        "task": "Task 6 - SQL",
        "status": "success",
        "detail": f"Loaded analysis {analysis_id} into SQLite"
    })

    exec_ms = int((datetime.now() - start).total_seconds() * 1000)
    log_task_execution(10, "etl_pipeline", "success", exec_ms, f"ETL complete for {analysis_id}")

    return {
        "analysis_id": analysis_id,
        "pipeline": "ETL",
        "execution_ms": exec_ms,
        "steps": pipeline_log,
        "transformed_data": transformed
    }


# ── Tasks 12-17: Big Data Simulation ─────────────────────────────────────────
@app.get("/api/bigdata/status")
def bigdata_status():
    """
    Shows status of Big Data simulation layer (Tasks 12-17).
    In production: connects to real HDFS/Spark via Docker.
    """
    return {
        "hadoop_hdfs": {
            "status": "simulated",
            "task": "Tasks 12-13",
            "description": "HDFS simulation via Python classes (real HDFS available via Docker)",
            "docker_command": "docker-compose up hadoop",
            "namenode_url": "http://localhost:9870",
        },
        "apache_spark": {
            "status": "simulated",
            "task": "Tasks 14-17",
            "description": "Spark simulation via Python classes (real Spark available via Docker)",
            "docker_command": "docker-compose up spark",
            "spark_ui_url": "http://localhost:4040",
        },
        "note": "All Big Data tasks (12-17) have Python simulation implementations. "
                "See docker-compose.yml to spin up real Hadoop and Spark clusters locally."
    }


@app.post("/api/bigdata/spark/process")
def spark_process_resumes(data: dict):
    """
    Simulates Spark DataFrame processing on resume data (Tasks 14-17).
    """
    start = datetime.now()
    try:
        module = load_task_module(14)  # Spark basics
        results = {
            "task": "Tasks 14-17 - Apache Spark",
            "operation": "Resume text processing with simulated Spark",
            "input_records": data.get("count", 1),
            "transformations": ["filter", "map", "groupBy", "agg"],
            "partitions": 4,
            "cached": True,
        }
        exec_ms = int((datetime.now() - start).total_seconds() * 1000)
        log_task_execution(14, "spark_process", "success", exec_ms, "Spark simulation complete")
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Tasks 18-21: Streaming Simulation ────────────────────────────────────────
@app.get("/api/streaming/status")
def streaming_status():
    """
    Shows streaming pipeline status (Tasks 18-21 - Kafka + Structured Streaming).
    """
    return {
        "kafka": {
            "status": "simulated",
            "tasks": "Tasks 19-20",
            "topics": ["resume-uploads", "analysis-results", "notifications"],
            "partitions": 3,
            "docker_command": "docker-compose up kafka zookeeper",
            "bootstrap_servers": "localhost:9092"
        },
        "structured_streaming": {
            "status": "simulated",
            "task": "Task 21",
            "source": "kafka:resume-uploads",
            "output_mode": "append",
            "trigger": "processingTime 10 seconds"
        }
    }


@app.post("/api/streaming/produce")
def produce_resume_event(request: ResumeAnalysisRequest):
    """
    Simulates publishing a resume upload to Kafka (Task 19 - Kafka Basics).
    """
    start = datetime.now()
    try:
        module = load_task_module(19)
        event = {
            "topic": "resume-uploads",
            "partition": hash(request.resume_text[:50]) % 3,
            "offset": int(datetime.now().timestamp()),
            "key": request.analysis_id or "anonymous",
            "value": {
                "resume_length": len(request.resume_text),
                "has_jd": bool(request.job_description),
                "timestamp": datetime.now().isoformat()
            },
            "task": "Task 19 - Kafka Basics"
        }
        exec_ms = int((datetime.now() - start).total_seconds() * 1000)
        log_task_execution(19, "kafka_produce", "success", exec_ms, f"Produced to partition {event['partition']}")
        return event
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Task 22-23: Airflow DAG status ───────────────────────────────────────────
@app.get("/api/orchestration/dag")
def get_dag_status():
    """
    Returns the Airflow DAG definition for the resume pipeline (Tasks 22-23).
    """
    return {
        "dag_id": "resume_analyzer_pipeline",
        "schedule": "@hourly",
        "tasks": [
            {"id": "ingest_resumes", "operator": "PythonOperator", "task_script": "task11_data_ingestion.py", "upstream": []},
            {"id": "run_etl", "operator": "PythonOperator", "task_script": "task10_etl_vs_elt.py", "upstream": ["ingest_resumes"]},
            {"id": "spark_process", "operator": "SparkSubmitOperator", "task_script": "task14_spark_basics.py", "upstream": ["run_etl"]},
            {"id": "kafka_stream", "operator": "PythonOperator", "task_script": "task19_kafka_basics.py", "upstream": ["spark_process"]},
            {"id": "data_quality", "operator": "PythonOperator", "task_script": "task29_data_quality.py", "upstream": ["spark_process"]},
            {"id": "load_warehouse", "operator": "PythonOperator", "task_script": "task09_data_warehousing.py", "upstream": ["data_quality"]},
            {"id": "update_dashboard", "operator": "PythonOperator", "task_script": "task30_final_project.py", "upstream": ["load_warehouse"]},
        ],
        "retry_policy": {"retries": 3, "retry_delay_minutes": 5},
        "sla_minutes": 30,
        "airflow_ui": "http://localhost:8080",
        "docker_command": "docker-compose up airflow",
        "tasks_demonstrated": ["Task 22 - Airflow Basics", "Task 23 - Airflow Advanced"]
    }


# ── Task 24-27: Cloud ─────────────────────────────────────────────────────────
@app.get("/api/cloud/comparison")
def cloud_comparison():
    """
    Cloud service comparison (Task 24 - Cloud Basics).
    """
    return {
        "aws": {
            "storage": "S3", "compute": "EC2", "warehouse": "Redshift",
            "streaming": "Kinesis", "orchestration": "MWAA (Airflow)"
        },
        "gcp": {
            "storage": "GCS", "compute": "Compute Engine", "warehouse": "BigQuery",
            "streaming": "Pub/Sub", "orchestration": "Cloud Composer (Airflow)"
        },
        "azure": {
            "storage": "Blob Storage", "compute": "Azure VM", "warehouse": "Synapse Analytics",
            "streaming": "Event Hubs", "orchestration": "Azure Data Factory"
        },
        "local_simulation": {
            "storage": "MinIO (S3-compatible)", "compute": "Docker", "warehouse": "PostgreSQL",
            "streaming": "Kafka (Docker)", "orchestration": "Airflow (Docker)"
        },
        "task": "Task 24 - Cloud Basics Comparison"
    }


# ── Task 29: Data Quality ─────────────────────────────────────────────────────
@app.post("/api/quality/validate")
def validate_resume_data(request: ResumeAnalysisRequest):
    """
    Data quality validation checks (Task 29 - Data Quality).
    """
    text = request.resume_text
    checks = []

    # Completeness checks
    checks.append({"check": "not_empty", "passed": len(text.strip()) > 100, "category": "completeness"})
    checks.append({"check": "has_contact_info", "passed": "@" in text, "category": "completeness"})
    checks.append({"check": "has_experience_section", "passed": "EXPERIENCE" in text.upper() or "WORK" in text.upper(), "category": "completeness"})
    checks.append({"check": "has_skills_section", "passed": "SKILL" in text.upper(), "category": "completeness"})
    checks.append({"check": "has_education", "passed": "EDUCATION" in text.upper() or "DEGREE" in text.upper(), "category": "completeness"})

    # Format checks
    checks.append({"check": "reasonable_length", "passed": 200 < len(text) < 10000, "category": "format"})
    checks.append({"check": "no_excessive_whitespace", "passed": text.count('\n\n\n') < 5, "category": "format"})

    # Anomaly detection
    checks.append({"check": "no_suspicious_chars", "passed": len([c for c in text if ord(c) > 127]) / max(len(text), 1) < 0.1, "category": "anomaly"})

    passed = sum(1 for c in checks if c["passed"])
    quality_score = round(passed / len(checks) * 100)

    return {
        "quality_score": quality_score,
        "checks_run": len(checks),
        "checks_passed": passed,
        "checks_failed": len(checks) - passed,
        "details": checks,
        "recommendation": "Resume passes quality checks" if quality_score >= 70 else "Resume needs improvement before analysis",
        "task": "Task 29 - Data Quality"
    }


# ── Task 30: Full pipeline summary ───────────────────────────────────────────
@app.get("/api/pipeline/summary")
def pipeline_summary():
    """
    End-to-end pipeline summary (Task 30 - Final Project).
    """
    conn = get_db_connection()

    total_analyses = conn.execute("SELECT COUNT(*) as c FROM fact_resume_analysis").fetchone()["c"]
    recent_tasks = conn.execute(
        "SELECT task_name, status, execution_time_ms, executed_at FROM task_execution_log ORDER BY executed_at DESC LIMIT 10"
    ).fetchall()
    conn.close()

    return {
        "pipeline": "AI Resume Analyzer - End-to-End Data Engineering Pipeline",
        "total_resumes_analyzed": total_analyses,
        "architecture": "Ingestion → ETL → Spark Processing → Kafka Stream → Airflow DAG → Star Schema → Dashboard",
        "tasks_covered": {
            "foundation": [1, 2, 3, 4, 5],
            "database": [6, 7, 8, 9, 10],
            "big_data": [11, 12, 13, 14, 15, 16, 17],
            "streaming": [18, 19, 20, 21],
            "orchestration": [22, 23],
            "cloud": [24, 25, 26, 27],
            "advanced": [28, 29, 30]
        },
        "recent_task_executions": [dict(r) for r in recent_tasks],
        "task": "Task 30 - Final Project"
    }


# ── Task execution log endpoint ───────────────────────────────────────────────
@app.get("/api/logs")
def get_execution_logs(limit: int = 50):
    """Get task execution logs (Task 1 - Logging, Task 23 - Monitoring)."""
    conn = get_db_connection()
    logs = conn.execute(
        "SELECT * FROM task_execution_log ORDER BY executed_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return {"logs": [dict(r) for r in logs], "count": len(logs)}

# ── Hybrid AI Analysis (Gemini + local fallback) ─────────────────────────────
@app.post("/analyze")
def analyze_resume(request: ResumeAnalysisRequest):
    """Analyze resume with Gemini first and local fallback if needed."""
    if not request.resume_text or not request.resume_text.strip():
        raise HTTPException(status_code=400, detail="resume_text is required")

    # STEP 2: Log received job_description
    print(f"RECEIVED JD: {request.job_description or '(empty)'}")
    print(f"JD length: {len(request.job_description) if request.job_description else 0}")

    try:
        return analyze_resume_hybrid(
            resume_text=request.resume_text,
            job_description=request.job_description or "",
        )
    except Exception as exc:
        logger.exception("Hybrid analysis failed: %s", exc)
        raise HTTPException(status_code=500, detail="Resume analysis failed")


@app.post("/api/analyze")
def analyze_resume_compat(request: ResumeAnalysisRequest):
    """Backward-compatible route using the hybrid analyzer."""
    return analyze_resume(request)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
