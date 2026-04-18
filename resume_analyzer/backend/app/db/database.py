import os
import sqlite3


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(CURRENT_DIR, "resume_analyzer.db")


def get_db_connection() -> sqlite3.Connection:
    """Return a reusable SQLite connection for the project database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn
