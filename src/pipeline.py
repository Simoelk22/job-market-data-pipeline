import os
import sqlite3
import time
import uuid
from datetime import datetime, UTC

import pandas as pd
import requests

from utils import setup_logger

# ==============================
# Config
# ==============================

API_URL = "https://remotive.com/api/remote-jobs"

# Mets True UNE SEULE FOIS si tu veux reset la DB (quand tu changes le schéma)
RESET_DB = False

# ==============================
# Setup chemins et logger
# ==============================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

logger = setup_logger(BASE_DIR)

DB_PATH = os.path.join(DATA_DIR, "jobs.db")
CSV_PATH = os.path.join(DATA_DIR, "jobs_cleaned.csv")

# ==============================
# Run metadata (monitoring)
# ==============================

run_id = str(uuid.uuid4())
started_at = datetime.now(UTC)
start_time = time.time()

rows_fetched = 0
raw_rows_added = 0
new_clean_rows = 0
status = "STARTED"
error_message = None

# ==============================
# Extraction
# ==============================

try:
    logger.info(f"[{run_id}] Starting extraction from API")

    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    jobs = data.get("jobs", [])

    df = pd.DataFrame(jobs)
    rows_fetched = len(df)

    logger.info(f"[{run_id}] Extraction successful - {rows_fetched} jobs retrieved")

except Exception as e:
    error_message = str(e)
    logger.error(f"[{run_id}] Error during extraction: {error_message}")
    status = "FAILED"
    raise

# ==============================
# Transformation
# ==============================

try:
    # IMPORTANT: on garde id pour déduplication
    df = df[
        [
            "id",
            "title",
            "company_name",
            "category",
            "job_type",
            "publication_date",
            "salary",
            "candidate_required_location",
        ]
    ].copy()

    df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce")
    df = df.dropna(subset=["id", "title"])

    # Timestamp ingestion timezone-aware
    df["ingestion_timestamp"] = datetime.now(UTC)

    # SQLite stocke en TEXT proprement
    df["publication_date"] = df["publication_date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["ingestion_timestamp"] = df["ingestion_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"[{run_id}] Transformation successful")

except Exception as e:
    error_message = str(e)
    logger.error(f"[{run_id}] Error during transformation: {error_message}")
    status = "FAILED"
    raise

# ==============================
# Load - Incremental (SQLite) + Monitoring
# ==============================

conn = None
try:
    conn = sqlite3.connect(DB_PATH)

    # Table monitoring
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            ended_at TEXT,
            duration_sec REAL,
            rows_fetched INTEGER,
            raw_rows_added INTEGER,
            new_clean_rows INTEGER,
            status TEXT,
            error_message TEXT
        );
        """
    )

    # (Optionnel) reset DB si tu veux repartir propre
    if RESET_DB:
        logger.warning(f"[{run_id}] RESET_DB=True -> Dropping raw_jobs and clean_jobs tables")
        conn.execute("DROP TABLE IF EXISTS raw_jobs;")
        conn.execute("DROP TABLE IF EXISTS clean_jobs;")
        conn.commit()

    # Counts BEFORE (si tables existent)
    raw_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_jobs';"
    ).fetchone() is not None
    clean_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='clean_jobs';"
    ).fetchone() is not None

    raw_before = conn.execute("SELECT COUNT(*) FROM raw_jobs;").fetchone()[0] if raw_exists else 0
    clean_before = conn.execute("SELECT COUNT(*) FROM clean_jobs;").fetchone()[0] if clean_exists else 0

    # 1) Append staging
    df.to_sql("raw_jobs", conn, if_exists="append", index=False)

    # 2) Table clean avec PRIMARY KEY
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS clean_jobs (
            id INTEGER PRIMARY KEY,
            title TEXT,
            company_name TEXT,
            category TEXT,
            job_type TEXT,
            publication_date TEXT,
            salary TEXT,
            candidate_required_location TEXT,
            ingestion_timestamp TEXT
        );
        """
    )

    # 3) Insert OR IGNORE vers clean (dedup par PK id)
    conn.execute(
        """
        INSERT OR IGNORE INTO clean_jobs (
            id, title, company_name, category, job_type,
            publication_date, salary, candidate_required_location, ingestion_timestamp
        )
        SELECT
            id, title, company_name, category, job_type,
            publication_date, salary, candidate_required_location, ingestion_timestamp
        FROM raw_jobs;
        """
    )

    # Counts AFTER
    raw_after = conn.execute("SELECT COUNT(*) FROM raw_jobs;").fetchone()[0]
    clean_after = conn.execute("SELECT COUNT(*) FROM clean_jobs;").fetchone()[0]

    raw_rows_added = raw_after - raw_before
    new_clean_rows = clean_after - clean_before

    # 4) Index perf
    conn.execute("CREATE INDEX IF NOT EXISTS idx_raw_jobs_id ON raw_jobs(id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_clean_jobs_id ON clean_jobs(id);")

    # 5) Data quality checks
    null_ids = conn.execute("SELECT COUNT(*) FROM raw_jobs WHERE id IS NULL;").fetchone()[0]
    empty_titles = conn.execute("SELECT COUNT(*) FROM raw_jobs WHERE title IS NULL OR title = '';").fetchone()[0]
    logger.info(f"[{run_id}] Data quality - null_ids={null_ids}, empty_titles={empty_titles}")

    # Export CSV depuis la table clean (production)
    clean_df = pd.read_sql("SELECT * FROM clean_jobs", conn)
    clean_df.to_csv(CSV_PATH, index=False)
    logger.info(f"[{run_id}] CSV export completed from clean_jobs: {CSV_PATH}")

    # Monitoring success
    ended_at = datetime.now(UTC)
    duration_sec = time.time() - start_time
    status = "SUCCESS"

    conn.execute(
        """
        INSERT OR REPLACE INTO pipeline_runs (
            run_id, started_at, ended_at, duration_sec,
            rows_fetched, raw_rows_added, new_clean_rows,
            status, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            run_id,
            started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            duration_sec,
            rows_fetched,
            raw_rows_added,
            new_clean_rows,
            status,
            error_message,
        ),
    )

    conn.commit()
    logger.info(
        f"[{run_id}] Incremental load completed - fetched={rows_fetched}, raw_added={raw_rows_added}, new_clean={new_clean_rows}, duration={duration_sec:.2f}s"
    )

except Exception as e:
    error_message = str(e)
    status = "FAILED"
    logger.error(f"[{run_id}] Error during load/monitoring: {error_message}")

    # Tenter d'écrire le run FAILED dans pipeline_runs
    try:
        if conn is None:
            conn = sqlite3.connect(DB_PATH)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                ended_at TEXT,
                duration_sec REAL,
                rows_fetched INTEGER,
                raw_rows_added INTEGER,
                new_clean_rows INTEGER,
                status TEXT,
                error_message TEXT
            );
            """
        )

        ended_at = datetime.now(UTC)
        duration_sec = time.time() - start_time

        conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs (
                run_id, started_at, ended_at, duration_sec,
                rows_fetched, raw_rows_added, new_clean_rows,
                status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                run_id,
                started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                duration_sec,
                rows_fetched,
                raw_rows_added,
                new_clean_rows,
                status,
                error_message,
            ),
        )
        conn.commit()
    except Exception:
        pass

    raise

finally:
    if conn is not None:
        conn.close()

print("Pipeline executed successfully.")