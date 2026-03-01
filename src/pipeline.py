import os
import time
import uuid
from datetime import datetime, UTC

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from utils import setup_logger

API_URL = "https://remotive.com/api/remote-jobs"
RESET_DB = False  # True une seule fois si tu changes le schéma

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

logger = setup_logger(BASE_DIR)
CSV_PATH = os.path.join(DATA_DIR, "jobs_cleaned.csv")

# Postgres env
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "jobdb")
DB_USER = os.getenv("DB_USER", "jobuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "jobpassword")
DB_PORT = os.getenv("DB_PORT", "5432")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Run metadata
run_id = str(uuid.uuid4())
started_at = datetime.now(UTC)
start_time = time.time()

rows_fetched = 0
raw_rows_added = 0
new_clean_rows = 0
status = "STARTED"
error_message = None


def wait_for_postgres(retries=30, delay=2):
    last_err = None
    for i in range(1, retries + 1):
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1;"))
            logger.info(f"[{run_id}] Postgres is ready (attempt {i}/{retries})")
            return
        except Exception as e:
            last_err = e
            logger.warning(f"[{run_id}] Waiting for Postgres... (attempt {i}/{retries}) - {e}")
            time.sleep(delay)
    raise RuntimeError(f"Postgres not ready after {retries} attempts. Last error: {last_err}")


try:
    # 1) Extract
    logger.info(f"[{run_id}] Starting extraction from API")
    response = requests.get(API_URL, timeout=15)
    response.raise_for_status()
    data = response.json()
    jobs = data.get("jobs", [])
    df = pd.DataFrame(jobs)
    rows_fetched = len(df)
    logger.info(f"[{run_id}] Extraction successful - {rows_fetched} jobs retrieved")

    # 2) Transform
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

    df["ingestion_timestamp"] = datetime.now(UTC)
    df["publication_date"] = df["publication_date"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df["ingestion_timestamp"] = df["ingestion_timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"[{run_id}] Transformation successful")

    # 3) Load
    wait_for_postgres()

    # transaction DDL + insert + monitoring
    with engine.begin() as conn:
        # Create tables
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                ended_at TEXT,
                duration_sec DOUBLE PRECISION,
                rows_fetched INTEGER,
                raw_rows_added INTEGER,
                new_clean_rows INTEGER,
                status TEXT,
                error_message TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_jobs (
                id BIGINT,
                title TEXT,
                company_name TEXT,
                category TEXT,
                job_type TEXT,
                publication_date TEXT,
                salary TEXT,
                candidate_required_location TEXT,
                ingestion_timestamp TEXT
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clean_jobs (
                id BIGINT PRIMARY KEY,
                title TEXT,
                company_name TEXT,
                category TEXT,
                job_type TEXT,
                publication_date TEXT,
                salary TEXT,
                candidate_required_location TEXT,
                ingestion_timestamp TEXT
            );
        """))

        if RESET_DB:
            logger.warning(f"[{run_id}] RESET_DB=True -> TRUNCATE tables")
            conn.execute(text("TRUNCATE TABLE raw_jobs;"))
            conn.execute(text("TRUNCATE TABLE clean_jobs;"))
            conn.execute(text("TRUNCATE TABLE pipeline_runs;"))

        raw_before = conn.execute(text("SELECT COUNT(*) FROM raw_jobs;")).scalar_one()
        clean_before = conn.execute(text("SELECT COUNT(*) FROM clean_jobs;")).scalar_one()

    # pandas to_sql fait ses propres transactions -> ok
    df.to_sql("raw_jobs", engine, if_exists="append", index=False, method="multi")

    with engine.begin() as conn:
        # Dedup insert
        conn.execute(text("""
            INSERT INTO clean_jobs (
                id, title, company_name, category, job_type,
                publication_date, salary, candidate_required_location, ingestion_timestamp
            )
            SELECT
                id, title, company_name, category, job_type,
                publication_date, salary, candidate_required_location, ingestion_timestamp
            FROM raw_jobs
            ON CONFLICT (id) DO NOTHING;
        """))

        raw_after = conn.execute(text("SELECT COUNT(*) FROM raw_jobs;")).scalar_one()
        clean_after = conn.execute(text("SELECT COUNT(*) FROM clean_jobs;")).scalar_one()

        raw_rows_added = raw_after - raw_before
        new_clean_rows = clean_after - clean_before

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_raw_jobs_id ON raw_jobs(id);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_clean_jobs_id ON clean_jobs(id);"))

        null_ids = conn.execute(text("SELECT COUNT(*) FROM raw_jobs WHERE id IS NULL;")).scalar_one()
        empty_titles = conn.execute(text("SELECT COUNT(*) FROM raw_jobs WHERE title IS NULL OR title = '';")).scalar_one()
        logger.info(f"[{run_id}] Data quality - null_ids={null_ids}, empty_titles={empty_titles}")

        clean_df = pd.read_sql("SELECT * FROM clean_jobs", conn)
        clean_df.to_csv(CSV_PATH, index=False)
        logger.info(f"[{run_id}] CSV export completed from clean_jobs: {CSV_PATH}")

        ended_at = datetime.now(UTC)
        duration_sec = time.time() - start_time
        status = "SUCCESS"

        conn.execute(
            text("""
                INSERT INTO pipeline_runs (
                    run_id, started_at, ended_at, duration_sec,
                    rows_fetched, raw_rows_added, new_clean_rows,
                    status, error_message
                ) VALUES (:run_id, :started_at, :ended_at, :duration_sec,
                          :rows_fetched, :raw_rows_added, :new_clean_rows,
                          :status, :error_message)
                ON CONFLICT (run_id) DO UPDATE SET
                    started_at = EXCLUDED.started_at,
                    ended_at = EXCLUDED.ended_at,
                    duration_sec = EXCLUDED.duration_sec,
                    rows_fetched = EXCLUDED.rows_fetched,
                    raw_rows_added = EXCLUDED.raw_rows_added,
                    new_clean_rows = EXCLUDED.new_clean_rows,
                    status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message;
            """),
            {
                "run_id": run_id,
                "started_at": started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ended_at": ended_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "duration_sec": duration_sec,
                "rows_fetched": rows_fetched,
                "raw_rows_added": raw_rows_added,
                "new_clean_rows": new_clean_rows,
                "status": status,
                "error_message": error_message,
            },
        )

        logger.info(
            f"[{run_id}] Incremental load completed - fetched={rows_fetched}, raw_added={raw_rows_added}, new_clean={new_clean_rows}, duration={duration_sec:.2f}s"
        )

except Exception as e:
    error_message = str(e)
    status = "FAILED"
    logger.error(f"[{run_id}] Pipeline FAILED: {error_message}")
    raise

print("Pipeline executed successfully.")
