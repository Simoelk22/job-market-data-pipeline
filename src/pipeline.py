import requests
import pandas as pd
import sqlite3
import os
from utils import setup_logger

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
# Extraction
# ==============================

try:
    logger.info("Starting extraction from API")

    url = "https://remotive.com/api/remote-jobs"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    jobs = data['jobs']

    df = pd.DataFrame(jobs)

    logger.info(f"Extraction successful - {len(df)} jobs retrieved")

except Exception as e:
    logger.error(f"Error during extraction: {e}")
    raise

# ==============================
# Transformation
# ==============================

try:
    df = df[['title', 'company_name', 'category', 'job_type',
             'publication_date', 'salary', 'candidate_required_location']]

    df['publication_date'] = pd.to_datetime(df['publication_date'])
    df = df.dropna(subset=['title'])

    logger.info("Transformation successful")

except Exception as e:
    logger.error(f"Error during transformation: {e}")
    raise

# ==============================
# Load (SQLite)
# ==============================

try:
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("jobs", conn, if_exists="replace", index=False)
    conn.close()

    logger.info("Data loaded into SQLite")

except Exception as e:
    logger.error(f"Error during load: {e}")
    raise

# ==============================
# Export CSV
# ==============================

df.to_csv(CSV_PATH, index=False)
logger.info("CSV export completed")

print("Pipeline executed successfully.")