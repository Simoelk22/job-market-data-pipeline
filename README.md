# 🚀 Job Market Data Pipeline & Analytics Dashboard

Production-style ETL pipeline that extracts remote job data from the Remotive API, cleans it, loads it into PostgreSQL, tracks pipeline runs, and visualizes insights with Metabase.

---

## ✨ Features

### ✅ ETL Pipeline (Python)
- Extract jobs from **Remotive API**
- Transform & clean fields (dates, nulls, schema)
- Incremental load:
  - **raw_jobs** = staging append-only
  - **clean_jobs** = deduplicated production table (PK on `id`)
- Data quality checks (null IDs, empty titles)
- CSV export to `data/jobs_cleaned.csv`
- Logging to `logs/pipeline.log`

### ✅ Monitoring
- `pipeline_runs` table stores:
  - run_id, timestamps, duration, rows fetched, inserted counts, status, error message

### ✅ Analytics (Metabase Dashboard)
- Total jobs KPI
- Jobs by category
- Jobs by job type
- Top 5 companies
- Jobs by location
- Optional filters (Category / Job Type)

---

## 🏗 Architecture

Remotive API  
⬇  
Python ETL (`src/pipeline.py`)  
⬇  
PostgreSQL (Docker)  
⬇  
Metabase Dashboard  

---

## 🧰 Tech Stack
- Python (pandas, requests, SQLAlchemy)
- PostgreSQL
- Docker & Docker Compose
- Metabase

---

## 📷 Dashboard Preview

> Add screenshots in `screenshots/` then they will render here.

![Dashboard Overview](screenshots/dashboard_overview.png)
![Jobs by Category](screenshots/jobs_by_category.png)
![Top Companies](screenshots/top_companies.png)

---

## 📂 Project Structure

```text
job-market-pipeline/
├── src/
│   ├── pipeline.py
│   └── utils.py
├── data/
│   └── jobs_cleaned.csv
├── logs/
│   └── pipeline.log
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
