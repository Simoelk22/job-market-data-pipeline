import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# ==============================
# Paths (robustes)
# ==============================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "jobs.db")

TOP_CAT_IMG = os.path.join(DATA_DIR, "top_categories.png")
RUNS_NEW_IMG = os.path.join(DATA_DIR, "runs_new_clean_rows.png")
RUNS_RAW_IMG = os.path.join(DATA_DIR, "runs_raw_rows_added.png")

# ==============================
# Load data from SQLite
# ==============================

conn = sqlite3.connect(DB_PATH)

# Production table
clean_df = pd.read_sql("SELECT * FROM clean_jobs", conn)

# Monitoring table (may be empty early on)
runs_df = pd.read_sql(
    """
    SELECT
        run_id,
        started_at,
        duration_sec,
        rows_fetched,
        raw_rows_added,
        new_clean_rows,
        status
    FROM pipeline_runs
    ORDER BY started_at ASC
    """,
    conn,
)

conn.close()

print("=== clean_jobs ===")
print(f"Rows: {len(clean_df)}")
print(clean_df.head(5))

# ==============================
# Analysis 1: Top 5 categories
# ==============================

if "category" in clean_df.columns and len(clean_df) > 0:
    top_categories = clean_df["category"].value_counts().head(5)

    print("\n=== Top 5 categories ===")
    print(top_categories)

    top_categories.plot(kind="bar")
    plt.title("Top 5 Job Categories (clean_jobs)")
    plt.tight_layout()
    plt.savefig(TOP_CAT_IMG)
    plt.show()

    print(f"\nSaved: {TOP_CAT_IMG}")
else:
    print("\nNo data available in clean_jobs for category analysis.")

# ==============================
# Monitoring plots
# ==============================

if len(runs_df) > 0:
    # Parse started_at for plotting
    runs_df["started_at"] = pd.to_datetime(runs_df["started_at"], errors="coerce")

    # Keep only SUCCESS runs to avoid weird spikes
    runs_ok = runs_df[runs_df["status"] == "SUCCESS"].copy()

    print("\n=== pipeline_runs (latest 5) ===")
    print(runs_df.tail(5))

    if len(runs_ok) > 0:
        # Plot 1: new_clean_rows per run
        runs_ok.set_index("started_at")["new_clean_rows"].plot(kind="line", marker="o")
        plt.title("New clean rows per run (monitoring)")
        plt.ylabel("new_clean_rows")
        plt.tight_layout()
        plt.savefig(RUNS_NEW_IMG)
        plt.show()
        print(f"Saved: {RUNS_NEW_IMG}")

        # Plot 2: raw_rows_added per run
        runs_ok.set_index("started_at")["raw_rows_added"].plot(kind="line", marker="o")
        plt.title("Raw rows added per run (staging growth)")
        plt.ylabel("raw_rows_added")
        plt.tight_layout()
        plt.savefig(RUNS_RAW_IMG)
        plt.show()
        print(f"Saved: {RUNS_RAW_IMG}")
    else:
        print("\nNo SUCCESS runs found in pipeline_runs (nothing to plot).")
else:
    print("\nNo rows in pipeline_runs yet (run pipeline.py at least once).")