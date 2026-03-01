import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

# ==============================
# Chemins robustes
# ==============================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

DB_PATH = os.path.join(DATA_DIR, "jobs.db")
IMG_PATH = os.path.join(DATA_DIR, "top_categories.png")

# ==============================
# Lecture depuis la base
# ==============================

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql("SELECT * FROM jobs", conn)
conn.close()

print("Nombre de jobs en base :", len(df))

# ==============================
# Analyse
# ==============================

top_categories = df['category'].value_counts().head(5)

print("\nTop 5 catégories :")
print(top_categories)

# ==============================
# Visualisation
# ==============================

top_categories.plot(kind='bar')
plt.title("Top 5 Job Categories")
plt.tight_layout()
plt.savefig(IMG_PATH)
plt.show()