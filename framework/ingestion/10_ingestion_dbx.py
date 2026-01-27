# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 10 - Ingestion (Bronze/Landing)
# MAGIC Este notebook realiza una ingesta mínima:
# MAGIC - Lee data/source/customers.csv
# MAGIC - Escribe en data/landing/customers_<timestamp>.csv
# MAGIC - Loggea issues básicos (sin bloquear aún)

# COMMAND ----------

import os
import csv
import shutil
from datetime import datetime
from pathlib import Path

# COMMAND ----------

# Parametrización simple
ENV = os.getenv("ENV", "dev")  # para Actions / futuro Databricks Jobs
SOURCE_FILE = Path("data/source/customers.csv")
LANDING_DIR = Path("data/landing")
LOGS_DIR = Path("logs")

# COMMAND ----------

def detect_issues(rows):
    """Detecta issues típicos de calidad (solo observación, no bloqueo)."""
    issues = {"duplicate_ids": 0, "missing_name": 0, "underage": 0}
    seen_ids = set()

    for r in rows:
        cid = (r.get("customer_id") or "").strip()
        name = (r.get("name") or "").strip()

        # duplicates
        if cid in seen_ids:
            issues["duplicate_ids"] += 1
        else:
            seen_ids.add(cid)

        # missing name
        if name == "":
            issues["missing_name"] += 1

        # underage
        try:
            if int(r.get("age", "0")) < 18:
                issues["underage"] += 1
        except ValueError:
            # Si age viene mal, lo cuentas como issue (opcional)
            issues["underage"] += 1

    return issues

# COMMAND ----------

def run_ingestion():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    # Leer CSV
    with open(SOURCE_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Detectar issues (solo logging)
    issues = detect_issues(rows)

    # Escribir a landing con timestamp
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    target_file = LANDING_DIR / f"customers_{ts}.csv"

    shutil.copy2(SOURCE_FILE, target_file)

    # Log
    log_msg = (
        f"[OK] Ingestion completed | ENV={ENV}\n"
        f"Source: {SOURCE_FILE}\n"
        f"Target: {target_file}\n"
        f"Rows ingested: {len(rows)}\n"
        f"Issues detected:\n"
        f"  - Duplicate customer_id: {issues['duplicate_ids']}\n"
        f"  - Missing name: {issues['missing_name']}\n"
        f"  - Underage customers: {issues['underage']}\n"
        f"Timestamp: {ts}\n"
        "------------------------------------\n"
    )

    print(log_msg)
    with open(LOGS_DIR / "ingestion.log", "a", encoding="utf-8") as lf:
        lf.write(log_msg)

# COMMAND ----------

if __name__ == "__main__":
    run_ingestion()
