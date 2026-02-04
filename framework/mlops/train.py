import os
import json
from datetime import datetime, timezone

import pandas as pd
from databricks import sql

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import joblib


SILVER_TABLE = os.getenv("SILVER_TABLE", "demo.silver_customers_v2")
MODEL_NAME = os.getenv("MODEL_NAME", "customer_risk")
ARTIFACT_DIR = os.getenv("ARTIFACT_DIR", "framework/mlops/artifacts")
GIT_SHA = os.getenv("GIT_SHA", "local")


def fetch_training_data() -> pd.DataFrame:
   host = os.environ["DATABRICKS_HOST"]
   http_path = os.environ["DATABRICKS_HTTP_PATH"]
   token = os.environ["DATABRICKS_TOKEN"]

   query = f"""
     SELECT
       dq_is_name_null,
       dq_is_underage,
       CASE WHEN dq_status <> 'PASS' THEN 1 ELSE 0 END AS is_risky
     FROM {SILVER_TABLE}
   """

   with sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
       with conn.cursor() as cur:
           cur.execute(query)
           rows = cur.fetchall()
           cols = [d[0] for d in cur.description]
   return pd.DataFrame(rows, columns=cols)


def main():
   os.makedirs(ARTIFACT_DIR, exist_ok=True)

   df = fetch_training_data()

   # Validación mínima para no entrenar basura
   if df.empty:
       raise RuntimeError(f"No hay data en {SILVER_TABLE} para entrenar.")

   # Features + label
   X = df[["dq_is_name_null", "dq_is_underage"]].astype(int)
   y = df["is_risky"].astype(int)

   # Si el dataset es demasiado pequeño, entrenamos con todo y evaluamos sobre lo mismo (solo didáctico)
   if len(df) < 10 or y.nunique() < 2:
       X_train, X_test, y_train, y_test = X, X, y, y
       note = "tiny_dataset_or_single_class -> evaluated on same data (didactic)"
   else:
       X_train, X_test, y_train, y_test = train_test_split(
           X, y, test_size=0.3, random_state=42, stratify=y
       )
       note = "train_test_split"

   model = LogisticRegression()
   model.fit(X_train, y_train)

   y_pred = model.predict(X_test)

   metrics = {
       "accuracy": float(accuracy_score(y_test, y_pred)),
       "precision": float(precision_score(y_test, y_pred, zero_division=0)),
       "recall": float(recall_score(y_test, y_pred, zero_division=0)),
       "f1": float(f1_score(y_test, y_pred, zero_division=0)),
       "rows": int(len(df)),
       "note": note,
   }

   schema = {
       "features": ["dq_is_name_null", "dq_is_underage"],
       "label": "is_risky",
       "source": SILVER_TABLE,
   }

   metadata = {
       "model_name": MODEL_NAME,
       "git_sha": GIT_SHA,
       "created_at_utc": datetime.now(timezone.utc).isoformat(),
   }

   # Guardar artefactos
   model_path = os.path.join(ARTIFACT_DIR, "model.pkl")
   joblib.dump(model, model_path)

   with open(os.path.join(ARTIFACT_DIR, "metrics.json"), "w", encoding="utf-8") as f:
       json.dump(metrics, f, indent=2)

   with open(os.path.join(ARTIFACT_DIR, "schema.json"), "w", encoding="utf-8") as f:
       json.dump(schema, f, indent=2)

   with open(os.path.join(ARTIFACT_DIR, "run_metadata.json"), "w", encoding="utf-8") as f:
       json.dump(metadata, f, indent=2)

   print("✅ Training OK")
   print("📌 Metrics:", metrics)
   print("📦 Artifacts saved in:", ARTIFACT_DIR)


if __name__ == "__main__":
   main()
