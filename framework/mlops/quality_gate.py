import json
import sys
from pathlib import Path

MIN_ACCURACY = 0.8
MIN_F1 = 0.75

ARTIFACTS_DIR = Path("framework/mlops/artifacts")
METRICS_FILE = ARTIFACTS_DIR / "metrics.json"

def main():
   if not METRICS_FILE.exists():
       print("❌ Metrics file not found")
       sys.exit(1)

   metrics = json.loads(METRICS_FILE.read_text())

   acc = metrics.get("accuracy", 0)
   f1 = metrics.get("f1", 0)

   print(f"📊 Model metrics → accuracy={acc}, f1={f1}")

   if acc < MIN_ACCURACY or f1 < MIN_F1:
       print("❌ Quality Gate FAILED")
       sys.exit(1)

   print("✅ Quality Gate PASSED")

if __name__ == "__main__":
   main()
