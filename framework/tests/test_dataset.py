import csv

CSV_PATH = "data/source/customers.csv"

def main():
   with open(CSV_PATH, "r", encoding="utf-8") as f:
       reader = csv.DictReader(f)
       assert reader.fieldnames == ["customer_id","name","age","country","email"], "Header mismatch"
       rows = list(reader)
       assert len(rows) > 0, "Dataset empty"

   print("✅ Local dataset tests passed")

if __name__ == "__main__":
   main()
