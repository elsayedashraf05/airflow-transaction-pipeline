from datetime import datetime
with open("/data/transactions_report.txt", "a") as f:
  f.write(f"Processed at {datetime.now()}\n")
  print("Report updated successfully.")
