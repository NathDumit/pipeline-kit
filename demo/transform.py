"""Demo step 2: reads raw.csv and writes a summary per category. Fails loudly if the input is missing."""
import csv, os, sys
from collections import defaultdict
from pathlib import Path

out = Path(os.environ.get("PIPELINE_OUT", "demo_out")); raw = out / "raw.csv"
if not raw.exists():
    sys.exit(f"missing input {raw}")
tot = defaultdict(float); n = defaultdict(int)
with open(raw, encoding="utf-8") as f:
    for r in csv.DictReader(f):
        tot[r["category"]] += float(r["amount"]); n[r["category"]] += 1
with open(out / "summary.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["category", "orders", "total", "average"])
    for k in sorted(tot):
        w.writerow([k, n[k], round(tot[k], 2), round(tot[k] / n[k], 2)])
print("summary.csv written")
