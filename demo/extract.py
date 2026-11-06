"""Demo step 1: writes a synthetic 'raw' file (no real data anywhere in this repository)."""
import csv, os, random
from pathlib import Path

out = Path(os.environ.get("PIPELINE_OUT", "demo_out")); out.mkdir(parents=True, exist_ok=True)
random.seed(7)
with open(out / "raw.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["id", "category", "amount"])
    for i in range(1, 1201):
        w.writerow([i, random.choice("ABCD"), round(random.uniform(10, 500), 2)])
print("raw.csv written")
