"""Shared setup. Every test uses synthetic data only."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for sub in ("orchestrator", "monitoring"):
    sys.path.insert(0, str(ROOT / sub))
