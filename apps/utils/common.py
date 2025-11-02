# apps/utils/common.py
from pathlib import Path

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"

def get_latest_parquet(prefixes):
    """prefix のリストに対して、output フォルダ内の最新 Parquet を取得"""
    pq_files = sorted(OUTPUT_DIR.glob("*.parquet"))
    targets = []
    for prefix in prefixes:
        matches = sorted([f for f in pq_files if f.name.startswith(prefix)])
        if matches:
            targets.append(matches[-1])
    return targets
