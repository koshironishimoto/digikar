# apps/inspect_headers.py
from pathlib import Path
import re
import pandas as pd
from apps.utils.common import OUTPUT_DIR

RE_DIAG = re.compile(r"^diagnosis_\d{8}_\d{6}\.parquet$")
RE_KARTE = re.compile(r"^karte_\d{8}_\d{6}\.parquet$")
RE_PROC_BASE = re.compile(r"^procedure_\d{8}_\d{6}\.parquet$")  # with_patient を除外

def _pick_latest_by_regex(regex: re.Pattern) -> Path | None:
    files = sorted(OUTPUT_DIR.glob("*.parquet"))
    matched = [f for f in files if regex.match(f.name)]
    return matched[-1] if matched else None

def show_parquet_header(pq_path: Path):
    try:
        df = pd.read_parquet(pq_path, columns=None)
        print(f"\n📁 {pq_path.name}")
        print("=" * (len(pq_path.name) + 4))
        print(", ".join(df.columns))
    except Exception as e:
        print(f"⚠ {pq_path.name} の読み込みに失敗: {e}")

def show_latest_headers():
    print("=== diagnosis / karte / procedure の Parquetヘッダーを確認 ===")
    targets = []
    for regex in (RE_DIAG, RE_KARTE, RE_PROC_BASE):
        p = _pick_latest_by_regex(regex)
        if p:
            targets.append(p)

    if not targets:
        print("⚠ Parquetファイルが見つかりません。")
        return

    print(f"📂 最新 Parquet ファイル: {[p.name for p in targets]}")
    for pq_file in targets:
        show_parquet_header(pq_file)
