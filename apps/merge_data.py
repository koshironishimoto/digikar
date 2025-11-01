from pathlib import Path
import pandas as pd
import re
from datetime import datetime

# ==========================================
# 設定
# ==========================================

# このファイル (main.py) の1つ上の階層に data フォルダがある想定
BASE_DIR = Path(__file__).resolve().parents[2] / "data"

# 読み込むCSVファイル名（固定）
TARGET_FILES = {
    "カルテ集計.csv": "karte",
    "処置行為集計.csv": "procedure",
    "傷病名一覧.csv": "diagnosis",
}

# 出力フォルダ: main.py と同じ階層の output フォルダを指定
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# business_report_YYYYMMDD_YYYYMMDD という形式のみ許可
DATE_DIR_RE = re.compile(r"^business_report_(\d{8})_(\d{8})$")

# ==========================================
# 関数定義
# ==========================================

def read_csv_with_fallback(csv_path: Path) -> pd.DataFrame:
    """日本語CSVに対応するため、エンコーディングを順番に試す"""
    for enc in ("cp932", "utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(csv_path, encoding=enc, dtype=str)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(csv_path, encoding="cp932", errors="ignore", dtype=str)

def parse_period_from_dir(dir_name: str) -> tuple[str, str]:
    """フォルダ名から期間を抽出（business_report_YYYYMMDD_YYYYMMDD のみ）"""
    m = DATE_DIR_RE.match(dir_name)
    if not m:
        raise ValueError(f"期間フォルダ名として解釈できません: {dir_name}")
    beg, end = m.groups()
    return (f"{beg[:4]}-{beg[4:6]}-{beg[6:]}", f"{end[:4]}-{end[4:6]}-{end[6:]}")

def iter_period_dirs(base: Path):
    """base直下のフォルダから 'business_report_YYYYMMDD_YYYYMMDD' のみ列挙"""
    for p in sorted(base.iterdir()):
        if p.is_dir() and DATE_DIR_RE.match(p.name):
            yield p

def load_all_periods() -> dict[str, pd.DataFrame]:
    """全期間フォルダのデータを読み込み、種類ごとに結合"""
    buckets: dict[str, list[pd.DataFrame]] = {v: [] for v in TARGET_FILES.values()}

    for period_dir in iter_period_dirs(BASE_DIR):
        period_start, period_end = parse_period_from_dir(period_dir.name)
        print(f"処理中: {period_dir.name}")
        for file_name, key in TARGET_FILES.items():
            csv_path = period_dir / file_name
            if not csv_path.exists():
                print(f"  ⚠ {file_name} が存在しません。スキップします。")
                continue
            df = read_csv_with_fallback(csv_path)
            df["期間開始"] = period_start
            df["期間終了"] = period_end
            df["ソースフォルダ"] = period_dir.name
            buckets[key].append(df)

    combined = {}
    for key, frames in buckets.items():
        combined[key] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return combined

def save_outputs(dfs: dict[str, pd.DataFrame]):
    """CSV (UTF-8-BOM) と Parquet の保存"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    for key, df in dfs.items():
        csv_out = OUTPUT_DIR / f"{key}_{ts}.csv"
        df.to_csv(csv_out, index=False, encoding="utf-8-sig")
        try:
            pq_out = OUTPUT_DIR / f"{key}_{ts}.parquet"
            df.to_parquet(pq_out, index=False)
        except Exception:
            pass
        print(f"✅ 出力完了: {csv_out.name}")

# ==========================================
# メイン処理
# ==========================================

if __name__ == "__main__":
    print("=== 期間フォルダ内のCSVを統合します ===")
    dfs = load_all_periods()
    save_outputs(dfs)
    print("\n=== すべて完了しました ===")
    print(f"出力フォルダ: {OUTPUT_DIR.resolve()}")
