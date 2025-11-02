# apps/export_unique_procedures.py
from pathlib import Path
import re
from datetime import datetime
import pandas as pd
from apps.utils.common import OUTPUT_DIR

def _get_latest_procedure_with_patient() -> Path | None:
    """output内の最新 procedure_with_patient_*.parquet を返す"""
    files = sorted(OUTPUT_DIR.glob("procedure_with_patient_*.parquet"))
    return files[-1] if files else None

def _suffix_from_filename(p: Path) -> str:
    """
    ファイル名末尾の _HHMMSS を取り出す。
    なければ現在時刻。
    """
    m = re.search(r"_(\d{6})\.parquet$", p.name)
    if m:
        return m.group(1)
    return datetime.now().strftime("%H%M%S")

def export_unique_procedures() -> None:
    """
    最新の procedure_with_patient_*.parquet から
    一意の「処置行為」リストを出力（CSV/Parquet）。
    参考として件数付きの表も併せて出力。
    """
    pq = _get_latest_procedure_with_patient()
    if not pq:
        print("⚠ procedure_with_patient_*.parquet が見つかりません。先に結合処理を実行してください。")
        return

    print(f"📂 対象ファイル: {pq.name}")
    df = pd.read_parquet(pq)

    if "処置行為" not in df.columns:
        print("⚠ カラム『処置行為』が見つかりません。")
        return

    # 一意リスト（NaN除去 → 重複削除 → 昇順ソート）
    unique_df = (
        df["処置行為"]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
        .to_frame(name="処置行為")
    )

    # 件数付き（参考）
    counts_df = (
        df["処置行為"]
        .dropna()
        .value_counts()
        .rename_axis("処置行為")
        .reset_index(name="件数")
        .sort_values(["処置行為"])
        .reset_index(drop=True)
    )

    suf = _suffix_from_filename(pq)

    # 出力
    out_unique_csv = OUTPUT_DIR / f"unique_procedures_{suf}.csv"
    out_unique_pq  = OUTPUT_DIR / f"unique_procedures_{suf}.parquet"
    unique_df.to_csv(out_unique_csv, index=False, encoding="utf-8-sig")
    unique_df.to_parquet(out_unique_pq, index=False)

    out_counts_csv = OUTPUT_DIR / f"unique_procedures_with_counts_{suf}.csv"
    out_counts_pq  = OUTPUT_DIR / f"unique_procedures_with_counts_{suf}.parquet"
    counts_df.to_csv(out_counts_csv, index=False, encoding="utf-8-sig")
    counts_df.to_parquet(out_counts_pq, index=False)

    print(f"✅ 一意リスト: {out_unique_csv.name} / {out_unique_pq.name}")
    print(f"✅ 件数付き   : {out_counts_csv.name} / {out_counts_pq.name}")
    print(f"🔢 一意件数: {len(unique_df):,}")
