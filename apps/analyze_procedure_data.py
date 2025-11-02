# apps/analyze_procedure_data.py
from pathlib import Path
import pandas as pd
from apps.utils.common import OUTPUT_DIR


def get_latest_procedure_with_patient() -> Path | None:
    """最新の procedure_with_patient_*.parquet を取得"""
    pq_files = sorted(OUTPUT_DIR.glob("procedure_with_patient_*.parquet"))
    return pq_files[-1] if pq_files else None


def show_procedure_header(df: pd.DataFrame, file_name: str) -> None:
    """カラム名一覧を表示"""
    print(f"\n📁 {file_name}")
    print("=" * (len(file_name) + 4))
    print(", ".join(df.columns))


def analyze_procedure_data() -> None:
    """procedure_with_patient データの基本情報と簡単な集計を表示"""
    pq_path = get_latest_procedure_with_patient()
    if not pq_path:
        print("⚠ procedure_with_patient_*.parquet が見つかりません。")
        return

    print(f"📂 対象ファイル: {pq_path.name}")
    df = pd.read_parquet(pq_path)

    # ヘッダー表示
    show_procedure_header(df, pq_path.name)

    # レコード件数・患者数・処置種類の数を表示
    n_rows = len(df)
    n_patients = df["患者番号"].nunique() if "患者番号" in df.columns else 0
    n_proc_types = df["処置行為"].nunique() if "処置行為" in df.columns else 0

    print(f"\n📊 レコード数: {n_rows:,}")
    print(f"👥 一意患者数: {n_patients:,}")
    print(f"💊 処置行為の種類数: {n_proc_types:,}")

    # 簡易上位10件表示（例：処置行為別件数）
    if "処置行為" in df.columns:
        print("\n🔝 処置行為トップ10（件数順）:")
        print(df["処置行為"].value_counts().head(10))


if __name__ == "__main__":
    analyze_procedure_data()
