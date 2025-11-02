# apps/find_duplicate_patients.py
import pandas as pd
from apps.utils.common import get_latest_parquet, OUTPUT_DIR

def find_duplicate_patients():
    """患者氏名あたりに複数の患者番号があるケースを検出"""
    diagnosis_path = get_latest_parquet("diagnosis")
    if not diagnosis_path:
        print("⚠ diagnosis の Parquet ファイルが見つかりません。")
        return

    df = pd.read_parquet(diagnosis_path)

    if not {"患者氏名", "患者番号"}.issubset(df.columns):
        print("⚠ 必要な列（患者氏名, 患者番号）が見つかりません。")
        print("実際の列名:", df.columns.tolist())
        return

    dupes = (
        df.groupby("患者氏名")["患者番号"]
        .nunique()
        .reset_index(name="患者番号の種類数")
        .query("患者番号の種類数 > 1")
    )

    if dupes.empty:
        print("✅ 患者氏名ごとの患者番号の重複はありません。")
    else:
        print("⚠ 以下の患者氏名に複数の患者番号があります:\n")
        print(dupes.to_string(index=False))

if __name__ == "__main__":
    print("=== 患者氏名あたりの患者番号重複チェック ===")
    find_duplicate_patients()
