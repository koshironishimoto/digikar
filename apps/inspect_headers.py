# apps/inspect_headers.py
import pandas as pd
from apps.utils.common import get_latest_parquet

def show_parquet_header(pq_path):
    """Parquetのカラム名を表示"""
    try:
        df = pd.read_parquet(pq_path, columns=None)
        print(f"\n📁 {pq_path.name}")
        print("=" * (len(pq_path.name) + 4))
        print(", ".join(df.columns))
    except Exception as e:
        print(f"⚠ {pq_path.name} の読み込みに失敗: {e}")

def show_latest_headers():
    """diagnosis / karte / procedure の最新Parquetのヘッダーをこの順で表示"""
    targets = get_latest_parquet(["diagnosis_", "karte_", "procedure_"])
    print("\n=== diagnosis / karte / procedure の Parquetヘッダーを確認 ===")
    if not targets:
        print("⚠ Parquetファイルが見つかりません。")
        return
    print(f"📂 最新 Parquet ファイル: {[p.name for p in targets]}")
    for pq_file in targets:
        show_parquet_header(pq_file)

# 単体実行でも動作するように
if __name__ == "__main__":
    show_latest_headers()
