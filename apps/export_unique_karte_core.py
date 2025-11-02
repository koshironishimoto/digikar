# apps/export_unique_karte_core.py
from pathlib import Path
import pandas as pd
from apps.utils.common import get_latest_parquet, OUTPUT_DIR

# 対象カラム
COLUMNS = ["カルテID", "患者番号", "患者氏名", "診療科", "保険種別", "日付"]

def export_unique_karte_core():
    """karteの主要情報をカルテID・患者番号で一意化して出力"""
    karte_files = get_latest_parquet(["karte_"])
    if not karte_files:
        print("⚠ karte_ ファイルが見つかりません。先に結合を実行してください。")
        return

    pq_path = karte_files[0]
    print(f"📂 対象ファイル: {pq_path.name}")

    # データ読み込み
    df = pd.read_parquet(pq_path, engine="pyarrow")

    # 必要カラムがあるか確認
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        print(f"⚠ 必要列が不足しています: {missing}")
        print(f"  karteの列: {list(df.columns)}")
        return

    # 必要カラムだけ抽出
    df = df[COLUMNS]

    # 一意化：カルテID×患者番号
    unique_df = df.drop_duplicates(subset=["カルテID", "患者番号"])

    # 並び替え：患者番号（昇順）→ 日付（昇順）
    unique_df = unique_df.sort_values(by=["患者番号", "日付"], kind="mergesort").reset_index(drop=True)

    # 出力ファイル名
    out_ts = pq_path.stem.split("_")[-1]
    csv_out = OUTPUT_DIR / f"unique_karte_core_{out_ts}.csv"
    pq_out  = OUTPUT_DIR / f"unique_karte_core_{out_ts}.parquet"

    # 出力
    unique_df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    unique_df.to_parquet(pq_out, index=False)

    print(f"✅ 一意カルテリストを出力しました: {csv_out.name}")
    print(f"✅ 一意カルテリストを出力しました: {pq_out.name}")
    print(f"📊 レコード数: {len(unique_df)}")

if __name__ == "__main__":
    export_unique_karte_core()
