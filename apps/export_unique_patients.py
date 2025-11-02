from pathlib import Path
import pandas as pd
from apps.utils.common import get_latest_parquet, OUTPUT_DIR

def export_unique_patients():
    """karteファイルから患者番号・患者氏名の一意リストを作成して出力"""
    # 最新の karte_ ファイルを取得
    karte_files = get_latest_parquet(["karte_"])
    if not karte_files:
        print("⚠ karte_ ファイルが見つかりません。")
        return

    pq_path = karte_files[0]
    print(f"📂 対象ファイル: {pq_path.name}")

    # データ読み込み
    df = pd.read_parquet(pq_path, columns=["患者番号", "患者氏名"])
    
    # 重複を除去してソート
    unique_df = df.drop_duplicates(subset=["患者番号", "患者氏名"]).sort_values("患者番号")

    # 出力ファイル名を生成
    out_ts = pq_path.stem.split("_")[-1]
    csv_out = OUTPUT_DIR / f"unique_patients_{out_ts}.csv"
    pq_out = OUTPUT_DIR / f"unique_patients_{out_ts}.parquet"

    # 出力
    unique_df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    unique_df.to_parquet(pq_out, index=False)
    
    print(f"✅ 一意患者リストを出力しました: {csv_out.name}")
    print(f"✅ 一意患者リストを出力しました: {pq_out.name}")
    print(f"👥 総患者数: {len(unique_df)} 名")

if __name__ == "__main__":
    export_unique_patients()
