from datetime import datetime
from apps.merge_data import load_all_periods, save_outputs

def main() -> None:
    start = datetime.now()
    print("=== データ結合を開始します ===")

    # data/配下の business_report_YYYYMMDD_YYYYMMDD を走査して
    # 「処置行為集計.csv」だけを縦結合（merge_data.py の設定どおり）
    dfs = load_all_periods()

    # 結合結果を output/ に出力（CSV と Parquet）
    save_outputs(dfs)

    dur = (datetime.now() - start).total_seconds()
    print(f"\n=== 結合完了 ({dur:.1f}s) ===")

if __name__ == "__main__":
    main()
