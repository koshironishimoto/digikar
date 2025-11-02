# main.py
from datetime import datetime

# 結合・出力
from apps.merge_data import load_all_periods, save_outputs

# 解析系
from apps.find_duplicate_patients import find_duplicate_patients
from apps.find_katakana_patients import find_katakana_patients
from apps.inspect_headers import show_latest_headers
from apps.export_unique_patients import export_unique_patients  # ★ 追加


def run_duplicate_check() -> None:
    print("\n--- ① 氏名あたり患者番号の重複チェック ---")
    try:
        find_duplicate_patients()
    except Exception as e:
        print(f"⚠ 重複チェックでエラー: {e}")


def run_katakana_check() -> None:
    print("\n--- ② カタカナ氏名の患者一覧 ---")
    try:
        find_katakana_patients()
    except Exception as e:
        print(f"⚠ カタカナ抽出でエラー: {e}")


def run_show_headers() -> None:
    print("\n--- ③ 各テーブルの最新Parquetヘッダー確認 ---")
    try:
        show_latest_headers()
    except Exception as e:
        print(f"⚠ ヘッダー確認でエラー: {e}")


def run_export_unique_patients() -> None:
    print("\n--- ④ 一意の患者リストを出力（CSV/Parquet） ---")
    try:
        export_unique_patients()
    except Exception as e:
        print(f"⚠ 一意患者リスト出力でエラー: {e}")


def run_post_analyses() -> None:
    """結合完了後に行う分析をまとめて実行"""
    run_duplicate_check()
    run_katakana_check()
    run_show_headers()
    run_export_unique_patients()


def main() -> None:
    start = datetime.now()
    print("=== データ結合を開始します ===")

    # data/ 配下の business_report_YYYYMMDD_YYYYMMDD を走査して
    # 「処置行為集計.csv」「カルテ集計.csv」「傷病名一覧.csv」を縦結合（merge_data.py の設定に依存）
    dfs = load_all_periods()

    # 結合結果を output/ に出力（CSV と Parquet）
    save_outputs(dfs)

    # 結合直後に分析を実行（最新 Parquet を使ってターミナル出力）
    run_post_analyses()

    dur = (datetime.now() - start).total_seconds()
    print(f"\n=== 結合 + 分析 完了 ({dur:.1f}s) ===")


if __name__ == "__main__":
    main()
