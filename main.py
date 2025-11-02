# main.py
from datetime import datetime

# 結合・出力
from apps.merge_data import load_all_periods, save_outputs

# 解析系
from apps.find_duplicate_patients import find_duplicate_patients
from apps.find_katakana_patients import find_katakana_patients
from apps.inspect_headers import show_latest_headers
from apps.export_unique_patients import export_unique_patients
from apps.export_unique_karte_core import export_unique_karte_core
from apps.join_procedure_with_patients import join_procedure_with_patients  # ⑥
from apps.analyze_procedure_data import analyze_procedure_data              # ⑦ 追加
from apps.export_unique_procedures import export_unique_procedures          # ⑧ 追加


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


def run_export_unique_karte_core() -> None:
    print("\n--- ⑤ カルテID×患者番号で一意化したリストを出力（CSV/Parquet） ---")
    try:
        export_unique_karte_core()
    except Exception as e:
        print(f"⚠ karte一意リスト出力でエラー: {e}")


def run_join_procedure_with_patients() -> None:
    print("\n--- ⑥ procedure に患者番号・氏名を付与して出力（CSV/Parquet） ---")
    try:
        join_procedure_with_patients()
    except Exception as e:
        print(f"⚠ procedure結合出力でエラー: {e}")


def run_analyze_procedure_data() -> None:
    print("\n--- ⑦ procedure集計のサマリ表示（ターミナル出力） ---")
    try:
        analyze_procedure_data()
    except Exception as e:
        print(f"⚠ procedure集計でエラー: {e}")


def run_export_unique_procedures() -> None:
    print("\n--- ⑧ 一意の処置行為リストを出力（CSV/Parquet） ---")
    try:
        export_unique_procedures()
    except Exception as e:
        print(f"⚠ 一意の処置行為リスト出力でエラー: {e}")


def run_post_analyses() -> None:
    """結合完了後に行う分析をまとめて実行（依存関係順）"""
    run_duplicate_check()              # ①
    run_katakana_check()               # ②
    run_show_headers()                 # ③
    run_export_unique_patients()       # ④
    run_export_unique_karte_core()     # ⑤
    run_join_procedure_with_patients() # ⑥（⑦,⑧の前提）
    run_analyze_procedure_data()       # ⑦
    run_export_unique_procedures()     # ⑧


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
