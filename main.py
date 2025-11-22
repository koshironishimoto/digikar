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
from apps.join_procedure_with_patients import join_procedure_with_patients
from apps.analyze_procedure_data import analyze_procedure_data
from apps.export_unique_procedures import export_unique_procedures
from apps.extract_free_comments import run_extract_free_comments  # ⑨ 追加


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
        print(f"⚠ カタカナチェックでエラー: {e}")


def run_inspect_headers() -> None:
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
        print(f"⚠ 一意カルテリスト出力でエラー: {e}")


def run_join_procedure_with_patients() -> None:
    print("\n--- ⑥ procedure に患者番号・氏名を付与して出力（CSV/Parquet） ---")
    try:
        join_procedure_with_patients()
    except Exception as e:
        print(f"⚠ procedure結合でエラー: {e}")


def run_analyze_procedure_data() -> None:
    print("\n--- ⑦ procedure集計のサマリ表示（ターミナル出力） ---")
    try:
        analyze_procedure_data()
    except Exception as e:
        print(f"⚠ procedure解析でエラー: {e}")


def run_export_unique_procedures() -> None:
    print("\n--- ⑧ 一意の処置行為リストを出力（CSV/Parquet） ---")
    try:
        export_unique_procedures()
    except Exception as e:
        print(f"⚠ 一意処置行為リスト出力でエラー: {e}")


def main() -> None:
    start = datetime.now()
    print("=== データ結合を開始します ===")

    try:
        periods = load_all_periods()
        save_outputs(periods)
    except Exception as e:
        print(f"⚠ 結合処理でエラー: {e}")
        return

    # 解析フロー
    run_duplicate_check()
    run_katakana_check()
    run_inspect_headers()
    run_export_unique_patients()
    run_export_unique_karte_core()
    run_join_procedure_with_patients()
    run_analyze_procedure_data()
    run_export_unique_procedures()

    # ⑨ レセプト・フリーコメント抽出
    run_extract_free_comments()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n=== 結合 + 分析 完了 ({elapsed:.1f}s) ===")


if __name__ == "__main__":
    main()
