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
from apps.analyze_procedure_data import analyze_procedure_data  # ★ 追加（⑦）


# --- 分析実行関数群 ---
def run_duplicate_check():
    print("\n--- ① 氏名あたり患者番号の重複チェック ---")
    try:
        find_duplicate_patients()
    except Exception as e:
        print(f"⚠ 重複チェックでエラー: {e}")


def run_katakana_check():
    print("\n--- ② カタカナ氏名の患者一覧 ---")
    try:
        find_katakana_patients()
    except Exception as e:
        print(f"⚠ カタカナ抽出でエラー: {e}")


def run_show_headers():
    print("\n--- ③ 各テーブルの最新Parquetヘッダー確認 ---")
    try:
        show_latest_headers()
    except Exception as e:
        print(f"⚠ ヘッダー確認でエラー: {e}")


def run_export_unique_patients():
    print("\n--- ④ 一意の患者リストを出力（CSV/Parquet） ---")
    try:
        export_unique_patients()
    except Exception as e:
        print(f"⚠ 一意患者リスト出力でエラー: {e}")


def run_export_unique_karte_core():
    print("\n--- ⑤ カルテID×患者番号で一意化したリストを出力（CSV/Parquet） ---")
    try:
        export_unique_karte_core()
    except Exception as e:
        print(f"⚠ karte一意リスト出力でエラー: {e}")


def run_join_procedure_with_patients():
    print("\n--- ⑥ procedure に患者番号・氏名を付与して出力（CSV/Parquet） ---")
    try:
        join_procedure_with_patients()
    except Exception as e:
        print(f"⚠ procedure結合出力でエラー: {e}")


def run_analyze_procedure_data():
    print("\n--- ⑦ procedure集計データを確認（ヘッダーと件数のみ表示） ---")
    try:
        analyze_procedure_data()
    except Exception as e:
        print(f"⚠ procedure解析でエラー: {e}")


# --- メイン実行 ---
def run_post_analyses():
    """結合完了後に行う分析をまとめて実行"""
    run_duplicate_check()
    run_katakana_check()
    run_show_headers()
    run_export_unique_patients()
    run_export_unique_karte_core()
    run_join_procedure_with_patients()
    run_analyze_procedure_data()  # ★ 最後に追加（シンプル化）


def main():
    start = datetime.now()
    print("=== データ結合を開始します ===")

    # business_report_YYYYMMDD_YYYYMMDD を走査して各CSVを結合
    dfs = load_all_periods()

    # 結合結果を output/ に出力
    save_outputs(dfs)

    # 結合直後に一連の分析を実行
    run_post_analyses()

    dur = (datetime.now() - start).total_seconds()
    print(f"\n=== 結合 + 分析 完了 ({dur:.1f}s) ===")


if __name__ == "__main__":
    main()
