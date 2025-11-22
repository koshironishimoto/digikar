from datetime import datetime
from pathlib import Path
import pandas as pd


def extract_free_comments_from_file(
    uke_path: Path,
    insurer_type: str,
    receipt_month: str
) -> list[dict]:
    """
    RECEIPTC.UKE をパースして、CO(810000001) と
    その直上にある SI コード（9桁数字）を抽出する。
    """
    results: list[dict] = []

    current_re_id = None
    current_date = None
    last_si_code: str | None = None  # ★ 直近のSIコード（最新1つだけ保持）

    with open(uke_path, "r", encoding="cp932", errors="replace") as f:
        for raw in f:
            line = raw.rstrip()
            if not line:
                continue

            cols = line.split(",")
            tag = cols[0]

            # --- RE：患者ID ---
            if tag == "RE":
                if len(cols) > 13 and cols[13]:
                    current_re_id = cols[13]
                else:
                    current_re_id = cols[1] if len(cols) > 1 else None

                current_date = None
                last_si_code = None  # ★ RE が変われば SI リセット

            # --- SY：診療日 ---
            elif tag == "SY":
                if len(cols) > 2 and cols[2]:
                    current_date = cols[2]

                last_si_code = None  # ★ SY が変われば SI もリセット

            # --- SI：行為コード ---
            elif tag == "SI":
                if len(cols) > 3 and cols[3]:
                    si_code = cols[3].strip()

                    # ★ 9桁数字だけを対象
                    if len(si_code) == 9 and si_code.isdigit():
                        last_si_code = si_code  # ★ 直近のSIだけ保持

            # --- CO：フリーコメント（810000001） ---
            elif tag == "CO":
                if len(cols) > 3 and cols[3] == "810000001":
                    comment_text = cols[4] if len(cols) > 4 else ""

                    if current_re_id and current_date:
                        results.append({
                            "receipt_month": receipt_month,
                            "insurer_type": insurer_type,
                            "patient_id": current_re_id,
                            "comment_date": current_date,
                            "free_comment": comment_text,
                            "si_code": last_si_code or ""  # ★ 直前のSI1つだけ
                        })

    return results


def extract_all_receipts(base_path: str = "../data") -> pd.DataFrame:
    base = Path(base_path)

    receipt_dirs = sorted(
        [p for p in base.iterdir() if p.is_dir() and p.name.startswith("receipt_")],
        key=lambda p: p.name
    )

    all_results: list[dict] = []
    TARGET_INSURERS = ["kokuho", "shaho"]

    for receipt_dir in receipt_dirs:
        receipt_month = receipt_dir.name.replace("receipt_", "")
        print(f"\n===== 処理中: {receipt_dir} ({receipt_month}) =====")

        for insurer in TARGET_INSURERS:
            uke_path = receipt_dir / insurer / "RECEIPTC.UKE"

            if not uke_path.exists():
                print(f"  ⚠ {uke_path} が存在しません。スキップ")
                continue

            print(f"  → 読み込み: {uke_path.name} ({insurer})")

            recs = extract_free_comments_from_file(
                uke_path=uke_path,
                insurer_type=insurer,
                receipt_month=receipt_month
            )
            all_results.extend(recs)

    if not all_results:
        print("⚠ 1件もフリーコメントが見つかりませんでした。")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    df["comment_date_ymd"] = pd.to_datetime(df["comment_date"], format="%Y%m%d", errors="coerce")
    return df


def export_comment_results(
    df: pd.DataFrame,
    output_dir: str = "output",
    prefix: str = "receipt_free_comments_all"
):
    output_base = Path(output_dir)
    output_base.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_path = output_base / f"{prefix}_{ts}.csv"
    parquet_path = output_base / f"{prefix}_{ts}.parquet"

    df.to_csv(csv_path, index=False, encoding="cp932")
    print(f"📤 CSV 出力: {csv_path}")

    try:
        df.to_parquet(parquet_path, index=False)
        print(f"📤 Parquet 出力: {parquet_path}")
    except Exception as e:
        print(f"⚠ Parquet 出力失敗: {e}")


def run_extract_free_comments(
    base_path: str = "../data",
    output_dir: str = "output",
):
    print("\n--- ⑨ レセプト・フリーコメント抽出（receipt_* 全フォルダ / kokuho+shahoのみ） ---")

    df_all = extract_all_receipts(base_path)

    if df_all.empty:
        print("⚠ フリーコメントが0件でした。スキップ")
        return

    export_comment_results(df_all, output_dir=output_dir)


if __name__ == "__main__":
    print("\n=== 🧾 レセプト・フリーコメント抽出 単体実行 ===")
    df_all = extract_all_receipts("../data")
    if not df_all.empty:
        print("\n--- 先頭5行 ---")
        print(df_all.head())
        export_comment_results(df_all)
    print("\n=== ✅ 完了 ===")
