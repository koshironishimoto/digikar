# apps/join_procedure_with_patients.py
from pathlib import Path
import re
import pandas as pd
from apps.utils.common import OUTPUT_DIR

# 正規表現で「素の procedure」「unique_karte_core」だけを厳密に拾う
RE_PROCEDURE_BASE = re.compile(r"^procedure_\d{8}_\d{6}\.parquet$")
RE_UNIQUE_KARTE_CORE = re.compile(r"^unique_karte_core_\d{6}\.parquet$")

def _pick_latest_by_regex(regex: re.Pattern) -> Path | None:
    files = sorted(OUTPUT_DIR.glob("*.parquet"))
    matched = [f for f in files if regex.match(f.name)]
    return matched[-1] if matched else None

def join_procedure_with_patients() -> None:
    # ← ここで with_patient を除外した「素の」procedure_* を取得
    proc_pq = _pick_latest_by_regex(RE_PROCEDURE_BASE)
    core_pq = _pick_latest_by_regex(RE_UNIQUE_KARTE_CORE)

    if proc_pq is None:
        print("⚠ procedure_* の Parquet が見つかりません。")
        return
    if core_pq is None:
        print("⚠ unique_karte_core_* の Parquet が見つかりません。")
        return

    print(f"📂 procedure: {proc_pq.name}")
    print(f"📂 unique_karte_core: {core_pq.name}")

    proc = pd.read_parquet(proc_pq)
    core = pd.read_parquet(core_pq)[["カルテID", "患者番号", "患者氏名"]].drop_duplicates()

    # 左結合で患者番号・患者氏名を付与
    df = core.merge(proc, on="カルテID", how="right")

    # 患者番号を数値として扱う（欠損はNA）
    if "患者番号" in df.columns:
        df["患者番号"] = pd.to_numeric(df["患者番号"], errors="coerce").astype("Int64")

    # 並び替え（患者番号 昇順 → 日付 昇順）
    sort_cols = []
    if "患者番号" in df.columns:
        sort_cols.append("患者番号")
    if "日付" in df.columns:
        sort_cols.append("日付")
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last").reset_index(drop=True)

    # 列順（患者情報を先頭へ）
    front = [c for c in ["患者番号", "患者氏名"] if c in df.columns]
    cols = front + [c for c in df.columns if c not in front]
    df = df[cols]

    # 出力ファイル名は procedure_* の時刻部分を流用
    # 例: procedure_20251102_110939.parquet → 110939
    ts = proc_pq.stem.split("_")[-1]
    csv_out = OUTPUT_DIR / f"procedure_with_patient_{ts}.csv"
    pq_out = OUTPUT_DIR / f"procedure_with_patient_{ts}.parquet"

    df.to_csv(csv_out, index=False, encoding="utf-8-sig")
    df.to_parquet(pq_out, index=False)

    print(f"✅ 出力しました: {csv_out.name}")
    print(f"✅ 出力しました: {pq_out.name}")
    print(f"📊 レコード数: {len(df):,}")
