# apps/find_katakana_patients.py
import re
import pandas as pd
from apps.utils.common import get_latest_parquet

# --------------------------------------------------
# カタカナ判定（全角カタカナ・長音・スペースを許可）
# --------------------------------------------------
KATAKANA_PATTERN = re.compile(r'^[\u30A0-\u30FFー\s]+$')

def is_katakana_only(name: str) -> bool:
    """氏名がカタカナのみかどうか判定"""
    if not isinstance(name, str):
        return False
    name = name.strip()
    return bool(KATAKANA_PATTERN.fullmatch(name))


def find_katakana_patients():
    """最新の karte_ ファイルからカタカナ氏名の患者を抽出"""
    targets = get_latest_parquet(["karte_"])
    if not targets:
        print("⚠ karte_ の Parquetファイルが見つかりません。")
        return

    karte_path = targets[0]
    print(f"📂 対象ファイル: {karte_path.name}")

    df = pd.read_parquet(karte_path)
    if "患者氏名" not in df.columns:
        print("⚠ '患者氏名' 列が見つかりません。")
        return

    # カタカナ氏名の抽出
    df_kata = df[df["患者氏名"].apply(is_katakana_only)]

    if df_kata.empty:
        print("✅ カタカナ氏名のみの患者は見つかりませんでした。")
        return

    print(f"\n🧾 カタカナ氏名のみの患者 ({len(df_kata)} 件):")
    for _, row in df_kata.iterrows():
        print(f"  - {row['患者番号']} : {row['患者氏名']}")


if __name__ == "__main__":
    print("=== カタカナ氏名の患者を抽出 ===")
    find_katakana_patients()
