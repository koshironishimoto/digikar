# data_anal

## 概要
このアプリケーションは、`data/` フォルダ以下にある  
`business_report_YYYYMMDD_YYYYMMDD` 形式のフォルダから  
`処置行為集計.csv` ファイルを自動で統合する Python スクリプトです。

出力結果は `output/` フォルダに保存され、  
CSV と Parquet の両方の形式で出力されます。

---

## ディレクトリ構成
data_anal/
├─ apps/
│  ├─ merge_data.py
│  ├─ find_duplicate_patients.py
│  ├─ find_katakana_patients.py
│  ├─ export_unique_patients.py
│  ├─ inspect_headers.py
│  └─ utils/
│     └─ common.py
├─ output/                ← Git管理外
├─ data/                  ← Git管理外
├─ main.py                ← 結合＋分析の統合エントリポイント
├─ requirements.txt
├─ setup_venv.bat
└─ .gitignore


---

## セットアップ手順

1. 仮想環境の作成と有効化
   ```bash
   python -m venv venv
   .\venv\Scripts\activate

2. 依存パッケージのインストール
pip install -r requirements.txt

3. 実行
python main.py

## 注意事項
data/ フォルダおよび output/ フォルダは .gitignore により Git 管理外です。
個人情報・機密データは GitHub にアップロードされません。

Python 3.12 以上を推奨。

---

このファイルをプロジェクト直下（`data_anal/`）に保存すればOKです。  

次のステップとして：
```bash
git add README.md
git commit -m "Add README.md with project description"
git push

## FutureNet
必要なファイル
#1 18透析回数
#2 31透析記録
#3 32バイタル
#4 33愁訴処置

