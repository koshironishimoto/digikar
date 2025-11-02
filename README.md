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
│ └─ merge_data.py ← データ統合処理
├─ main.py ← アプリ実行エントリポイント
├─ data/ ← 元データ格納用（Git管理外）
├─ output/ ← 出力フォルダ（Git管理外）
├─ requirements.txt ← 必要パッケージ一覧
├─ setup_venv.bat ← 仮想環境セットアップ用スクリプト
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


