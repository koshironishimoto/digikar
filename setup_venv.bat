@echo off
REM --- Python仮想環境セットアップスクリプト ---
python -m venv venv
call venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
pause
