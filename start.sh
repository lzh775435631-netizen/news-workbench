#!/bin/bash
# 新聞情報工作台 · 啟動腳本
# 依賴（已安裝於 WorkBuddy 受管 venv）：feedparser, requests, jieba
cd "$(dirname "$0")"
PY=/Users/alic1688e/.workbuddy/binaries/python/envs/default/bin/python3
PORT="${PORT:-8800}"
echo "啟動新聞情報工作台 -> http://localhost:${PORT}"
"$PY" app.py
