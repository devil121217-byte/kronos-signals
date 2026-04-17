import json
import os
import time
import shutil
import logging
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
import pandas as pd
import requests

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
EMA_PERIOD = 200
ATR_PERIOD = 14
RR_RATIO = 3.0
BE_TRIGGER = 1.5
EXPIRE_HOURS = 48
ENABLE_15M = False
TOP_N = 100
TOP_SIGNAL_N = 10
SIGNALS_FILE = "docs/signals.json"
BACKUP_DIR = "docs/backups"
LOG_FILE = "logs/scanner.log"
BASE_URL = "https://api.binance.com"

os.makedirs("logs", exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

logger = logging.getLogger("kronos")
logger.setLevel(logging.INFO)
fh = RotatingFileHandler(LOG_FILE, maxBytes=2*1024*1024, backupCount=3, encoding="utf-8")
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)

def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def load_signals():
    if os.path.exists(SIGNALS_FILE):
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []

def save_signals(signals):
    os.makedirs(os.path.dirname(SIGNALS_FILE), exist_ok=True)
    with open(SIGNALS_FILE, "w", encoding="utf-8") as f:
        json.dump(signals[-1000:], f, ensure_ascii=False, indent=2)

def backup_signals():
    if not os.path.exists(SIGNALS_FILE):
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    dst = f"{BACKUP_DIR}/signals_{ts}.json"
    try:
        shutil.copy2(SIGNALS_FILE, dst)
        files = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith("signals_"))
        while len(files) > 10:
            os.remove(os.path.join(BACKUP_DIR, files.pop(0)))
        logger.info(f"백업 완료: {dst}")
    except Exception as e:
        logger.error(f"백업 실패: {e}")

def fmt(v):
    if v is None: return "-"
    s = f"{float(v):.10f}".rstrip("0").rstrip(".")
    return s

def send_error_alert(token, chat_id, error_msg):
    if not token or not chat_id:
        return
    msg = f"🚨 *Kronos 오류 발생*\n```\n{error_msg[:500]}\n```\n{utc_now_str()}"
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"오류 알람 전송 실패: {e}")
