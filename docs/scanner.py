#!/usr/bin/env python3
"""
Kronos ICT Scanner v4
"""

import copy
import html
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

import pandas as pd
import requests

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

EMA_PERIOD = 200
ATR_PERIOD = 14
RR_TP1 = 2.0
BE_TRIGGER = 1.0
EXPIRE_HOURS = 48

TOP_N = 100
TOP_SIGNAL_N = 10

SIGNALS_FILE = "docs/signals.json"
BACKUP_DIR = "docs/backups"
LOG_FILE = "logs/scanner.log"
BASE_URL = "https://api.binance.com"

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "DAI", "USDP", "USDE", "UST", "USTC",
    "AEUR", "XUSD",
}

OTE_LOW = 0.618
OTE_HIGH = 0.79

SWING_BARS_1H = 5
SWING_BARS_5M = 5
MSS_BARS = 10
OB_BARS = 20
VOL_MULT = 1.2

os.makedirs("logs", exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

logger = logging.getLogger("kronos")
logger.setLevel(logging.INFO)

fh = RotatingFileHandler(
    LOG_FILE,
    maxBytes=2 * 1024 * 1024,
    backupCount=3,
    encoding="utf-8",
)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(ch)


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def display_to_raw_symbol(symbol_display: str) -> str:
    return symbol_display.replace("/", "")


def load_signals():
    if not os.path.exists(SIGNALS_FILE):
        return []
    try:
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"signals.json 파싱 실패, 빈 리스트로 초기화: {e}")
        return []


def save_signals(signals):
    os.makedirs(os.path.dirname(SIGNALS_FILE), exist_ok=True)
    dir_name = os.path.dirname(SIGNALS_FILE) or "."
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            dir=dir_name,
            delete=False,
            suffix=".tmp",
            encoding="utf-8",
        ) as tmp:
            json.dump(signals[-1000:], tmp, ensure_ascii=False, indent=2)
            tmp_path = tmp.name
        os.replace(tmp_path, SIGNALS_FILE)
    except Exception as e:
        logger.error(f"signals 저장 실패: {e}")
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass


def backup_signals():
    if not os.path.exists(SIGNALS_FILE):
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    dst = f"{BACKUP_DIR}/signals_{ts}.json"
    try:
        shutil.copy2(SIGNALS_FILE, dst)
        files = sorted(
            f for f in os.listdir(BACKUP_DIR) if f.startswith("signals_")
        )
        while len(files) > 10:
            os.remove(os.path.join(BACKUP_DIR, files.pop(0)))
        logger.info(f"백업 완료: {dst}")
    except Exception as e:
        logger.error(f"백업 실패: {e}")


def fmt(v) -> str:
    if v is None:
        return "-"
    s = f"{float(v):.10f}".rstrip("0").rstrip(".")
    return s


def send_error_alert(token: str, chat_id: str, error_msg: str):
    if not token or not chat_id:
        return
    msg = (
        f"<b>Kronos 오류</b>\n"
        f"<pre>{html.escape(error_msg[:300])}</pre>\n"
        f"{html.escape(utc_now_str())}"
    )
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"오류 알람 전송 실패: {e}")


def send_telegram(token: str, chat_id: str, signal: dict, is_result: bool = False):
    if not token or not chat_id:
        return

    sym = html.escape(signal["symbol"])
    direct = html.escape(signal["direction"])
    entry = html.escape(fmt(signal["entry"]))
    sl = html.escape(fmt(signal["stop_loss"]))
    tp1 = html.escape(fmt(signal["take_profit1"]))
    tp2 = html.escape(fmt(signal["take_profit2"]))
    be = html.escape(fmt(signal["be_target"]))
    slp = html.escape(str(signal["sl_pct"]))
    tp1p = html.escape(str(signal["tp1_pct"]))
    tp2p = html.escape(str(signal["tp2_pct"]))
    ts = html.escape(signal["time"])

    if is_result:
        status = signal["status"]
        icons = {"WIN": "🏆", "LOSS": "💀", "EXPIRED": "⏰", "TP1": "🎯"}
        icon = icons.get(status, "❓")
        status_esc = html.escape(status)
        pct = signal.get("result_pct")
        rp = html.escape(fmt(signal.get("result_price")))
        rt = html.escape(str(signal.get("result_time", "")))
        pct_str = ""
        if pct is not None:
            sign = "+" if pct >= 0 else ""
            pct_str = f"\n손익: <code>{html.escape(f'{sign}{pct}%')}</code>"
        msg = (
            f"{icon} <b>결과: {sym}</b>\n"
            f"방향: {direct} | 결과: <b>{status_esc}</b>\n"
            f"진입: <code>{entry}</code> → <code>{rp}</code>"
            f"{pct_str}\n{rt}"
        )
    else:
        icon = "🚀" if signal["direction"] == "LONG" else "🎯"
        dirlbl = "🟢 LONG" if signal["direction"] == "LONG" else "🔴 SHORT"
        qv = signal.get("quote_volume", 0)
        qv_str = html.escape(
            f"{qv / 1e6:.1f}M" if qv >= 1e6 else f"{qv / 1e3:.0f}K"
        )
        msg = (
            f"{icon} <b>{sym}</b> {dirlbl}\n"
            f"진입: <code>{entry}</code>\n"
            f"손절: <code>{sl}</code> (-{slp}%)\n"
            f"TP1: <code>{tp1}</code> (+{tp1p}%) RR 1:2\n"
            f"TP2: <code>{tp2}</code> (+{tp2p}%)\n"
            f"본절: <code>{be}</code>\n"
            f"거래대금: {qv_str}\n{ts}"
        )
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


def get_top_symbols(top_n: int = TOP_N) -> dict:
    MIN_QV_USD = 20_000_000
    HARD_CAP = 10

    info_r = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=20)
    info_r.raise_for_status()
    info = info_r.json()

    tick_r = requests.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=20)
    tick_r.raise_for_status()
    tickers = tick_r.json()

    allowed: set[str] = set()
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("isSpotTradingAllowed") is not True:
            continue
        if s.get("baseAsset", "") in STABLE_BASES:
            continue
        allowed.add(s["symbol"])

    rows = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in allowed:
            continue
        try:
            qv = float(t.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        if qv >= MIN_QV_USD:
            rows.append((sym, qv))

    rows.sort(key=lambda x: x[1], reverse=True)
    limit = min(HARD_CAP, top_n)
    return {s: qv for s, qv in rows[:limit]}


def symbol_to_display(symbol: str) -> str:
    return symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol


def fetch_ohlcv(symbol: str, interval: str = "1h",
