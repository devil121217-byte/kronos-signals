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
    msg = f"Kronos 오류\n{error_msg[:300]}\n{utc_now_str()}"
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"오류 알람 전송 실패: {e}")

def send_telegram(token, chat_id, signal, is_result=False):
    if not token or not chat_id:
        return
    tf = signal.get("timeframe", "1h")
    sym = signal["symbol"]
    direct = signal["direction"]
    entry = fmt(signal["entry"])
    sl = fmt(signal["stop_loss"])
    tp = fmt(signal["take_profit"])
    be = fmt(signal["be_target"])
    rr = str(signal["rr"])
    slp = str(signal["sl_pct"])
    tpp = str(signal["tp_pct"])
    ts = signal["time"]
    if is_result:
        status = signal["status"]
        icons = {"WIN": "🏆", "LOSS": "💀", "EXPIRED": "⏰"}
        icon = icons.get(status, "❓")
        pct = signal.get("result_pct")
        rp = fmt(signal.get("result_price"))
        rt = str(signal.get("result_time"))
        pct_str = ""
        if pct is not None:
            sign = "+" if pct >= 0 else ""
            pct_str = f"\n손익: `{sign}{pct}%`"
        msg = (f"{icon} *결과 확정: {sym}* [{tf}]\n"
               f"방향: {direct}\n결과: *{status}*\n"
               f"진입가: `{entry}`\n종료가: `{rp}`{pct_str}\n{rt}")
    else:
        icon = "🚀" if direct == "LONG" else "🎯"
        dirlbl = "🟢 LONG" if direct == "LONG" else "🔴 SHORT"
        qv = signal.get("quote_volume", 0)
        qv_str = f"{qv/1e6:.1f}M" if qv >= 1e6 else f"{qv/1e3:.0f}K"
        msg = (f"{icon} *{sym}* [{tf}] {dirlbl}\n"
               f"진입: `{entry}`\n손절: `{sl}` (-{slp}%)\n"
               f"목표: `{tp}` (+{tpp}%)\n본절: `{be}`\n"
               f"RR: 1:{rr} | 거래대금: {qv_str}\n{ts}")
    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")

def get_top_symbols(top_n=TOP_N):
    info = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=20).json()
    tickers = requests.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=20).json()
    allowed = set()
    for s in info["symbols"]:
        if s.get("status") != "TRADING": continue
        if s.get("quoteAsset") != "USDT": continue
        if s.get("isSpotTradingAllowed") is not True: continue
        if s.get("baseAsset", "") in ["BTC", "USDC", "FDUSD", "TUSD", "USDT"]: continue
        allowed.add(s["symbol"])
    rows = []
    for t in tickers:
        sym = t.get("symbol")
        if sym not in allowed: continue
        try:
            qv = float(t.get("quoteVolume", 0))
            if qv > 0: rows.append((sym, qv))
        except: continue
    rows.sort(key=lambda x: x[1], reverse=True)
    return {s: qv for s, qv in rows[:top_n]}

def symbol_to_display(symbol):
    return symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol

def fetch_ohlcv(symbol, interval="1h", limit=250):
    r = requests.get(f"{BASE_URL}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_last_prices(symbols):
    r = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
    r.raise_for_status()
    want = set(symbols)
    return {row["symbol"]: float(row["price"]) for row in r.json() if row.get("symbol") in want}

def calculate_indicators(df):
    df = df.copy()
    df["s_high"] = df["high"].shift(1).rolling(20).max()
    df["s_low"] = df["low"].shift(1).rolling(20).min()
    df["bull_fvg"] = df["low"] > df["high"].shift(2)
    df["bear_fvg"] = df["high"] < df["low"].shift(2)
    df["ema200"] = df["close"].ewm(span=EMA_PERIOD, adjust=False).mean()
    hl = df["high"] - df["low"]
    hcp = (df["high"] - df["close"].shift(1)).abs()
    lcp = (df["low"] - df["close"].shift(1)).abs()
    df["atr"] = pd.concat([hl, hcp, lcp], axis=1).max(axis=1).rolling(ATR_PERIOD).mean()
    return df

def scan_symbol(symbol, timeframe="1h"):
    try:
        raw = fetch_ohlcv(symbol, timeframe, 250)
        if len(raw) < 220: return None
        df = pd.DataFrame(
            [[x[0],float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5])] for x in raw],
            columns=["ts","open","high","low","close","vol"])
        df = calculate_indicators(df)
        c = df.iloc[-1]
        price = float(c["close"])
        atr = float(c["atr"]) if pd.notna(c["atr"]) else None
        if not atr: return None
        long_sig = price > float(c["s_high"]) + atr*0.1 and bool(c["bull_fvg"]) and price > float(c["ema200"])
        short_sig = price < float(c["s_low"]) - atr*0.1 and bool(c["bear_fvg"]) and price < float(c["ema200"])
        if not (long_sig or short_sig): return None
        direction = "LONG" if long_sig else "SHORT"
        sl = float(c["s_low"]) if long_sig else float(c["s_high"])
        dist = abs(price - sl)
        if dist < atr * 0.2: return None
        tp = price + dist*RR_RATIO if long_sig else price - dist*RR_RATIO
        be = price + dist*BE_TRIGGER if long_sig else price - dist*BE_TRIGGER
        return {
            "id": symbol+"_"+timeframe+"_"+str(int(time.time())),
            "symbol": symbol_to_display(symbol),
            "raw_symbol": symbol,
            "timeframe": timeframe,
            "direction": direction,
            "entry": round(price,6),
            "stop_loss": round(sl,6),
            "take_profit": round(tp,6),
            "be_target": round(be,6),
            "sl_pct": round(abs(price-sl)/price*100,2),
            "tp_pct": round(abs(tp-price)/price*100,2),
            "atr": round(atr,6),
            "rr": RR_RATIO,
            "quote_volume": 0,
            "status": "OPEN",
            "result_price": None,
            "result_pct": None,
            "result_time": None,
            "time": utc_now_str(),
        }
    except Exception as e:
        logger.warning(f"{symbol} [{timeframe}] 스캔 실패: {e}")
        return None

def resolve_open_signals(signals):
    now = datetime.now(timezone.utc)
    open_symbols = list({
        s.get("raw_symbol", s["symbol"].replace("/",""))
        for s in signals if s.get("status") == "OPEN"
    })
    if not open_symbols:
        return []
    try:
        prices = fetch_last_prices(open_symbols)
    except Exception as e:
        logger.error(f"현재가 조회 실패: {e}")
        return []
    resolved = []
    for sig in signals:
        if sig.get("status") != "OPEN":
            continue
        raw_symbol = sig.get("raw_symbol", sig["symbol"].replace("/",""))
        curr = prices.get(raw_symbol)
        if curr is None:
            continue
        try:
            sig_time = datetime.strptime(sig["time"], "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
            elapsed = (now - sig_time).total_seconds() / 3600
        except:
            elapsed = 0
        result = None
        if sig["direction"] == "LONG":
            if curr >= sig["take_profit"]: result = "WIN"
            elif curr <= sig["stop_loss"]: result = "LOSS"
            elif elapsed >= EXPIRE_HOURS: result = "EXPIRED"
        else:
            if curr <= sig["take_profit"]: result = "WIN"
            elif curr >= sig["stop_loss"]: result = "LOSS"
            elif elapsed >= EXPIRE_HOURS: result = "EXPIRED"
        if result:
            entry = sig["entry"]
            result_pct = round((curr-entry)/entry*100,2) if sig["direction"]=="LONG" else round((entry-curr)/entry*100,2)
            sig["status"] = result
            sig["result_price"] = round(curr,6)
            sig["result_pct"] = result_pct
            sig["result_time"] = utc_now_str()
            resolved.append(sig)
    return resolved


def main():
    try:
        logger.info("===== Kronos 스캔 시작 =====")
        backup_signals()
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("TELEGRAM_TOKEN 또는 TELEGRAM_CHAT_ID 가 비어 있습니다.")
        signals = load_signals()
        resolved = resolve_open_signals(signals)
        if resolved:
            logger.info(f"결과 확정: {len(resolved)}건")
            for sig in resolved:
                send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)
                logger.info(f"{sig['symbol']} -> {sig['status']} ({sig.get('result_pct','?')}%)")
        open_set = {
            (s.get("raw_symbol", s["symbol"].replace("/","")), s["direction"], s["timeframe"])
            for s in signals if s.get("status") == "OPEN"
        }
        logger.info(f"거래대금 상위 {TOP_N}개 바이낸스 현물 알트코인 스캔 중...")
        symbol_vol = get_top_symbols()
        symbols = list(symbol_vol.keys())
        all_signals = []
        logger.info("[1h] 스캔 시작...")
        for sym in symbols:
            result = scan_symbol(sym, "1h")
            if result and (result["raw_symbol"], result["direction"], result["timeframe"]) not in open_set:
                result["quote_volume"] = symbol_vol.get(sym, 0)
                all_signals.append(result)
                open_set.add((result["raw_symbol"], result["direction"], result["timeframe"]))

        all_signals.sort(key=lambda x: x.get("quote_volume", 0), reverse=True)
        top_signals = all_signals[:TOP_SIGNAL_N]
        rest_signals = all_signals[TOP_SIGNAL_N:]
        logger.info(f"전체 시그널 {len(all_signals)}건")
        for sig in top_signals:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)
            logger.info(f"[전송] {sig['symbol']} {sig['direction']}")
        for sig in rest_signals:
            logger.info(f"[저장만] {sig['symbol']}")
        signals.extend(all_signals)
        save_signals(signals)
        logger.info("===== Kronos 스캔 완료 =====")
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        logger.error(f"오류: {err}")
        send_error_alert(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, err)


if __name__ == "__main__":
    main()
