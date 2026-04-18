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

# 전략 파라미터
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

# OTE 피보나치 기준
OTE_LOW = 0.618
OTE_HIGH = 0.79

# MSS 확인용 봉 수
MSS_BARS = 10

# Order Block 확인용 봉 수
OB_BARS = 20

# 거래량 평균 기준 배수
VOL_MULT = 1.2

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
    tf = signal.get("timeframe", "1h/5m")
    sym = signal["symbol"]
    direct = signal["direction"]
    entry = fmt(signal["entry"])
    sl = fmt(signal["stop_loss"])
    tp1 = fmt(signal["take_profit1"])
    tp2 = fmt(signal["take_profit2"])
    be = fmt(signal["be_target"])
    slp = str(signal["sl_pct"])
    tp1p = str(signal["tp1_pct"])
    tp2p = str(signal["tp2_pct"])
    ts = signal["time"]

    if is_result:
        status = signal["status"]
        icons = {"WIN": "🏆", "LOSS": "💀", "EXPIRED": "⏰", "TP1": "🎯"}
        icon = icons.get(status, "❓")
        pct = signal.get("result_pct")
        rp = fmt(signal.get("result_price"))
        rt = str(signal.get("result_time"))
        pct_str = ""
        if pct is not None:
            sign = "+" if pct >= 0 else ""
            pct_str = f"\n손익: `{sign}{pct}%`"
        msg = (f"{icon} *결과: {sym}*\n"
               f"방향: {direct} | 결과: *{status}*\n"
               f"진입: `{entry}` → `{rp}`"
               f"{pct_str}\n{rt}")
    else:
        icon = "🚀" if direct == "LONG" else "🎯"
        dirlbl = "🟢 LONG" if direct == "LONG" else "🔴 SHORT"
        qv = signal.get("quote_volume", 0)
        qv_str = f"{qv/1e6:.1f}M" if qv >= 1e6 else f"{qv/1e3:.0f}K"
        msg = (f"{icon} *{sym}* {dirlbl}\n"
               f"진입: `{entry}`\n"
               f"손절: `{sl}` (-{slp}%)\n"
               f"TP1: `{tp1}` (+{tp1p}%) RR 1:2\n"
               f"TP2: `{tp2}` (+{tp2p}%)\n"
               f"본절: `{be}`\n"
               f"거래대금: {qv_str}\n{ts}")
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


def fetch_ohlcv(symbol, interval="1h", limit=300):
    r = requests.get(f"{BASE_URL}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_last_prices(symbols):
    r = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
    r.raise_for_status()
    want = set(symbols)
    return {row["symbol"]: float(row["price"]) for row in r.json() if row.get("symbol") in want}


def to_df(raw):
    df = pd.DataFrame(
        [[x[0],float(x[1]),float(x[2]),float(x[3]),float(x[4]),float(x[5])] for x in raw],
        columns=["ts","open","high","low","close","vol"])
    return df


def calc_ema(df, period):
    return df["close"].ewm(span=period, adjust=False).mean()


def find_fvg(df, direction="bull"):
    fvgs = []
    for i in range(2, len(df)):
        if direction == "bull":
            gap_low = df["low"].iloc[i]
            gap_high = df["high"].iloc[i-2]
            if gap_low > gap_high:
                fvgs.append({"low": gap_high, "high": gap_low, "idx": i})
        else:
            gap_high = df["high"].iloc[i]
            gap_low = df["low"].iloc[i-2]
            if gap_high < gap_low:
                fvgs.append({"low": gap_high, "high": gap_low, "idx": i})
    return fvgs


def find_swing_points(df, bars=10):
    highs = []
    lows = []
    for i in range(bars, len(df)-bars):
        if df["high"].iloc[i] == df["high"].iloc[i-bars:i+bars+1].max():
            highs.append((i, float(df["high"].iloc[i])))
        if df["low"].iloc[i] == df["low"].iloc[i-bars:i+bars+1].min():
            lows.append((i, float(df["low"].iloc[i])))
    return highs, lows


def check_ote(df, swing_low, swing_high, direction="bull"):
    price = float(df["close"].iloc[-1])
    if direction == "bull":
        rng = swing_high - swing_low
        ote_low = swing_high - rng * OTE_HIGH
        ote_high = swing_high - rng * OTE_LOW
        return ote_low <= price <= ote_high
    else:
        rng = swing_high - swing_low
        ote_low = swing_low + rng * OTE_LOW
        ote_high = swing_low + rng * OTE_HIGH
        return ote_low <= price <= ote_high


def check_mss(df, direction="bull", bars=MSS_BARS):
    if len(df) < bars + 2:
        return False
    recent = df.iloc[-bars:]
    if direction == "bull":
        prev_high = float(recent["high"].iloc[:-2].max())
        last_high = float(df["high"].iloc[-1])
        return last_high > prev_high
    else:
        prev_low = float(recent["low"].iloc[:-2].min())
        last_low = float(df["low"].iloc[-1])
        return last_low < prev_low


def find_order_block(df, direction="bull", bars=OB_BARS):
    recent = df.iloc[-bars:]
    if direction == "bull":
        idx = recent["low"].idxmin()
        ob_high = float(df.loc[idx, "high"])
        ob_low = float(df.loc[idx, "low"])
    else:
        idx = recent["high"].idxmax()
        ob_high = float(df.loc[idx, "high"])
        ob_low = float(df.loc[idx, "low"])
    return {"high": ob_high, "low": ob_low}


def check_liquidity_sweep(df, direction="bull", bars=20):
    if len(df) < bars + 2:
        return False
    prev = df.iloc[-bars:-2]
    last = df.iloc[-1]
    if direction == "bull":
        prev_low = float(prev["low"].min())
        return float(last["low"]) < prev_low and float(last["close"]) > prev_low
    else:
        prev_high = float(prev["high"].max())
        return float(last["high"]) > prev_high and float(last["close"]) < prev_high


def check_volume(df, mult=VOL_MULT, bars=20):
    if len(df) < bars + 1:
        return False
    avg_vol = float(df["vol"].iloc[-bars-1:-1].mean())
    curr_vol = float(df["vol"].iloc[-1])
    return curr_vol >= avg_vol * mult

def scan_symbol(symbol, symbol_vol=0):
    try:
        # 1h 데이터
        raw_1h = fetch_ohlcv(symbol, "1h", 300)
        if len(raw_1h) < 250: return None
        df_1h = to_df(raw_1h)

        # 1h 추세 확인
        ema200 = calc_ema(df_1h, EMA_PERIOD)
        price_1h = float(df_1h["close"].iloc[-1])
        trend = "LONG" if price_1h > float(ema200.iloc[-1]) else "SHORT"

        # 1h FVG 찾기
        fvgs = find_fvg(df_1h, "bull" if trend == "LONG" else "bear")
        if not fvgs: return None

        # 가장 최근 FVG
        fvg = fvgs[-1]
        fvg_low = fvg["low"]
        fvg_high = fvg["high"]

        # 가격이 FVG 구간 안에 있는지 확인
        if not (fvg_low <= price_1h <= fvg_high):
            return None

        # 1h 스윙 포인트
        swing_highs, swing_lows = find_swing_points(df_1h, bars=5)
        if not swing_highs or not swing_lows: return None

        last_swing_high = swing_highs[-1][1]
        last_swing_low = swing_lows[-1][1]

        # TP2 = 다음 스윙 기준
        if trend == "LONG":
            tp2_price = last_swing_high
        else:
            tp2_price = last_swing_low

        # 5m 데이터
        raw_5m = fetch_ohlcv(symbol, "5m", 300)
        if len(raw_5m) < 100: return None
        df_5m = to_df(raw_5m)

        price = float(df_5m["close"].iloc[-1])
        direction = trend

        # ICT 조건 체크
        conditions = []

        # OTE 확인
        ote_ok = check_ote(df_5m, last_swing_low, last_swing_high, "bull" if direction == "LONG" else "bear")
        if ote_ok: conditions.append("OTE")

        # MSS 확인
        mss_ok = check_mss(df_5m, "bull" if direction == "LONG" else "bear")
        if mss_ok: conditions.append("MSS")

        # Order Block 확인
        ob = find_order_block(df_5m, "bull" if direction == "LONG" else "bear")
        ob_ok = ob["low"] <= price <= ob["high"]
        if ob_ok: conditions.append("OB")

        # Liquidity Sweep 확인
        liq_ok = check_liquidity_sweep(df_5m, "bull" if direction == "LONG" else "bear")
        if liq_ok: conditions.append("LiqSweep")

        # 거래량 확인
        vol_ok = check_volume(df_5m)
        if vol_ok: conditions.append("Vol")

        # 최소 3개 조건 충족 필요
        if len(conditions) < 3: return None

        # SL 설정
        atr_vals = df_5m["high"] - df_5m["low"]
        atr = float(atr_vals.iloc[-14:].mean())

        if direction == "LONG":
            sl = fvg_low - atr * 0.5
        else:
            sl = fvg_high + atr * 0.5

        dist = abs(price - sl)
        if dist <= 0: return None

        # TP1 = RR 1:2
        tp1 = price + dist * RR_TP1 if direction == "LONG" else price - dist * RR_TP1

        # TP2 = 스윙 기준
        if direction == "LONG":
            tp2 = tp2_price if tp2_price > tp1 else tp1 * 1.01
        else:
            tp2 = tp2_price if tp2_price < tp1 else tp1 * 0.99

        be = price + dist * BE_TRIGGER if direction == "LONG" else price - dist * BE_TRIGGER

        sl_pct = round(abs(price - sl) / price * 100, 2)
        tp1_pct = round(abs(tp1 - price) / price * 100, 2)
        tp2_pct = round(abs(tp2 - price) / price * 100, 2)

        return {
            "id": symbol + "_1h5m_" + str(int(time.time())),
            "symbol": symbol_to_display(symbol),
            "raw_symbol": symbol,
            "timeframe": "1h/5m",
            "direction": direction,
            "entry": round(price, 6),
            "stop_loss": round(sl, 6),
            "take_profit1": round(tp1, 6),
            "take_profit2": round(tp2, 6),
            "take_profit": round(tp1, 6),
            "be_target": round(be, 6),
            "sl_pct": sl_pct,
            "tp1_pct": tp1_pct,
            "tp2_pct": tp2_pct,
            "tp_pct": tp1_pct,
            "atr": round(atr, 6),
            "rr": RR_TP1,
            "quote_volume": symbol_vol,
            "conditions": conditions,
            "fvg_low": round(fvg_low, 6),
            "fvg_high": round(fvg_high, 6),
            "status": "OPEN",
            "result_price": None,
            "result_pct": None,
            "result_time": None,
            "time": utc_now_str(),
        }
    except Exception as e:
        logger.warning(f"{symbol} 스캔 실패: {e}")
        return None


def resolve_open_signals(signals):
    now = datetime.now(timezone.utc)
    open_symbols = list({
        s.get("raw_symbol", s["symbol"].replace("/", ""))
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
        raw_symbol = sig.get("raw_symbol", sig["symbol"].replace("/", ""))
        curr = prices.get(raw_symbol)
        if curr is None:
            continue
        try:
            sig_time = datetime.strptime(sig["time"], "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
            elapsed = (now - sig_time).total_seconds() / 3600
        except:
            elapsed = 0
        result = None
        tp1 = sig.get("take_profit1", sig.get("take_profit"))
        tp2 = sig.get("take_profit2", sig.get("take_profit"))
        if sig["direction"] == "LONG":
            if curr >= tp2: result = "WIN"
            elif curr >= tp1: result = "TP1"
            elif curr <= sig["stop_loss"]: result = "LOSS"
            elif elapsed >= EXPIRE_HOURS: result = "EXPIRED"
        else:
            if curr <= tp2: result = "WIN"
            elif curr <= tp1: result = "TP1"
            elif curr >= sig["stop_loss"]: result = "LOSS"
            elif elapsed >= EXPIRE_HOURS: result = "EXPIRED"
        if result:
            entry = sig["entry"]
            if sig["direction"] == "LONG":
                result_pct = round((curr - entry) / entry * 100, 2)
            else:
                result_pct = round((entry - curr) / entry * 100, 2)
            sig["status"] = result
            sig["result_price"] = round(curr, 6)
            sig["result_pct"] = result_pct
            sig["result_time"] = utc_now_str()
            resolved.append(sig)
    return resolved


def main():
    try:
        logger.info("===== Kronos ICT 스캔 시작 =====")
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
            (s.get("raw_symbol", s["symbol"].replace("/", "")), s["direction"], s["timeframe"])
            for s in signals if s.get("status") == "OPEN"
        }

        logger.info(f"거래대금 상위 {TOP_N}개 바이낸스 현물 스캔 중 (1h FVG + 5m ICT)...")
        symbol_vol = get_top_symbols()
        symbols = list(symbol_vol.keys())

        all_signals = []
        for sym in symbols:
            result = scan_symbol(sym, symbol_vol.get(sym, 0))
            if result and (result["raw_symbol"], result["direction"], result["timeframe"]) not in open_set:
                all_signals.append(result)
                open_set.add((result["raw_symbol"], result["direction"], result["timeframe"]))
                logger.info(f"{result['symbol']} {result['direction']} 조건:{result['conditions']}")

        all_signals.sort(key=lambda x: x.get("quote_volume", 0), reverse=True)
        top_signals = all_signals[:TOP_SIGNAL_N]
        rest_signals = all_signals[TOP_SIGNAL_N:]

        logger.info(f"전체 시그널 {len(all_signals)}건 → 상위 {len(top_signals)}건 텔레그램 전송")

        for sig in top_signals:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)
            logger.info(f"[전송] {sig['symbol']} {sig['direction']} 거래대금:{sig.get('quote_volume',0)/1e6:.1f}M")

        for sig in rest_signals:
            logger.info(f"[저장만] {sig['symbol']} {sig['direction']}")

        signals.extend(all_signals)
        save_signals(signals)
        logger.info("===== Kronos ICT 스캔 완료 =====")

    except Exception as e:
        import traceback
        err = traceback.format_exc()
        logger.error(f"치명적 오류 발생:\n{err}")
        send_error_alert(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, err)


if __name__ == "__main__":
    main()
