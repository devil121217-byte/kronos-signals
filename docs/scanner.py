#!/usr/bin/env python3

import json
import os
import time
from datetime import datetime, timezone

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
BASE_URL = "https://api.binance.com"

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "DAI", "USDP", "USDE", "UST", "USTC",
    "AEUR", "XUSD",
}


def utc_now_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fmt(v):
    if v is None:
        return "-"
    s = f"{float(v):.10f}".rstrip("0").rstrip(".")
    return s


def load_signals():
    if not os.path.exists(SIGNALS_FILE):
        return []
    try:
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_signals(signals):
    os.makedirs("docs", exist_ok=True)
    with open(SIGNALS_FILE, "w", encoding="utf-8") as f:
        json.dump(signals[-1000:], f, ensure_ascii=False, indent=2)


def send_telegram(token, chat_id, signal, is_result=False):
    if not token or not chat_id:
        return

    sym = signal["symbol"]
    direct = signal["direction"]
    entry = fmt(signal["entry"])
    sl = fmt(signal["stop_loss"])
    tp1 = fmt(signal["take_profit1"])
    tp2 = fmt(signal["take_profit2"])
    be = fmt(signal["be_target"])
    ts = signal["time"]

    if is_result:
        status = signal["status"]
        rp = fmt(signal.get("result_price"))
        pct = signal.get("result_pct")
        pct_str = f" ({pct}%)" if pct is not None else ""
        msg = (
            f"결과 {sym}\n"
            f"{direct} {status}{pct_str}\n"
            f"진입: {entry}\n"
            f"결과가: {rp}\n"
            f"{ts}"
        )
    else:
        qv = signal.get("quote_volume", 0)
        qv_str = f"{qv / 1e6:.1f}M" if qv >= 1e6 else f"{qv / 1e3:.0f}K"
        msg = (
            f"{sym} {direct}\n"
            f"진입: {entry}\n"
            f"손절: {sl}\n"
            f"TP1: {tp1}\n"
            f"TP2: {tp2}\n"
            f"본절: {be}\n"
            f"거래대금: {qv_str}\n"
            f"{ts}"
        )

    try:
        requests.get(
            f"https://api.telegram.org/bot{token}/sendMessage",
            params={"chat_id": chat_id, "text": msg},
            timeout=10,
        )
    except Exception:
        pass


def symbol_to_display(symbol):
    return symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol


def fetch_ohlcv(symbol, interval="1h", limit=300):
    r = requests.get(
        f"{BASE_URL}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def to_df(raw):
    return pd.DataFrame(
        [
            [x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])]
            for x in raw
        ],
        columns=["ts", "open", "high", "low", "close", "vol"],
    )


def calc_ema(df, period):
    return df["close"].ewm(span=period, adjust=False).mean()


def calc_atr(df, period=ATR_PERIOD):
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return float(tr.iloc[-period:].mean())


def get_top_symbols(top_n=TOP_N):
    MIN_QV_USD = 20_000_000

    info = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=20).json()
    tickers = requests.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=20).json()

    allowed = set()
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
            qv = float(t.get("quoteVolume", 0))
        except Exception:
            continue
        if qv >= MIN_QV_USD:
            rows.append((sym, qv))

    rows.sort(key=lambda x: x[1], reverse=True)
    return {s: qv for s, qv in rows[:top_n]}


def scan_symbol(symbol, quote_volume=0):
    try:
        raw = fetch_ohlcv(symbol, "1h", 300)
        if len(raw) < 250:
            return None

        df = to_df(raw)
        ema200 = calc_ema(df, EMA_PERIOD)
        atr = calc_atr(df, ATR_PERIOD)

        price = float(df["close"].iloc[-1])
        last_high = float(df["high"].iloc[-20:].max())
        last_low = float(df["low"].iloc[-20:].min())

        direction = "LONG" if price > float(ema200.iloc[-1]) else "SHORT"

        if direction == "LONG":
            sl = last_low - atr * 0.5
            dist = price - sl
            if dist <= 0:
                return None
            tp1 = price + dist * RR_RATIO
            tp2 = price + dist * (RR_RATIO + 1)
            be = price + dist * BE_TRIGGER
        else:
            sl = last_high + atr * 0.5
            dist = sl - price
            if dist <= 0:
                return None
            tp1 = price - dist * RR_RATIO
            tp2 = price - dist * (RR_RATIO + 1)
            be = price - dist * BE_TRIGGER

        sl_pct = round(abs(price - sl) / price * 100, 2)
        tp1_pct = round(abs(tp1 - price) / price * 100, 2)
        tp2_pct = round(abs(tp2 - price) / price * 100, 2)

        return {
            "id": f"{symbol}_{int(time.time())}",
            "symbol": symbol_to_display(symbol),
            "raw_symbol": symbol,
            "timeframe": "1h",
            "direction": direction,
            "entry": round(price, 6),
            "stop_loss": round(sl, 6),
            "take_profit1": round(tp1, 6),
            "take_profit2": round(tp2, 6),
            "be_target": round(be, 6),
            "sl_pct": sl_pct,
            "tp1_pct": tp1_pct,
            "tp2_pct": tp2_pct,
            "quote_volume": quote_volume,
            "status": "OPEN",
            "result_price": None,
            "result_pct": None,
            "result_time": None,
            "time": utc_now_str(),
        }
    except Exception:
        return None


def fetch_last_prices(symbols):
    r = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
    r.raise_for_status()
    want = set(symbols)
    return {
        row["symbol"]: float(row["price"])
        for row in r.json()
        if row.get("symbol") in want
    }


def resolve_open_signals(signals):
    now = datetime.now(timezone.utc)

    open_symbols = list(
        {
            s.get("raw_symbol", s["symbol"].replace("/", ""))
            for s in signals
            if s.get("status") == "OPEN"
        }
    )
    if not open_symbols:
        return []

    try:
        prices = fetch_last_prices(open_symbols)
    except Exception:
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
            sig_time = datetime.strptime(
                sig["time"], "%Y-%m-%d %H:%M UTC"
            ).replace(tzinfo=timezone.utc)
            elapsed = (now - sig_time).total_seconds() / 3600
        except Exception:
            elapsed = 0

        result = None
        if sig["direction"] == "LONG":
            if curr >= sig["take_profit2"]:
                result = "WIN"
            elif curr >= sig["take_profit1"]:
                result = "TP1"
            elif curr <= sig["stop_loss"]:
                result = "LOSS"
            elif elapsed >= EXPIRE_HOURS:
                result = "EXPIRED"
        else:
            if curr <= sig["take_profit2"]:
                result = "WIN"
            elif curr <= sig["take_profit1"]:
                result = "TP1"
            elif curr >= sig["stop_loss"]:
                result = "LOSS"
            elif elapsed >= EXPIRE_HOURS:
                result = "EXPIRED"

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
    print(f"거래대금 상위 {TOP_N}개 바이낸스 현물 알트코인 스캔 중...")

    signals = load_signals()

    resolved = resolve_open_signals(signals)
    for sig in resolved:
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)

    open_set = {
        (s.get("raw_symbol", s["symbol"].replace("/", "")), s["direction"], s["timeframe"])
        for s in signals
        if s.get("status") == "OPEN"
    }

    symbol_vol = get_top_symbols(TOP_N)
    symbols = list(symbol_vol.keys())

    new_signals = []
    for sym in symbols:
        result = scan_symbol(sym, symbol_vol.get(sym, 0))
        time.sleep(0.1)

        if result is None:
            continue

        key = (result["raw_symbol"], result["direction"], result["timeframe"])
        if key in open_set:
            continue

        new_signals.append(result)
        open_set.add(key)

    new_signals.sort(key=lambda x: x.get("quote_volume", 0), reverse=True)
    telegram_signals = new_signals[:TOP_SIGNAL_N]

    print(f"새 시그널 전체 {len(new_signals)}건")
    print(f"텔레그램 전송 대상 상위 {len(telegram_signals)}건")

    for sig in telegram_signals:
        print(f"[전송] {sig['symbol']} {sig['direction']}")
        send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig)

    for sig in new_signals[TOP_SIGNAL_N:]:
        print(f"[저장만] {sig['symbol']} {sig['direction']}")

    signals.extend(new_signals)
    save_signals(signals)
    print("Kronos 스캔 완료")


if __name__ == "__main__":
    main()
