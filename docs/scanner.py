#!/usr/bin/env python3

import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# ─────────────────────────────────────────────────────────────
# 환경 변수
# ─────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────
# 전략 / 실행 설정
# ─────────────────────────────────────────────────────────────
EMA_PERIOD = 200
ATR_PERIOD = 14
RR_RATIO = 2.0       # RR 1:2 고정
BE_TRIGGER = 1.5
EXPIRE_HOURS = 48

TOP_N = 100          # 거래대금 상위 N개 심볼 스캔
TOP_SIGNAL_N = 10    # 이 중 상위 몇 개만 텔레그램 전송

SIGNALS_FILE = "docs/signals.json"
BASE_URL = "https://api.binance.com"

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "BUSD",
    "DAI", "USDP", "USDE", "UST", "USTC",
    "AEUR", "XUSD",
}


# ─────────────────────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────
# 텔레그램
# ─────────────────────────────────────────────────────────────
def send_telegram(token, chat_id, signal, is_result: bool = False):
    """
    텔레그램 메시지 포맷:
    - 신규 시그널: '알트종목 시그널 공유' 규격
    - 결과 시그널: 🎯/🏆/💀/⏰ 포맷
    """
    if not token or not chat_id:
        return

    symbol = signal["symbol"]          # "HIGH/USDT"
    direction = signal["direction"]    # "LONG" or "SHORT"
    entry = fmt(signal["entry"])
    sl = fmt(signal["stop_loss"])
    tp1 = fmt(signal["take_profit1"])
    tp2 = fmt(signal["take_profit2"])
    be = fmt(signal["be_target"])
    slp = signal.get("sl_pct", None)
    tp1p = signal.get("tp1_pct", None)
    tp2p = signal.get("tp2_pct", None)
    ts = signal["time"]
    tf = signal.get("timeframe", "1h")
    qv = signal.get("quote_volume", 0)

    qv_str = f"{qv / 1e6:.1f}M" if qv >= 1e6 else f"{qv / 1e3:.0f}K"

    # 방향 이모지 / 라벨
    if direction == "LONG":
        dir_icon = "🟢"
    else:
        dir_icon = "🔴"

    if is_result:
        # 결과 시그널
        status = signal["status"]
        icons = {"WIN": "🏆", "LOSS": "💀", "EXPIRED": "⏰", "TP1": "🎯"}
        icon = icons.get(status, "❓")
        rp = fmt(signal.get("result_price"))
        pct = signal.get("result_pct")
        if pct is None:
            pct_str = "?"
        else:
            sign = "+" if pct >= 0 else ""
            pct_str = f"{sign}{pct:.2f}%"

        msg = (
            f"{icon} 결과: {symbol}\n"
            f"방향: {direction} | 결과: {status}\n"
            f"진입: {entry} → {rp}\n"
            f"손익: {pct_str}\n"
            f"{ts}"
        )
    else:
        # 신규 시그널 – 네가 준 규격
        rocket = "🚀"

        def pct_str(v):
            if v is None:
                return "?"
            sign = "-" if v < 0 else "+"
            return f"{sign}{abs(v):.2f}%"

        msg = (
            "알트종목 시그널 공유:\n"
            f"{rocket} {symbol} {dir_icon} {direction}\n"
            f"진입: {entry}\n"
            f"손절: {sl} ({pct_str(slp)})\n"
            f"TP1: {tp1} ({pct_str(tp1p)}) RR 1:{int(RR_RATIO)}\n"
            f"TP2: {tp2} ({pct_str(tp2p)})\n"
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
        # 텔레그램 오류로 스캐너 전체가 죽지 않도록 무시
        pass


# ─────────────────────────────────────────────────────────────
# 데이터 / 지표
# ─────────────────────────────────────────────────────────────
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
    """
    True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
    을 기반으로 하는 단순 평균 ATR.
    """
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
    """
    바이낸스 현물 USDT 마켓에서,
    일정 거래대금 이상 + 스테이블 제외 후 상위 top_n 심볼 반환.

    GitHub Actions 환경에서 응답이 비정상일 수 있어
    dict 타입이 아닌 항목은 방어적으로 스킵한다.
    """
    MIN_QV_USD = 20_000_000

    info = requests.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=20).json()
    tickers = requests.get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=20).json()

    allowed = set()
    for s in info.get("symbols", []):
        if not isinstance(s, dict):
            continue
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
        if not isinstance(t, dict):
            continue
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


def fetch_last_prices(symbols):
    r = requests.get(f"{BASE_URL}/api/v3/ticker/price", timeout=20)
    r.raise_for_status()
    want = set(symbols)
    return {
        row["symbol"]: float(row["price"])
        for row in r.json()
        if isinstance(row, dict) and row.get("symbol") in want
    }


# ─────────────────────────────────────────────────────────────
# ICT 간단 MSS 필터
# ─────────────────────────────────────────────────────────────
def simple_mss(df: pd.DataFrame, direction: str, bars: int = 5) -> bool:
    """
    간단 MSS 필터:
    - LONG: 최근 bars봉 직전 구조의 high를 현재 종가가 돌파
    - SHORT: 최근 bars봉 직전 구조의 low를 현재 종가가 하향 돌파
    """
    if len(df) < bars + 2:
        return False

    structure = df.iloc[-(bars + 1) : -1]
    last = df.iloc[-1]

    if direction == "LONG":
        prev_high = float(structure["high"].max())
        return float(last["close"]) > prev_high
    else:
        prev_low = float(structure["low"].min())
        return float(last["close"]) < prev_low


# ─────────────────────────────────────────────────────────────
# 심볼 스캔
# ─────────────────────────────────────────────────────────────
def scan_symbol(symbol, quote_volume=0):
    """
    1h 데이터 기준:
    - EMA200으로 LONG/SHORT 결정
    - 최근 5봉 MSS 필터(simple_mss) 통과
    - 최근 20봉 high/low + ATR로 SL/TP 계산
    """
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

        # ── ICT 구조 필터: 간단 MSS 느낌 ──
        if not simple_mss(df, direction, bars=5):
            return None

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


# ─────────────────────────────────────────────────────────────
# 시그널 결과 갱신
# ─────────────────────────────────────────────────────────────
def resolve_open_signals(signals):
    """
    OPEN 상태 시그널들에 대해:
    - TP2 / TP1 / SL / 만료(EXPIRED) 판정
    - 결과 확정 시 result_* 필드 채우고 리스트로 반환
    구버전 포맷( take_profit 만 있는 경우 )도 호환.
    """
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

        # 구버전 호환: take_profit 필드만 있을 수도 있음
        tp1 = sig.get("take_profit1", sig.get("take_profit"))
        tp2 = sig.get("take_profit2", tp1)

        result = None
        if sig["direction"] == "LONG":
            if curr >= tp2:
                result = "WIN"
            elif curr >= tp1:
                result = "TP1"
            elif curr <= sig["stop_loss"]:
                result = "LOSS"
            elif elapsed >= EXPIRE_HOURS:
                result = "EXPIRED"
        else:
            if curr <= tp2:
                result = "WIN"
            elif curr <= tp1:
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


# ─────────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────────
def main():
    print(f"거래대금 상위 {TOP_N}개 바이낸스 현물 알트코인 스캔 중...")

    signals = load_signals()

    # 미결 시그널 결과 확정
    resolved = resolve_open_signals(signals)
    if resolved:
        print(f"기존 시그널 결과 확정: {len(resolved)}건")
        for sig in resolved:
            send_telegram(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, sig, is_result=True)

    # 이미 OPEN 중인 (심볼, 방향, 타임프레임) 조합 수집
    open_set = {
        (s.get("raw_symbol", s["symbol"].replace("/", "")), s["direction"], s["timeframe"])
        for s in signals
        if s.get("status") == "OPEN"
    }

    # 거래대금 상위 심볼
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
