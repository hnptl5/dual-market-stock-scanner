"""
Vercel Serverless Function: /api/scan (v2)
Fetches US (NYSE/NASDAQ) and Indian (NSE) multi-timeframe data via yfinance.

Features:
  - 50 US (S&P 500 / NASDAQ 100) + 50 India (NIFTY 50 / NSE F&O) tickers
  - Custom on-demand ticker scanning via &custom=SYM1,SYM2
  - NIFTY 50 RVOL via NIFTYBEES.NS ETF fallback (since ^NSEI index has 0 volume)
  - Intraday time-of-day RVOL pace normalization during live market hours
  - Automatic daily candlestick pattern detection (Pin Bar, Engulfing, Inside Bar)
  - Multi-timeframe Wilder RSI(14) & EMA/SMA (9, 20, 50, 100, 200) across 1H, D, W, M

Endpoints:
  - GET /api/scan?market=US
  - GET /api/scan?market=IN
  - GET /api/scan?market=IN&fresh=1&custom=DIXON,COFORGE
  - GET /api/scan?market=ALL   (Daily 10:00 AM IST Vercel Cron Job)
"""

from __future__ import annotations

import json
import math
import re
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

IST = ZoneInfo("Asia/Kolkata")
ET = ZoneInfo("America/New_York")

try:
    from api._universes import INDIA_FULL_UNIVERSE as INDIA_UNIVERSE, US_FULL_UNIVERSE as US_UNIVERSE
except ImportError:
    from _universes import INDIA_FULL_UNIVERSE as INDIA_UNIVERSE, US_FULL_UNIVERSE as US_UNIVERSE


def safe_round(val: Any, digits: int = 2, default: float = 0.0) -> float:
    try:
        f = float(val)
        return round(f, digits) if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def wilder_rsi(series: pd.Series, period: int = 14) -> float:
    close = series.dropna().astype(float)
    if len(close) < period + 2:
        return 50.0
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    last_gain = float(avg_gain.iloc[-1])
    last_loss = float(avg_loss.iloc[-1])
    if last_loss == 0:
        return 50.0 if last_gain == 0 else 100.0
    rs = last_gain / last_loss
    return safe_round(100.0 - (100.0 / (1.0 + rs)), 1, 50.0)


def compute_mas(series: pd.Series) -> dict[str, float]:
    close = series.dropna().astype(float)
    if close.empty:
        return {"ema9": 0.0, "ema20": 0.0, "ema50": 0.0, "sma50": 0.0, "sma100": 0.0, "sma200": 0.0}
    last = float(close.iloc[-1])

    def ema(n: int) -> float:
        return safe_round(close.ewm(span=n, adjust=False).mean().iloc[-1], 2, last)

    def sma(n: int) -> float:
        window = min(n, len(close))
        return safe_round(close.rolling(window=window).mean().iloc[-1], 2, last)

    return {
        "ema9": ema(9),
        "ema20": ema(20),
        "ema50": ema(50),
        "sma50": sma(50),
        "sma100": sma(100),
        "sma200": sma(200),
    }


def detect_candlestick_pattern(df: pd.DataFrame) -> str | None:
    """Detect daily candlestick confirmation patterns on the latest completed/active bar."""
    if df is None or len(df) < 2:
        return None
    needed = {"Open", "High", "Low", "Close"}
    if not needed.issubset(df.columns):
        return None

    clean = df[["Open", "High", "Low", "Close"]].dropna()
    if len(clean) < 2:
        return None

    prev = clean.iloc[-2]
    curr = clean.iloc[-1]

    p_o, p_h, p_l, p_c = float(prev["Open"]), float(prev["High"]), float(prev["Low"]), float(prev["Close"])
    c_o, c_h, c_l, c_c = float(curr["Open"]), float(curr["High"]), float(curr["Low"]), float(curr["Close"])

    rng = c_h - c_l
    if rng <= 0:
        return None

    body = abs(c_c - c_o)
    upper_wick = c_h - max(c_o, c_c)
    lower_wick = min(c_o, c_c) - c_l
    body_eff = max(body, rng * 0.05)

    # 1. Bullish / Bearish Engulfing
    if p_c < p_o and c_c > c_o and c_o <= p_c and c_c >= p_o and body > abs(p_c - p_o):
        return "Bull Engulfing"
    if p_c > p_o and c_c < c_o and c_o >= p_c and c_c <= p_o and body > abs(p_c - p_o):
        return "Bear Engulfing"

    # 2. Bullish / Bearish Pin Bar (long rejection wick >= 2x body and >= 55% of total range)
    if lower_wick >= 2.0 * body_eff and lower_wick >= 0.55 * rng and upper_wick <= 0.25 * rng:
        return "Bull Pin Bar"
    if upper_wick >= 2.0 * body_eff and upper_wick >= 0.55 * rng and lower_wick <= 0.25 * rng:
        return "Bear Pin Bar"

    # 3. Inside Bar
    if c_h <= p_h and c_l >= p_l:
        return "Inside Bar"

    return None


def get_session_pace_factor(market_code: str, last_bar_date: Any, now_utc: datetime | None = None) -> tuple[float, bool]:
    """
    Return (elapsed_fraction, is_pace_adjusted).
    When the latest daily candle belongs to today's live trading session, normalize volume
    by elapsed session time (clamped to >= 8% so opening-minute spikes stay sane).
    """
    now = now_utc or datetime.now(timezone.utc)
    if market_code == "US":
        local_now = now.astimezone(ET)
        open_mins, close_mins = 9 * 60 + 30, 16 * 60  # 390 mins
    else:
        local_now = now.astimezone(IST)
        open_mins, close_mins = 9 * 60 + 15, 15 * 60 + 30  # 375 mins

    if local_now.weekday() >= 5:
        return 1.0, False

    bar_date_str = str(last_bar_date)[:10] if last_bar_date is not None else ""
    if bar_date_str != local_now.strftime("%Y-%m-%d"):
        return 1.0, False

    cur_mins = local_now.hour * 60 + local_now.minute
    if open_mins <= cur_mins < close_mins:
        elapsed = max(1, cur_mins - open_mins)
        fraction = max(0.08, min(1.0, elapsed / float(close_mins - open_mins)))
        return fraction, True

    return 1.0, False


def compute_rvol(vol_series: pd.Series, pace_factor: float = 1.0) -> tuple[int, int, float]:
    vol = vol_series.dropna().astype(float)
    if vol.empty:
        return 0, 0, 0.0
    last_vol = int(max(0.0, float(vol.iloc[-1])))
    prior = vol.tail(21).iloc[:-1] if len(vol) > 2 else vol
    avg_20 = int(max(0.0, float(prior.mean()))) if not prior.empty else 0
    if avg_20 <= 0 or last_vol <= 0:
        return last_vol, avg_20, 0.0
    projected_vol = last_vol / max(0.08, min(1.0, pace_factor))
    return last_vol, avg_20, safe_round(projected_vol / avg_20, 2, 0.0)


def parse_custom_tickers(raw_param: str, market_code: str, existing_symbols: set[str]) -> list[tuple[str, str, str, str, list[str]]]:
    """Sanitize & normalize comma-separated custom tickers for US or India (NSE)."""
    if not raw_param:
        return []
    out: list[tuple[str, str, str, str, list[str]]] = []
    seen = set(existing_symbols)
    for token in raw_param.split(","):
        cleaned = re.sub(r"[^A-Za-z0-9.&_-]", "", token.strip().upper())
        if not cleaned or len(cleaned) > 20:
            continue
        if market_code == "IN":
            base_sym = cleaned.replace(".NS", "").replace(".BO", "")
            yf_ticker = cleaned if cleaned.endswith((".NS", ".BO")) else f"{base_sym}.NS"
            tv_sym = f"NSE:{base_sym.replace('&', '_').replace('-', '_')}"
        else:
            base_sym = cleaned
            yf_ticker = cleaned
            tv_sym = cleaned
        if base_sym in seen:
            continue
        seen.add(base_sym)
        out.append((yf_ticker, tv_sym, f"{base_sym} (Custom)", "Custom Watchlist", ["Custom"]))
        if len(out) >= 15:
            break
    return out


def download_chunked(tickers: list[str], period: str, interval: str, chunk_size: int = 45) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for i in range(0, len(tickers), chunk_size):
        batch = tickers[i : i + chunk_size]
        for attempt in range(2):
            try:
                df = yf.download(
                    tickers=batch,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                if len(batch) == 1:
                    t = batch[0]
                    if not df.empty:
                        result[t] = df.dropna(how="all")
                else:
                    cols_l0 = set(df.columns.get_level_values(0))
                    for t in batch:
                        if t in cols_l0:
                            sub = df[t].dropna(how="all")
                            if not sub.empty:
                                result[t] = sub
                break
            except Exception:
                time.sleep(1.0 * (attempt + 1))
    return result


def build_benchmark(
    label: str,
    name: str,
    daily_df: pd.DataFrame | None,
    market_code: str,
    vol_proxy_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if daily_df is None or len(daily_df) < 2:
        return {
            "symbol": label,
            "name": name,
            "price": 0.0,
            "changePct": 0.0,
            "rvol": 0.0,
            "above20Ema": True,
            "above50Ema": True,
            "breadthUp": 50,
        }
    close = daily_df["Close"].dropna()
    price = safe_round(close.iloc[-1], 2)
    prev = safe_round(close.iloc[-2], 2, price)
    chg_pct = safe_round(((price - prev) / prev) * 100.0 if prev else 0.0, 2)

    pace, _ = get_session_pace_factor(market_code, close.index[-1])
    _, _, rvol = compute_rvol(daily_df["Volume"], pace)
    if rvol <= 0 and vol_proxy_df is not None and "Volume" in vol_proxy_df:
        proxy_pace, _ = get_session_pace_factor(market_code, vol_proxy_df.index[-1])
        _, _, rvol = compute_rvol(vol_proxy_df["Volume"], proxy_pace)

    mas = compute_mas(close)
    return {
        "symbol": label,
        "name": name,
        "price": price,
        "changePct": chg_pct,
        "rvol": rvol,
        "above20Ema": price > mas["ema20"],
        "above50Ema": price > mas["ema50"],
        "breadthUp": 50,
    }


def scan_market(market_code: str, custom_raw: str = "") -> dict[str, Any]:
    if market_code == "US":
        exchange, currency = "NYSE / NASDAQ", "$"
        base_universe = list(US_UNIVERSE)
        primary_bench = ("SPY", "SPY", "S&P 500 ETF")
        secondary_bench = ("QQQ", "QQQ", "NASDAQ 100 ETF")
        vol_proxy_ticker = None
    else:
        market_code = "IN"
        exchange, currency = "NSE", "₹"
        base_universe = list(INDIA_UNIVERSE)
        primary_bench = ("^NSEI", "NIFTY 50", "NSE Nifty 50 Index")
        secondary_bench = ("^NSEBANK", "BANK NIFTY", "Nifty Bank Index")
        vol_proxy_ticker = "NIFTYBEES.NS"

    existing_syms = {row[0].replace(".NS", "").replace(".BO", "") for row in base_universe}
    custom_rows = parse_custom_tickers(custom_raw, market_code, existing_syms)
    universe = base_universe + custom_rows

    all_tickers = [row[0] for row in universe] + [primary_bench[0], secondary_bench[0]]
    if vol_proxy_ticker:
        all_tickers.append(vol_proxy_ticker)

    daily_map = download_chunked(all_tickers, period="5y", interval="1d")
    hourly_map = download_chunked([row[0] for row in universe], period="60d", interval="60m")

    stocks_out: list[dict[str, Any]] = []
    advancing = 0
    any_pace_adjusted = False

    for yf_ticker, tv_symbol, name, sector, tags in universe:
        ddf = daily_map.get(yf_ticker)
        if ddf is None or len(ddf) < 20:
            continue

        close_d = ddf["Close"].dropna()
        if len(close_d) < 2:
            continue

        price = safe_round(close_d.iloc[-1], 2)
        prev = safe_round(close_d.iloc[-2], 2, price)
        change_pct = safe_round(((price - prev) / prev) * 100.0 if prev else 0.0, 2)
        if change_pct > 0:
            advancing += 1

        pace, is_pace = get_session_pace_factor(market_code, close_d.index[-1])
        if is_pace:
            any_pace_adjusted = True
        last_vol, avg_vol_20, rvol = compute_rvol(ddf["Volume"], pace)

        close_w = close_d.resample("W-FRI").last().dropna()
        close_m = close_d.resample("ME").last().dropna()
        hdf = hourly_map.get(yf_ticker)
        close_1h = hdf["Close"].dropna() if hdf is not None and "Close" in hdf else close_d.tail(60)

        clean_symbol = yf_ticker.replace(".NS", "").replace(".BO", "")
        history = [safe_round(v, 2) for v in close_d.tail(12).tolist()]
        pattern = detect_candlestick_pattern(ddf)

        stocks_out.append(
            {
                "symbol": clean_symbol,
                "tvSymbol": tv_symbol,
                "name": name,
                "sector": sector,
                "universe": tags,
                "price": price,
                "changePct": change_pct,
                "volume": last_vol,
                "avgVolume20": avg_vol_20,
                "rvol": rvol,
                "rvolPaceAdjusted": is_pace,
                "pattern": pattern,
                "rsi": {
                    "1H": wilder_rsi(close_1h),
                    "D": wilder_rsi(close_d),
                    "W": wilder_rsi(close_w),
                    "M": wilder_rsi(close_m),
                },
                "ma": {
                    "1H": compute_mas(close_1h),
                    "D": compute_mas(close_d),
                    "W": compute_mas(close_w),
                    "M": compute_mas(close_m),
                },
                "history": history,
            }
        )

    vol_proxy_df = daily_map.get(vol_proxy_ticker) if vol_proxy_ticker else None
    bench = build_benchmark(primary_bench[1], primary_bench[2], daily_map.get(primary_bench[0]), market_code, vol_proxy_df)
    bench["breadthUp"] = round((advancing / max(1, len(stocks_out))) * 100)
    sec_bench = build_benchmark(secondary_bench[1], secondary_bench[2], daily_map.get(secondary_bench[0]), market_code)

    now_ist = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")
    pace_tag = " • Intraday Pace RVOL" if any_pace_adjusted else ""
    return {
        "market": market_code,
        "exchange": exchange,
        "currency": currency,
        "benchmark": bench,
        "secondaryBenchmark": sec_bench,
        "rvolPaceAdjusted": any_pace_adjusted,
        "asOfLabel": f"Synced {now_ist} (Live API{pace_tag})",
        "source": "vercel-yfinance-v3",
        "stocks": stocks_out,
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        market = (params.get("market", ["IN"])[0] or "IN").upper()
        fresh = params.get("fresh", ["0"])[0] == "1"
        custom_raw = params.get("custom", [""])[0] or ""

        try:
            if market == "ALL":
                payload: Any = {
                    "ok": True,
                    "US": scan_market("US"),
                    "IN": scan_market("IN"),
                }
            else:
                payload = scan_market("US" if market == "US" else "IN", custom_raw=custom_raw)

            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            if fresh or custom_raw:
                self.send_header("Cache-Control", "no-store")
            else:
                self.send_header("Cache-Control", "public, s-maxage=3600, stale-while-revalidate=86400")
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            err = json.dumps({"error": str(exc)}).encode("utf-8")
            self.send_response(500)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(err)
