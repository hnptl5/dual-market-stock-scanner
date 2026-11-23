"""
Vercel Serverless Function: /api/stock
On-demand deep-dive research endpoint for ANY global/US/NSE/BSE ticker (not bounded to the scanner list).
Supports automatic market detection (market=AUTO):
  - Checks explicit .NS / .BO suffix
  - Checks known US & NSE universes for instant routing
  - For unknown tickers, automatically probes US -> NSE (.NS) -> BSE (.BO)
"""

from __future__ import annotations

import json
import math
import re
from http.server import BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
import yfinance as yf

try:
    from api.scan import compute_mas, detect_candlestick_pattern, wilder_rsi
    from api._universes import INDIA_FULL_UNIVERSE, US_FULL_UNIVERSE
except ImportError:
    from scan import compute_mas, detect_candlestick_pattern, wilder_rsi
    from _universes import INDIA_FULL_UNIVERSE, US_FULL_UNIVERSE

KNOWN_US_SYMS = {row[0].upper() for row in US_FULL_UNIVERSE}
KNOWN_IN_SYMS = {row[0].replace(".NS", "").replace(".BO", "").upper() for row in INDIA_FULL_UNIVERSE}


def clean_val(v: Any, digits: int = 2) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, (int, float)):
        f = float(v)
        if not math.isfinite(f):
            return None
        return round(f, digits) if isinstance(v, float) else v
    return str(v)


def candidate_symbols(raw: str, market: str = "AUTO") -> list[str]:
    """Return ordered list of yfinance ticker candidates to try."""
    sym = re.sub(r"[^A-Za-z0-9.&^-]", "", (raw or "").strip().upper())
    if not sym:
        return []
    if sym.endswith((".NS", ".BO")) or sym.startswith("^"):
        return [sym]
    if market == "IN":
        return [f"{sym}.NS", f"{sym}.BO", sym]
    if market == "US":
        return [sym, f"{sym}.NS", f"{sym}.BO"]
    # AUTO mode: check known universe membership first to avoid wasted probes
    if sym in KNOWN_IN_SYMS and sym not in KNOWN_US_SYMS:
        return [f"{sym}.NS", f"{sym}.BO", sym]
    if sym in KNOWN_US_SYMS and sym not in KNOWN_IN_SYMS:
        return [sym, f"{sym}.NS", f"{sym}.BO"]
    return [sym, f"{sym}.NS", f"{sym}.BO"]


def compute_atr(df: pd.DataFrame, period: int = 14) -> float | None:
    if df is None or len(df) < period + 2:
        return None
    h = df["High"].astype(float)
    l = df["Low"].astype(float)
    c = df["Close"].astype(float)
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean().iloc[-1]
    return clean_val(float(atr), 2)


def df_to_records(df: Any, max_rows: int = 10) -> list[dict[str, Any]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    sub = df.head(max_rows).copy()
    records: list[dict[str, Any]] = []
    for idx, row in sub.iterrows():
        item: dict[str, Any] = {}
        if idx is not None and str(idx) != "0":
            item["index"] = str(idx)[:10]
        for col in sub.columns:
            val = row[col]
            if hasattr(val, "strftime"):
                item[str(col)] = val.strftime("%Y-%m-%d")
            else:
                item[str(col)] = clean_val(val, 2)
        records.append(item)
    return records


def fetch_stock_detail(raw_symbol: str, market: str = "AUTO") -> dict[str, Any]:
    candidates = candidate_symbols(raw_symbol, market)
    if not candidates:
        raise ValueError("Ticker symbol is required.")

    yf_sym = ""
    tk = None
    hist_d = None
    for cand in candidates:
        try:
            probe_tk = yf.Ticker(cand)
            probe_hist = probe_tk.history(period="2y", interval="1d", auto_adjust=True)
            if probe_hist is not None and not probe_hist.empty and len(probe_hist["Close"].dropna()) >= 2:
                yf_sym, tk, hist_d = cand, probe_tk, probe_hist
                break
        except Exception:
            continue

    if tk is None or hist_d is None or hist_d.empty:
        raise LookupError(f"No market data found for '{raw_symbol}' on US, NSE, or BSE.")

    hist_1h = tk.history(period="30d", interval="60m", auto_adjust=True)
    close_d = hist_d["Close"].dropna()
    close_w = close_d.resample("W-FRI").last().dropna()
    close_m = close_d.resample("ME").last().dropna()
    close_1h = hist_1h["Close"].dropna() if hist_1h is not None and not hist_1h.empty else close_d.tail(60)

    info: dict[str, Any] = {}
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    price = clean_val(float(close_d.iloc[-1]), 2)
    prev_close = clean_val(float(close_d.iloc[-2]), 2) if len(close_d) >= 2 else price
    chg_pct = clean_val(((price - prev_close) / prev_close) * 100.0 if prev_close else 0.0, 2)
    vol_d = hist_d["Volume"].dropna()
    last_vol = int(vol_d.iloc[-1]) if len(vol_d) else 0
    avg_vol_20 = int(vol_d.tail(21).iloc[:-1].mean()) if len(vol_d) > 2 else max(1, last_vol)
    rvol = clean_val(last_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0, 2)

    insider_tx = []
    try:
        insider_tx = df_to_records(tk.insider_transactions, 12)
    except Exception:
        insider_tx = []

    inst_holders = []
    try:
        inst_holders = df_to_records(tk.institutional_holders, 8)
    except Exception:
        inst_holders = []

    major_holders = []
    try:
        mh = tk.major_holders
        if isinstance(mh, pd.DataFrame) and not mh.empty:
            for idx, row in mh.iterrows():
                major_holders.append({
                    "metric": str(idx),
                    "value": clean_val(row.iloc[0], 4) if len(row) else None,
                })
    except Exception:
        major_holders = []

    news_out = []
    try:
        raw_news = tk.news or []
        for n in raw_news[:6]:
            content = n.get("content") if isinstance(n, dict) and "content" in n else n
            if not isinstance(content, dict):
                continue
            title = content.get("title") or n.get("title")
            pub = (content.get("provider") or {}).get("displayName") or n.get("publisher") or ""
            link = (content.get("canonicalUrl") or {}).get("url") or n.get("link") or ""
            if title:
                news_out.append({"title": title, "publisher": pub, "link": link})
    except Exception:
        news_out = []

    clean_sym = yf_sym.replace(".NS", "").replace(".BO", "")
    is_in = yf_sym.endswith((".NS", ".BO"))
    tv_sym = f"{'BSE' if yf_sym.endswith('.BO') else 'NSE'}:{clean_sym.replace('&', '_')}" if is_in else clean_sym

    chart_series = [
        {"date": str(idx)[:10], "close": clean_val(float(val), 2)}
        for idx, val in close_d.tail(60).items()
    ]

    return {
        "symbol": clean_sym,
        "yfSymbol": yf_sym,
        "tvSymbol": tv_sym,
        "market": "IN" if is_in else "US",
        "currency": "₹" if is_in else "$",
        "name": info.get("longName") or info.get("shortName") or clean_sym,
        "exchange": info.get("exchange") or ("NSE" if is_in else "US"),
        "sector": info.get("sector") or "N/A",
        "industry": info.get("industry") or "N/A",
        "website": info.get("website") or "",
        "summary": info.get("longBusinessSummary") or "",
        "employees": clean_val(info.get("fullTimeEmployees"), 0),
        "price": price,
        "changePct": chg_pct,
        "volume": last_vol,
        "avgVolume20": avg_vol_20,
        "rvol": rvol,
        "pattern": detect_candlestick_pattern(hist_d),
        "atr14": compute_atr(hist_d, 14),
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
        "fundamentals": {
            "marketCap": clean_val(info.get("marketCap"), 0),
            "enterpriseValue": clean_val(info.get("enterpriseValue"), 0),
            "trailingPE": clean_val(info.get("trailingPE"), 2),
            "forwardPE": clean_val(info.get("forwardPE"), 2),
            "pegRatio": clean_val(info.get("pegRatio"), 2),
            "priceToBook": clean_val(info.get("priceToBook"), 2),
            "priceToSales": clean_val(info.get("priceToSalesTrailing12Months"), 2),
            "evToEbitda": clean_val(info.get("enterpriseToEbitda"), 2),
            "epsTrailing": clean_val(info.get("trailingEps"), 2),
            "epsForward": clean_val(info.get("forwardEps"), 2),
            "roe": clean_val(info.get("returnOnEquity"), 4),
            "roa": clean_val(info.get("returnOnAssets"), 4),
            "profitMargin": clean_val(info.get("profitMargins"), 4),
            "operatingMargin": clean_val(info.get("operatingMargins"), 4),
            "revenueGrowth": clean_val(info.get("revenueGrowth"), 4),
            "earningsGrowth": clean_val(info.get("earningsGrowth"), 4),
            "debtToEquity": clean_val(info.get("debtToEquity"), 2),
            "currentRatio": clean_val(info.get("currentRatio"), 2),
            "freeCashflow": clean_val(info.get("freeCashflow"), 0),
            "dividendYield": clean_val(info.get("dividendYield"), 4),
            "beta": clean_val(info.get("beta"), 2),
            "fiftyTwoWeekHigh": clean_val(info.get("fiftyTwoWeekHigh"), 2),
            "fiftyTwoWeekLow": clean_val(info.get("fiftyTwoWeekLow"), 2),
            "targetMeanPrice": clean_val(info.get("targetMeanPrice"), 2),
            "recommendationKey": info.get("recommendationKey") or None,
            "numberOfAnalystOpinions": clean_val(info.get("numberOfAnalystOpinions"), 0),
        },
        "ownership": {
            "heldPercentInsiders": clean_val(info.get("heldPercentInsiders"), 4),
            "heldPercentInstitutions": clean_val(info.get("heldPercentInstitutions"), 4),
            "shortRatio": clean_val(info.get("shortRatio"), 2),
            "shortPercentOfFloat": clean_val(info.get("shortPercentOfFloat"), 4),
            "majorHolders": major_holders,
            "insiderTransactions": insider_tx,
            "institutionalHolders": inst_holders,
        },
        "news": news_out,
        "chart": chart_series,
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        symbol = (params.get("symbol", [""])[0] or "").strip()
        market = (params.get("market", ["AUTO"])[0] or "AUTO").upper()

        try:
            payload = fetch_stock_detail(symbol, market)
            body = json.dumps(payload, allow_nan=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "public, s-maxage=300, stale-while-revalidate=600")
            self.end_headers()
            self.wfile.write(body)
        except ValueError as exc:
            self._send_err(400, str(exc))
        except LookupError as exc:
            self._send_err(404, str(exc))
        except Exception as exc:
            self._send_err(500, str(exc))

    def _send_err(self, code: int, msg: str) -> None:
        body = json.dumps({"error": msg}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
