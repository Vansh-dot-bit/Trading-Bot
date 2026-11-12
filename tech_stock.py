#!/usr/bin/env python3
"""Custom US tech-stock index detector.

Data sources:
  - Massive: daily OHLC candles
  - Financial Modeling Prep (FMP): float shares

Index methodology:
  - Uses latest available free-float shares * latest completed daily close to estimate
    free-float market-cap weights.
  - Applies those weights to each stock's daily close-to-close returns to build a
    synthetic index starting at 1,000. Because historical free-float shares/weights
    are not requested from the providers, this is a current-weight synthetic index,
    not an official historical index series.
  - Index OHLC is approximated from weighted component OHLC returns; daily highs/lows
    are not synchronized across stocks, so range-based signals are approximate.
  - For each new completed index candle, compares it with the previous 100 completed
    index candles (excluding the signal candle):
       LONG  if close > mean(previous 100 closes) AND range > mean(previous 100 ranges)
       SHORT if close < mean(previous 100 closes) AND range > mean(previous 100 ranges)

Version 2.0 adds a daily +/-7% movement alert. Each completed daily candle is
compared with the previous completed daily close (the previous close is the new
reference/base for that day). Alerts are printed, logged, and sent through the
existing shared email notifier when configured. Keep this process running for polling.
"""

from __future__ import annotations

import getpass
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

MASSIVE_BASE = "https://api.massive.com"
FMP_BASE = "https://financialmodelingprep.com"
NY_TZ = ZoneInfo("America/New_York")
VERSION = "2.0"
BASE_INDEX_VALUE = 1000.0
LOOKBACK_CANDLES = 100
DAILY_MOVE_ALERT_PCT = 7.0
POLL_SECONDS = 15 * 60
DATA_DIR = Path("tech_index_data")
STATE_PATH = DATA_DIR / "state.json"
WEIGHTS_PATH = DATA_DIR / "stock_weights.csv"
INDEX_PATH = DATA_DIR / "tech_index_daily.csv"
STOCKS_PATH = DATA_DIR / "stock_daily_ohlc.csv"
ALERTS_PATH = DATA_DIR / "alerts.log"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "TechIndexDetector/1.0"})
API_GUARD = None
SHARED_NOTIFIER = None
# Massive free/basic plans can enforce per-minute request limits.
MASSIVE_MIN_REQUEST_INTERVAL = 15.0
_LAST_MASSIVE_REQUEST_AT = 0.0


def ask_secret(label: str, env_name: str) -> str:
    value = os.getenv(env_name, "").strip()
    if value:
        print(f"Using {env_name} from your environment variables.")
        return value
    while True:
        value = getpass.getpass(f"Enter {label} (input is hidden): ").strip()
        if value:
            return value
        print("API key cannot be empty. Try again.")


def ask_tickers() -> list[str]:
    while True:
        raw = input("Enter US tech stock ticker symbols separated by commas (e.g. AAPL,NVDA,MSFT): ").strip()
        tickers = list(dict.fromkeys(x.strip().upper() for x in raw.split(",") if x.strip()))
        if len(tickers) >= 2:
            return tickers
        print("Please enter at least two ticker symbols, separated by commas.")


def api_json(url: str, params: dict[str, Any], label: str) -> dict[str, Any] | list[Any]:
    try:
        response = (API_GUARD.get(SESSION, url, params=params, timeout=30, service=label) if API_GUARD else SESSION.get(url, params=params, timeout=30))
    except requests.RequestException as exc:
        raise RuntimeError(f"{label}: network request failed: {exc}") from exc
    if response.status_code != 200:
        body = response.text[:500].replace("\n", " ")
        raise RuntimeError(f"{label}: HTTP {response.status_code}. Response: {body}")
    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"{label}: server did not return valid JSON.") from exc


def fetch_daily_bars(ticker: str, api_key: str, start: date, end: date) -> pd.DataFrame:
    global _LAST_MASSIVE_REQUEST_AT
    # Space requests so sequential ticker downloads do not immediately hit
    # Massive's per-minute rate limit. Retry/backoff remains handled by ApiResilience.
    elapsed = time.monotonic() - _LAST_MASSIVE_REQUEST_AT
    if _LAST_MASSIVE_REQUEST_AT and elapsed < MASSIVE_MIN_REQUEST_INTERVAL:
        wait_for = MASSIVE_MIN_REQUEST_INTERVAL - elapsed
        print(f"  [RATE LIMIT] Waiting {wait_for:.1f}s before Massive request for {ticker}...")
        time.sleep(wait_for)
    url = f"{MASSIVE_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    try:
        payload = api_json(
            url,
            {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key},
            f"Massive daily candles for {ticker}",
        )
    finally:
        _LAST_MASSIVE_REQUEST_AT = time.monotonic()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Massive returned an unexpected response for {ticker}.")
    results = payload.get("results") or []
    if not results:
        raise RuntimeError(
            f"No daily candles returned for {ticker}. Check the ticker, API permissions, and date range."
        )
    df = pd.DataFrame(results)
    required = {"t", "o", "h", "l", "c"}
    if not required.issubset(df.columns):
        raise RuntimeError(f"Unexpected candle fields for {ticker}: {list(df.columns)}")
    df["date"] = pd.to_datetime(df["t"], unit="ms", utc=True).dt.tz_convert(NY_TZ).dt.date
    df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"})
    keep = ["date", "open", "high", "low", "close"] + (["volume"] if "volume" in df.columns else [])
    df = df[keep].drop_duplicates("date").sort_values("date").set_index("date")
    # Only completed US trading days: the API request itself ends yesterday in New York.
    df = df.loc[[d for d in df.index if d <= end]]
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"])
    if len(df) < LOOKBACK_CANDLES + 1:
        raise RuntimeError(
            f"Only {len(df)} completed daily candles returned for {ticker}; need at least {LOOKBACK_CANDLES + 1}."
        )
    return df


def parse_float_shares(payload: Any, ticker: str) -> float:
    """Read FMP float shares, supporting common stable endpoint response shapes."""
    candidates = payload if isinstance(payload, list) else [payload]
    for item in candidates:
        if not isinstance(item, dict):
            continue
        for key in ("floatShares", "freeFloatShares", "float_shares", "freeFloat"): 
            value = item.get(key)
            if value is not None:
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if number > 0:
                    return number
    raise RuntimeError(
        f"FMP did not return a usable float-share count for {ticker}. "
        "Your FMP plan may not include this endpoint, or its response format may differ. "
        "The script will not substitute total shares because that would change the agreed weighting method."
    )


def fetch_float_shares(ticker: str, api_key: str) -> float:
    url = f"{FMP_BASE}/stable/shares-float"
    payload = api_json(url, {"symbol": ticker, "apikey": api_key}, f"FMP free-float shares for {ticker}")
    return parse_float_shares(payload, ticker)


def fetch_all_data(tickers: list[str], massive_key: str, fmp_key: str) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    now_ny = datetime.now(NY_TZ)
    # Before 5 PM New York time, exclude today's potentially forming candle.
    # After 5 PM, request today's candle because the regular US session has ended.
    # If the provider has not published it yet, the next poll will retry automatically.
    end = now_ny.date() if now_ny.hour >= 17 else now_ny.date() - timedelta(days=1)
    start = end - timedelta(days=300)  # enough calendar days for 100+ trading candles
    print(f"\nFetching daily candles from {start} through {end} (New York dates)...")

    bars: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for ticker in tickers:
        try:
            bars[ticker] = fetch_daily_bars(ticker, massive_key, start, end)
            latest_close = float(bars[ticker]["close"].iloc[-1])
            float_shares = fetch_float_shares(ticker, fmp_key)
            free_float_cap = latest_close * float_shares
            rows.append({
                "symbol": ticker,
                "latest_completed_date": bars[ticker].index[-1].isoformat(),
                "latest_completed_close": latest_close,
                "float_shares": float_shares,
                "estimated_free_float_market_cap": free_float_cap,
            })
            print(f"  {ticker}: {len(bars[ticker])} daily candles; float shares loaded.")
        except Exception as exc:
            errors.append(f"{ticker}: {exc}")
    if errors:
        details = "\n  - ".join(errors)
        raise RuntimeError(
            "Could not load valid candle and free-float data for every selected ticker. "
            "No index/alerts will be generated because incomplete weights would be misleading.\n  - " + details
        )

    cap_df = pd.DataFrame(rows).set_index("symbol")
    total = float(cap_df["estimated_free_float_market_cap"].sum())
    if total <= 0:
        raise RuntimeError("Estimated free-float market capitalization total is not positive.")
    cap_df["weight"] = cap_df["estimated_free_float_market_cap"] / total
    cap_df["weight_percent"] = cap_df["weight"] * 100
    return bars, cap_df


def build_synthetic_index(bars: dict[str, pd.DataFrame], weights: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build synthetic index from weighted component returns on shared trading dates."""
    common_dates = None
    for df in bars.values():
        dates = set(df.index)
        common_dates = dates if common_dates is None else common_dates.intersection(dates)
    dates = sorted(common_dates or [])
    if len(dates) < LOOKBACK_CANDLES + 1:
        raise RuntimeError(
            f"Only {len(dates)} common trading dates across all selected stocks; need at least {LOOKBACK_CANDLES + 1}."
        )

    # Restrict every component to common dates, so all returns are aligned.
    aligned = {ticker: df.loc[dates].copy() for ticker, df in bars.items()}
    close_prices = pd.DataFrame({t: df["close"] for t, df in aligned.items()}, index=dates)
    daily_returns = close_prices.pct_change().fillna(0.0)
    index_returns = daily_returns.mul(weights, axis=1).sum(axis=1)

    # Build index OHLC by applying the same fixed current weights to component returns
    # from each day's open/high/low relative to the previous close. High/low are proxies.
    index_rows: list[dict[str, float | str]] = []
    index_close_prev = BASE_INDEX_VALUE
    previous_closes = {t: float(aligned[t]["close"].iloc[0]) for t in aligned}
    # First common date is the base row; then compound returns on later dates.
    first_date = dates[0]
    index_rows.append({"date": first_date.isoformat(), "open": BASE_INDEX_VALUE, "high": BASE_INDEX_VALUE,
                       "low": BASE_INDEX_VALUE, "close": BASE_INDEX_VALUE, "range": 0.0})

    for i in range(1, len(dates)):
        d = dates[i]
        day_open_rets, day_high_rets, day_low_rets = [], [], []
        for ticker, df in aligned.items():
            row = df.loc[d]
            prev_close = float(aligned[ticker]["close"].iloc[i - 1])
            if prev_close <= 0:
                raise RuntimeError(f"Invalid previous close for {ticker} on {d}.")
            w = float(weights[ticker])
            day_open_rets.append(w * (float(row["open"]) / prev_close - 1.0))
            day_high_rets.append(w * (float(row["high"]) / prev_close - 1.0))
            day_low_rets.append(w * (float(row["low"]) / prev_close - 1.0))
        idx_open = index_close_prev * (1.0 + sum(day_open_rets))
        idx_close = index_close_prev * (1.0 + float(index_returns.iloc[i]))
        idx_high_proxy = index_close_prev * (1.0 + sum(day_high_rets))
        idx_low_proxy = index_close_prev * (1.0 + sum(day_low_rets))
        idx_high = max(idx_open, idx_close, idx_high_proxy, idx_low_proxy)
        idx_low = min(idx_open, idx_close, idx_high_proxy, idx_low_proxy)
        index_rows.append({"date": d.isoformat(), "open": idx_open, "high": idx_high,
                           "low": idx_low, "close": idx_close, "range": idx_high - idx_low})
        index_close_prev = idx_close

    index_df = pd.DataFrame(index_rows).set_index("date")
    # Preserve component candles in a long CSV for inspection.
    stock_parts = []
    for ticker, df in aligned.items():
        part = df.reset_index().rename(columns={"index": "date"})
        part["symbol"] = ticker
        part["date"] = part["date"].astype(str)
        stock_parts.append(part)
    stocks_df = pd.concat(stock_parts, ignore_index=True)
    return index_df, stocks_df


def evaluate_latest(index_df: pd.DataFrame) -> dict[str, Any] | None:
    if len(index_df) < LOOKBACK_CANDLES + 1:
        raise RuntimeError(f"Need at least {LOOKBACK_CANDLES + 1} index candles to evaluate a signal.")
    # The last row is the signal candle; previous 100 rows are the baseline.
    signal = index_df.iloc[-1]
    history = index_df.iloc[-(LOOKBACK_CANDLES + 1):-1]
    avg_close = float(history["close"].mean())
    avg_range = float(history["range"].mean())
    current_close = float(signal["close"])
    current_range = float(signal["range"])
    direction = None
    if current_close > avg_close and current_range > avg_range:
        direction = "LONG"
    elif current_close < avg_close and current_range > avg_range:
        direction = "SHORT"
    return {
        "date": str(index_df.index[-1]),
        "direction": direction,
        "close": current_close,
        "range": current_range,
        "average_close_100": avg_close,
        "average_range_100": avg_range,
    }


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def save_csvs(cap_df: pd.DataFrame, index_df: pd.DataFrame, stocks_df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cap_df.to_csv(WEIGHTS_PATH, index=True)
    index_df.to_csv(INDEX_PATH, index=True)
    stocks_df.to_csv(STOCKS_PATH, index=False)


def print_and_log_alert(signal: dict[str, Any], tickers: list[str], cap_df: pd.DataFrame) -> None:
    message = (
        f"\n{'=' * 68}\n"
        f"TECH STOCK INDEX {signal['direction']} ALERT\n"
        f"Completed candle date: {signal['date']}\n"
        f"Index close: {signal['close']:.4f}\n"
        f"Index candle range: {signal['range']:.4f}\n"
        f"Average close (previous {LOOKBACK_CANDLES} candles): {signal['average_close_100']:.4f}\n"
        f"Average range (previous {LOOKBACK_CANDLES} candles): {signal['average_range_100']:.4f}\n"
        f"Stocks: {', '.join(tickers)}\n"
        f"Weights: " + ", ".join(f"{t} {cap_df.loc[t, 'weight_percent']:.2f}%" for t in tickers) + "\n"
        f"{'=' * 68}\n"
    )
    print(message, flush=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with ALERTS_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now(timezone.utc).isoformat()}] {message}\n")
    if SHARED_NOTIFIER is not None:
        SHARED_NOTIFIER.send_custom_alert(f"Tech Stock Index {signal['direction']} Alert", message)


def send_daily_move_alert(latest_date: str, previous_close: float, current_close: float,
                          tickers: list[str], cap_df: pd.DataFrame) -> None:
    """Alert when a completed daily close moves +/-7% from the prior close."""
    if previous_close <= 0:
        return
    change_pct = (current_close / previous_close - 1.0) * 100.0
    if change_pct >= DAILY_MOVE_ALERT_PCT:
        direction = "INCREASE"
    elif change_pct <= -DAILY_MOVE_ALERT_PCT:
        direction = "DECREASE"
    else:
        return

    message = (
        f"\n{'=' * 68}\n"
        f"TECH SYNTHETIC INDEX v{VERSION} — DAILY {direction} ALERT\n"
        f"Completed candle date: {latest_date}\n"
        f"Previous daily close (today's reference base): {previous_close:.4f}\n"
        f"Latest daily close: {current_close:.4f}\n"
        f"Daily movement: {change_pct:+.2f}% (threshold: +/-{DAILY_MOVE_ALERT_PCT:.1f}%)\n"
        f"Stocks: {', '.join(tickers)}\n"
        f"Weights: " + ", ".join(
            f"{ticker} {cap_df.loc[ticker, 'weight_percent']:.2f}%" for ticker in tickers
        ) + f"\n{'=' * 68}\n"
    )
    print(message, flush=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with ALERTS_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now(timezone.utc).isoformat()}] {message}\n")
    if SHARED_NOTIFIER is not None:
        SHARED_NOTIFIER.send_custom_alert(
            f"[TECH INDEX v{VERSION}] Daily {direction} above {DAILY_MOVE_ALERT_PCT:.0f}%",
            message,
        )


def run_once(tickers: list[str], massive_key: str, fmp_key: str) -> None:
    bars, cap_df = fetch_all_data(tickers, massive_key, fmp_key)
    weights = cap_df["weight"]
    index_df, stocks_df = build_synthetic_index(bars, weights)
    save_csvs(cap_df, index_df, stocks_df)

    latest_date = str(index_df.index[-1])
    state = load_state()
    print(f"Latest completed common index candle: {latest_date}")
    print(f"Synthetic index close: {float(index_df['close'].iloc[-1]):.4f}")
    print("Current free-float weights:")
    for ticker in tickers:
        print(f"  {ticker}: {cap_df.loc[ticker, 'weight_percent']:.2f}%")

    # Only evaluate/alert once per new completed candle.
    if state.get("last_processed_candle") == latest_date:
        print("This daily candle was already evaluated. Waiting for a new completed candle.")
        return

    # The previous completed daily close is the reference base for this day.
    # Check both upward and downward movement once for each newly completed candle.
    if len(index_df) >= 2:
        previous_close = float(index_df["close"].iloc[-2])
        current_close = float(index_df["close"].iloc[-1])
        send_daily_move_alert(latest_date, previous_close, current_close, tickers, cap_df)

    signal = evaluate_latest(index_df)
    if signal and signal["direction"]:
        print_and_log_alert(signal, tickers, cap_df)
    else:
        print(
            f"No alert for {latest_date}. Conditions not both satisfied. "
            f"Close={signal['close']:.4f}, avg close={signal['average_close_100']:.4f}; "
            f"range={signal['range']:.4f}, avg range={signal['average_range_100']:.4f}."
        )
    state["last_processed_candle"] = latest_date
    save_state(state)




def get_startup_snapshot(tickers: list[str], massive_key: str, fmp_key: str) -> dict[str, Any]:
    """Fetch initial index + constituent contribution data for startup digest.

    The current completed daily candle is marked processed here so the background
    poller does not send a second email for the same initial candle.
    """
    bars, cap_df = fetch_all_data(tickers, massive_key, fmp_key)
    index_df, stocks_df = build_synthetic_index(bars, cap_df["weight"])
    save_csvs(cap_df, index_df, stocks_df)
    signal = evaluate_latest(index_df)
    latest_date = str(index_df.index[-1])
    components = []
    for ticker in tickers:
        df = bars[ticker]
        latest_close = float(df["close"].iloc[-1])
        previous_close = float(df["close"].iloc[-2])
        daily_return = (latest_close / previous_close - 1.0) if previous_close else 0.0
        weight = float(cap_df.loc[ticker, "weight"])
        components.append({
            "symbol": ticker,
            "close": latest_close,
            "weight_percent": weight * 100.0,
            "daily_return_percent": daily_return * 100.0,
            "index_contribution_percent": daily_return * weight * 100.0,
        })
    state = load_state()
    state["last_processed_candle"] = latest_date
    save_state(state)
    return {
        "date": latest_date,
        "index_close": float(index_df["close"].iloc[-1]),
        "index_open": float(index_df["open"].iloc[-1]),
        "index_high": float(index_df["high"].iloc[-1]),
        "index_low": float(index_df["low"].iloc[-1]),
        "index_range": float(index_df["range"].iloc[-1]),
        "signal": signal,
        "components": components,
        "lookback": LOOKBACK_CANDLES,
    }


def ask_runtime_config():
    """Called by main_bot.py after the main bot and Fed questions."""
    enabled = input("Enable US tech-stock index detector? (Y/n): ").strip().lower() != "n"
    if not enabled:
        return {"enabled": False}
    tickers = ask_tickers()
    massive_key = ask_secret("Massive API key", "MASSIVE_API_KEY")
    fmp_key = ask_secret("Financial Modeling Prep API key", "FMP_API_KEY")
    return {"enabled": True, "tickers": tickers, "massive_key": massive_key,
            "fmp_key": fmp_key, "poll_seconds": POLL_SECONDS}


def run_service(config, notifier, api_guard, stop_event=None):
    global API_GUARD, SHARED_NOTIFIER
    API_GUARD, SHARED_NOTIFIER = api_guard, notifier
    tickers = config["tickers"]
    massive_key, fmp_key = config["massive_key"], config["fmp_key"]
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("\n[TECH INDEX] Initial download and index construction starting...", flush=True)
    while stop_event is None or not stop_event.is_set():
        try:
            run_once(tickers, massive_key, fmp_key)
        except Exception as exc:
            print(f"\n[TECH INDEX] UPDATE FAILED: {exc}", file=sys.stderr, flush=True)
        wait_seconds = int(config.get("poll_seconds", POLL_SECONDS))
        if stop_event is not None:
            if stop_event.wait(wait_seconds):
                break
        else:
            time.sleep(wait_seconds)

