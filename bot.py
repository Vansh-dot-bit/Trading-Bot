import sys
import io

try:
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", errors="replace"
    )
except AttributeError:
    pass

# ================================================================
#  1.  STANDARD LIBRARY IMPORTS
# ================================================================
import json
import time
import hmac
import hashlib
import logging
import logging.handlers
import math
import threading
import difflib
from datetime import datetime, timezone
from collections import deque
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from dataclasses import dataclass, field
from enum import Enum

# ================================================================
#  2.  THIRD-PARTY IMPORTS
# ================================================================
import requests
import websocket

# ================================================================
#  3.  LOGGING
# ================================================================
LOG_FORMAT  = "%(asctime)s [%(levelname)-8s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _build_logger() -> logging.Logger:
    log = logging.getLogger("DeltaBot")
    if log.handlers:
        return log
    log.setLevel(logging.DEBUG)
    log.propagate = False
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    log.addHandler(ch)
    try:
        fh = logging.handlers.RotatingFileHandler(
            "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3,
            encoding="utf-8",
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        log.addHandler(fh)
    except Exception:
        pass
    return log


logger = _build_logger()


def _log(level: str, tag: str, msg: str) -> None:
    getattr(logger, level)(f"[{tag}] {msg}")


# ================================================================
#  4.  WEBSOCKET MANAGER
# ================================================================

class WSState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING   = "connecting"
    CONNECTED    = "connected"
    RECONNECTING = "reconnecting"
    STOPPED      = "stopped"


@dataclass
class WSConfig:
    url: str = "wss://socket.india.delta.exchange"
    ping_interval: int = 20
    ping_timeout: int = 10
    max_reconnect_attempts: int = 10
    reconnect_base_delay: int = 5
    reconnect_max_delay: int = 30
    reconnect_backoff_multiplier: int = 2


class DeltaWebSocket:
    """
    Production-grade WebSocket manager for Delta Exchange India.
    Features: auto-reconnect with exponential backoff, subscription
    restore, heartbeat, on-reconnect backfill hook.
    """

    def __init__(self, config: Optional[WSConfig] = None):
        self.config = config or WSConfig()

        self._state             = WSState.DISCONNECTED
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread]   = None
        self._thread_lock       = threading.Lock()

        self._subscriptions: Dict[str, List[str]] = {}
        self._subscription_lock = threading.Lock()

        self._reconnect_attempts = 0
        self._reconnect_delay    = self.config.reconnect_base_delay
        self._should_stop        = threading.Event()

        self._on_candle_callback:       Optional[callable] = None
        self._on_connected_callback:    Optional[callable] = None
        self._on_disconnected_callback: Optional[callable] = None
        self._on_error_callback:        Optional[callable] = None
        self._on_reconnect_callback:    Optional[callable] = None

    def set_candle_callback(self, cb: callable)      -> None: self._on_candle_callback       = cb
    def set_connected_callback(self, cb: callable)   -> None: self._on_connected_callback    = cb
    def set_disconnected_callback(self, cb: callable)-> None: self._on_disconnected_callback = cb
    def set_error_callback(self, cb: callable)       -> None: self._on_error_callback        = cb
    def set_reconnect_callback(self, cb: callable)   -> None: self._on_reconnect_callback    = cb

    def subscribe(self, timeframe: str, symbols: List[str]) -> None:
        channel = self._get_channel_name(timeframe)
        with self._subscription_lock:
            self._subscriptions[channel] = list(set(symbols))
        _log("info", "WS", f"Subscription updated: channel={channel}, symbols={symbols}")
        if self._state == WSState.CONNECTED and self._ws:
            self._send_subscription(channel, symbols)

    def unsubscribe(self, timeframe: str) -> None:
        channel = self._get_channel_name(timeframe)
        with self._subscription_lock:
            self._subscriptions.pop(channel, None)
        _log("info", "WS", f"Unsubscribed from {channel}")

    def start(self) -> None:
        if self._state == WSState.STOPPED:
            _log("error", "WS", "Cannot restart stopped WebSocket. Create new instance.")
            return
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                _log("warning", "WS", "WebSocket thread already running")
                return
            self._should_stop.clear()
            self._state = WSState.CONNECTING
            self._thread = threading.Thread(
                target=self._run_forever, daemon=True, name="DeltaWebSocket"
            )
            self._thread.start()
            _log("info", "WS", f"WebSocket thread started (id={self._thread.ident})")

    def stop(self) -> None:
        _log("info", "WS", "Stopping WebSocket...")
        self._should_stop.set()
        self._state = WSState.STOPPED
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                _log("debug", "WS", f"Error closing WS: {e}")
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)
                if self._thread.is_alive():
                    _log("warning", "WS", "Thread did not exit within 5 s")
        _log("info", "WS", "WebSocket stopped")

    def is_connected(self) -> bool:
        return self._state == WSState.CONNECTED

    def get_state(self) -> str:
        return self._state.value

    def _get_channel_name(self, timeframe: str) -> str:
        return {
            "1m": "candlestick_1m",
            "5m": "candlestick_5m",
            "15m": "candlestick_15m",
            "1h": "candlestick_1h",
        }.get(timeframe, f"candlestick_{timeframe}")

    def _run_forever(self) -> None:
        first_connect = True
        while not self._should_stop.is_set():
            try:
                self._connect()
                if not self._should_stop.is_set() and self._state != WSState.STOPPED:
                    if not first_connect and self._on_reconnect_callback:
                        try:
                            self._on_reconnect_callback()
                        except Exception as e:
                            _log("error", "WS", f"Reconnect callback error: {e}")
                    first_connect = False
                    self._reconnect()
            except Exception as e:
                _log("error", "WS", f"Unexpected error in WS loop: {e}")
                if not self._should_stop.is_set():
                    first_connect = False
                    self._reconnect()

    def _connect(self) -> None:
        self._state = WSState.CONNECTING
        _log("info", "WS", f"Connecting to {self.config.url}")
        self._ws = websocket.WebSocketApp(
            self.config.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._ws.run_forever(
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            reconnect=0,
        )

    def _reconnect(self) -> None:
        if self._reconnect_attempts >= self.config.max_reconnect_attempts:
            _log("error", "WS",
                 f"Max reconnect attempts ({self.config.max_reconnect_attempts}) reached.")
            self._state = WSState.DISCONNECTED
            if self._on_disconnected_callback:
                try:
                    self._on_disconnected_callback()
                except Exception as e:
                    _log("error", "WS", f"Disconnected callback error: {e}")
            return

        self._reconnect_attempts += 1
        self._state = WSState.RECONNECTING
        _log("warning", "WS",
             f"Reconnecting in {self._reconnect_delay}s "
             f"(attempt {self._reconnect_attempts}/{self.config.max_reconnect_attempts})")

        for _ in range(self._reconnect_delay):
            if self._should_stop.is_set():
                return
            time.sleep(1)

        self._reconnect_delay = min(
            self._reconnect_delay * self.config.reconnect_backoff_multiplier,
            self.config.reconnect_max_delay,
        )

    def _on_open(self, ws) -> None:
        self._state = WSState.CONNECTED
        self._reconnect_attempts = 0
        self._reconnect_delay    = self.config.reconnect_base_delay
        _log("info", "WS", f"Connected to Delta Exchange India ({self.config.url})")

        with self._subscription_lock:
            for channel, symbols in self._subscriptions.items():
                if symbols:
                    self._send_subscription(channel, symbols)

        if self._on_connected_callback:
            try:
                self._on_connected_callback()
            except Exception as e:
                _log("error", "WS", f"Connected callback error: {e}")

    def _send_subscription(self, channel: str, symbols: List[str]) -> None:
        if not self._ws:
            return
        msg = {
            "type": "subscribe",
            "payload": {
                "channels": [{"name": channel, "symbols": symbols}]
            },
        }
        try:
            self._ws.send(json.dumps(msg))
            _log("info", "WS", f"Subscribed: channel={channel}, symbols={symbols}")
        except Exception as e:
            _log("error", "WS", f"Failed to send subscription: {e}")

    def _on_message(self, ws, message: str) -> None:
        _log("debug", "WS-RAW", f"{message[:500]}")
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")

            if msg_type == "subscribe":
                _log("info", "WS-MSG", f"Subscription response: {data.get('result','?')}")
                return
            if msg_type == "error":
                _log("error", "WS-MSG", f"Error response: {data.get('message', data)}")
                return

            candle_data = self._parse_candle_message(data)
            if candle_data and self._on_candle_callback:
                symbol = candle_data.get("symbol")
                candle = candle_data.get("candle")
                if symbol and candle:
                    try:
                        self._on_candle_callback(symbol, candle)
                    except Exception as e:
                        _log("error", "WS", f"Candle callback error: {e}")
        except json.JSONDecodeError as e:
            _log("debug", "WS", f"Invalid JSON: {e}")
        except Exception as e:
            _log("error", "WS", f"Message processing error: {e}")

    def _parse_candle_message(self, data: dict) -> Optional[dict]:
        msg_type = data.get("type", "")
        if not msg_type.startswith("candlestick_"):
            return None
        ws_symbol = data.get("symbol", "")
        if not ws_symbol:
            _log("warning", "WS-PARSE", f"Candlestick message missing symbol: {data}")
            return None
        trading_symbol = ws_symbol if ws_symbol.endswith("_PERP") else ws_symbol + "_PERP"
        candle = self._normalize_candle_flat(data)
        if candle is None:
            return None
        return {"symbol": trading_symbol, "candle": candle}

    def _normalize_candle_flat(self, data: dict) -> Optional[dict]:
        try:
            ts_raw = data.get("candle_start_time")
            if ts_raw is not None:
                ts = int(ts_raw)
                if ts > 1_000_000_000_000_000:
                    ts = ts // 1_000_000_000
                elif ts > 10_000_000_000:
                    ts = ts // 1_000
            else:
                for key in ("time", "start", "t"):
                    v = data.get(key)
                    if v is not None:
                        ts = int(float(v))
                        if ts > 10_000_000_000:
                            ts = ts // 1_000
                        break
                else:
                    _log("warning", "WS-PARSE", f"No timestamp in candle: {data}")
                    return None
            if ts <= 0:
                return None
            return {
                "time":   ts,
                "open":   float(data.get("open",   0)),
                "high":   float(data.get("high",   0)),
                "low":    float(data.get("low",    0)),
                "close":  float(data.get("close",  0)),
                "volume": float(data.get("volume", 0)),
            }
        except (TypeError, ValueError) as exc:
            _log("warning", "WS-PARSE", f"Failed to normalise candle: {exc}")
            return None

    def _on_error(self, ws, error) -> None:
        error_msg = str(error)
        if "10054" in error_msg or "Connection reset" in error_msg:
            _log("info", "WS", "Connection reset (normal disconnect)")
        else:
            _log("error", "WS", f"Error: {error_msg}")
        if self._on_error_callback:
            try:
                self._on_error_callback(error_msg)
            except Exception as e:
                _log("error", "WS", f"Error callback error: {e}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._state = WSState.DISCONNECTED
        _log("warning", "WS",
             f"Disconnected (code={close_status_code}, msg={close_msg})")
        if self._on_disconnected_callback:
            try:
                self._on_disconnected_callback()
            except Exception as e:
                _log("error", "WS", f"Disconnected callback error: {e}")


# ================================================================
#  5.  SYMBOL HANDLING
# ================================================================

def to_trading_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"):
        s = s[:-5]
    if s.endswith("USDT"):
        s = s[:-4] + "USD"
    return s + "_PERP"


def to_ws_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"):
        s = s[:-5]
    if s.endswith("USDT"):
        s = s[:-4] + "USD"
    return s


def to_candle_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"):
        s = s[:-5]
    if s.endswith("USDT"):
        s = s[:-4] + "USD"
    return s


# ================================================================
#  6.  CONSTANTS
# ================================================================
REST_BASE_INDIA  = "https://api.india.delta.exchange"
REST_BASE_GLOBAL = "https://api.delta.exchange"

CANDLE_LIMIT        = 20
MAX_RETRIES         = 3
RETRY_DELAYS        = [5, 10, 15]
TIMEOUT             = 30
CANDLE_SAFETY_SHIFT = 1

# FIX: Fill poll settings — market orders need time to confirm fill
FILL_POLL_INTERVAL  = 0.5   # seconds between poll attempts
FILL_POLL_TIMEOUT   = 15    # max seconds to wait for fill confirmation

TIMEFRAME_MAP: Dict[str, Dict] = {
    "1m":  {"resolution": "1m",  "api_resolution": "1m",  "ws_channel": "candlestick_1m",  "secs": 60},
    "5m":  {"resolution": "5m",  "api_resolution": "5m",  "ws_channel": "candlestick_5m",  "secs": 300},
    "15m": {"resolution": "15m", "api_resolution": "15m", "ws_channel": "candlestick_15m", "secs": 900},
    "1h":  {"resolution": "1h",  "api_resolution": "1h",  "ws_channel": "candlestick_1h",  "secs": 3600},
}


# ================================================================
#  7.  CONNECTION WARM-UP
# ================================================================

def warm_up_connection() -> None:
    try:
        _log("info", "WARM-UP", "Pre-warming connection to Delta Exchange India...")
        resp = requests.get(REST_BASE_INDIA, timeout=5)
        _log("info", "WARM-UP", f"Warmed up (status={resp.status_code})")
    except Exception as exc:
        _log("warning", "WARM-UP", f"Warm-up failed (non-critical): {exc}")


# ================================================================
#  8.  API REQUEST HANDLER
# ================================================================

class APIRequestHandler:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.session    = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent":   "python-DeltaBot/10.0",
            "Accept":       "application/json",
            "Connection":   "keep-alive",
        })
        self.session.mount("https://", requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=40, max_retries=0, pool_block=False
        ))

    def _get_base_url(self, endpoint_type: str) -> str:
        return REST_BASE_INDIA

    def _sign_request(
        self, method: str, path: str,
        params: dict = None, body: dict = None
    ) -> dict:
        if not self.api_key or not self.api_secret:
            raise ValueError("API key/secret missing")

        # FIX: Always generate a FRESH timestamp per sign call (never reuse stale ones)
        timestamp = str(int(time.time()))

        query_string = ""
        if params:
            sorted_params = sorted(params.items())
            query_string  = "?" + urlencode(sorted_params)

        body_string = ""
        if body and method.upper() != "GET":
            body_string = json.dumps(body)

        message   = method.upper() + timestamp + path + query_string + body_string
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        _log("debug", "AUTH", f"sig_input={message[:120]}...")
        return {
            "api-key":   self.api_key,
            "timestamp": timestamp,
            "signature": signature,
        }

    def request(
        self,
        method:        str,
        endpoint:      str,
        endpoint_type: str  = "public",
        params:        dict = None,
        body:          dict = None,
        retry_count:   int  = 0,
    ) -> Optional[Dict]:
        base_url = self._get_base_url(endpoint_type)
        url      = base_url + endpoint
        headers  = {}

        if endpoint_type == "private":
            # FIX: sign_request called here — fresh timestamp every attempt (including retries)
            headers = self._sign_request(method, endpoint, params, body)
        if body and method.upper() != "GET":
            headers["Content-Type"] = "application/json"

        _log("info", "API-REQ",
             f"{method} {endpoint} | type={endpoint_type} | attempt={retry_count + 1}")
        try:
            if method == "GET":
                response = self.session.get(url, headers=headers, params=params, timeout=TIMEOUT)
            elif method == "POST":
                response = self.session.post(url, headers=headers, json=body, timeout=TIMEOUT)
            elif method == "PUT":
                response = self.session.put(url, headers=headers, json=body, timeout=TIMEOUT)
            elif method == "DELETE":
                response = self.session.delete(url, headers=headers, json=body, timeout=TIMEOUT)
            else:
                raise ValueError(f"Unsupported method: {method}")

            _log("debug", "API-RESP", f"URL={response.url} status={response.status_code}")
            try:
                response_data = response.json()
            except Exception:
                response_data = {"error": response.text}

            if response.status_code == 401:
                _log("error", "AUTH", f"Authentication failed (401). Response: {response_data}")
                return None

            response.raise_for_status()
            return response_data

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            _log("warning", "API-REQ",
                 f"Request failed (attempt {retry_count + 1}/{MAX_RETRIES}): {exc}")
            if retry_count < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[retry_count]
                _log("info", "API-REQ", f"Retrying in {delay}s...")
                time.sleep(delay)
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            _log("error", "API-REQ", f"Request failed after {MAX_RETRIES} attempts")
            return None

        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else 0
            _log("error", "API-REQ", f"HTTP {status}: {exc}")
            if exc.response:
                try:
                    _log("error", "API-REQ", f"Response: {exc.response.json()}")
                except Exception:
                    _log("error", "API-REQ", f"Response text: {exc.response.text}")
            if status == 429 and retry_count < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[retry_count]
                _log("warning", "API-REQ", f"Rate limited. Retrying in {delay}s...")
                time.sleep(delay)
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            return None

        except Exception as exc:
            _log("error", "API-REQ", f"Unexpected error: {exc}")
            return None


# ================================================================
#  9.  TIMEFRAME SAFETY
# ================================================================

class TimeframeSafe:
    def __init__(self, key: str):
        k = key.strip().lower()
        if k not in TIMEFRAME_MAP:
            raise ValueError(f"[TIMEFRAME] '{k}' not valid. Choose from: {list(TIMEFRAME_MAP)}")
        entry = TIMEFRAME_MAP[k]
        self._key            = k
        self._resolution     = entry["resolution"]
        self._api_resolution = entry["api_resolution"]
        self._ws_channel     = entry["ws_channel"]
        self._secs           = entry["secs"]
        self._validate()

    def _validate(self) -> None:
        expected_ws = f"candlestick_{self._resolution}"
        if self._ws_channel != expected_ws:
            raise RuntimeError(
                f"[TIMEFRAME] MISMATCH: resolution='{self._resolution}' "
                f"ws_channel='{self._ws_channel}' (expected '{expected_ws}')"
            )
        _log("info", "TIMEFRAME",
             f"Validated: key={self._key}  api_resolution={self._api_resolution}  "
             f"ws_channel={self._ws_channel}  secs={self._secs}")

    @property
    def key(self)            -> str: return self._key
    @property
    def resolution(self)     -> str: return self._resolution
    @property
    def api_resolution(self) -> str: return self._api_resolution
    @property
    def ws_channel(self)     -> str: return self._ws_channel
    @property
    def secs(self)           -> int: return self._secs


# ================================================================
#  10. CANDLE VALIDATOR
# ================================================================

_CANDLE_FIELDS = ("time", "open", "high", "low", "close", "volume")


def validate_candle(candle: dict, symbol: str = "") -> bool:
    tag = f"CANDLE [{symbol}]" if symbol else "CANDLE"
    for f in _CANDLE_FIELDS:
        if f not in candle or candle[f] is None:
            _log("warning", tag, f"Missing/None field '{f}'")
            return False
    for f in ("open", "high", "low", "close", "volume"):
        try:
            v = float(candle[f])
        except (TypeError, ValueError):
            _log("warning", tag, f"Non-numeric '{f}'={candle[f]!r}")
            return False
        if math.isnan(v) or math.isinf(v):
            _log("warning", tag, f"NaN/Inf in '{f}'={v}")
            return False
    if candle["time"] <= 0:
        _log("warning", tag, f"Bad timestamp time={candle['time']}")
        return False
    if candle["high"] < candle["low"]:
        _log("warning", tag, f"high({candle['high']}) < low({candle['low']})")
        return False
    for f in ("open", "high", "low", "close"):
        if candle[f] <= 0:
            _log("warning", tag, f"Non-positive '{f}'={candle[f]}")
            return False
    return True


# ================================================================
#  11. STRATEGY HELPERS
# ================================================================

def candle_body(c: dict) -> float:
    return abs(c["close"] - c["open"])


def upper_wick(c: dict) -> float:
    return c["high"] - max(c["open"], c["close"])


def lower_wick(c: dict) -> float:
    return min(c["open"], c["close"]) - c["low"]


def candle_range(c: dict) -> float:
    return c["high"] - c["low"]


def check_short_signal_strategy_1(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    STRATEGY 1: Breakout with open=previous close AND close=previous open.
    Conditions:
        1. i["open"] == i1["close"]   (open equals previous close)
        2. i["close"] == i1["open"]   (close equals previous open)
        3. i1["close"] > i2["high"]   (2nd breakout)
        4. i2["close"] > i3["high"]   (1st breakout)
    """
    if len(candles) < 4:
        return False, None, ""

    i  = candles[-1]
    i1 = candles[-2]
    i2 = candles[-3]
    i3 = candles[-4]

    conditions = (
        abs(i["open"] - i1["close"]) < 0.0001 and
        abs(i["close"] - i1["open"]) < 0.0001 and
        i1["close"] > i2["high"] and
        i2["close"] > i3["high"]
    )

    if conditions:
        return True, i, "STRATEGY_1"
    return False, None, ""


def check_short_signal_strategy_2(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    STRATEGY 2: 3-candle breakout with upper wick >= 1.8x body.
    """
    if len(candles) < 3:
        return False, None, ""

    i   = candles[-1]
    i1  = candles[-2]
    i2  = candles[-3]

    body_i       = candle_body(i)
    upper_wick_i = upper_wick(i)

    first_breakout  = i1["close"] > i2["high"]
    second_breakout = i["close"] > i1["high"]

    wick_condition = False
    if body_i > 0:
        ratio = upper_wick_i / body_i
        wick_condition = ratio >= 1.8

    conditions = first_breakout and second_breakout and wick_condition

    if conditions:
        ratio = upper_wick_i / body_i if body_i > 0 else 0
        _log("info", "STRATEGY_2",
             f"Signal triggered! ratio={ratio:.2f}x | "
             f"body={body_i:.4f} | wick={upper_wick_i:.4f}")
        return True, i, "STRATEGY_2"
    return False, None, ""


def check_short_signal_strategy_3(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    STRATEGY 3: Bearish Engulfing Pattern with uptrend confirmation.
    """
    if len(candles) < 4:
        return False, None, ""

    i   = candles[-1]
    i1  = candles[-2]
    i2  = candles[-3]
    i3  = candles[-4]

    uptrend      = i1["close"] > i2["close"] > i3["close"]
    prev_bullish = i1["close"] > i1["open"]
    curr_bearish = i["close"] < i["open"]
    engulfing    = (i["open"] > i1["close"]) and (i["close"] < i1["open"])

    conditions = uptrend and prev_bullish and curr_bearish and engulfing

    if conditions:
        _log("info", "STRATEGY_3",
             f"Bearish Engulfing detected! | "
             f"i1(O={i1['open']:.4f},C={i1['close']:.4f}) | "
             f"i(O={i['open']:.4f},C={i['close']:.4f})")
        return True, i, "STRATEGY_3"
    return False, None, ""


def check_short_signal_strategy_4(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    STRATEGY 4: Three Black Crows Pattern.
    Three consecutive long-bodied bearish candles, each opening within the
    previous body and closing progressively lower, with minimal wicks.
    """
    if len(candles) < 3:
        return False, None, ""

    i   = candles[-1]
    i1  = candles[-2]
    i2  = candles[-3]

    all_bearish = (
        i["close"] < i["open"] and
        i1["close"] < i1["open"] and
        i2["close"] < i2["open"]
    )
    if not all_bearish:
        return False, None, ""

    def has_low_wicks(candle: dict) -> bool:
        r = candle_range(candle)
        if r > 0:
            return (upper_wick(candle) / r <= 0.05) and (lower_wick(candle) / r <= 0.05)
        return (upper_wick(candle) < 0.0001) and (lower_wick(candle) < 0.0001)

    if not (has_low_wicks(i) and has_low_wicks(i1) and has_low_wicks(i2)):
        return False, None, ""

    open_in_prev_body = (
        i1["open"] >= i2["close"] and i1["open"] < i2["open"] and
        i["open"] >= i1["close"] and i["open"] < i1["open"]
    )
    if not open_in_prev_body:
        return False, None, ""

    progressive_lower_closes = (i1["close"] < i2["close"]) and (i["close"] < i1["close"])
    if not progressive_lower_closes:
        return False, None, ""

    highest_high   = max(i2["high"], i1["high"], i["high"])
    total_decline  = ((i2["close"] - i["close"]) / i2["close"]) * 100
    _log("info", "THREE_BLACK_CROWS",
         f"Pattern detected! | Total decline: {total_decline:.2f}% | SL: {highest_high:.4f}")

    signal_candle = i.copy()
    signal_candle["pattern_high"] = highest_high
    return True, signal_candle, "THREE_BLACK_CROWS"


def check_short_signal(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """Master signal checker — OR combination of all 4 strategies."""
    for checker in (
        check_short_signal_strategy_1,
        check_short_signal_strategy_2,
        check_short_signal_strategy_3,
        check_short_signal_strategy_4,
    ):
        triggered, signal_candle, strategy = checker(candles)
        if triggered:
            _log("info", "SIGNAL", f"Triggered by {strategy}")
            return True, signal_candle, strategy
    return False, None, ""


# ================================================================
#  12. SYMBOL VALIDATOR
# ================================================================

class SymbolValidator:
    def __init__(self, product_map: Dict[str, int]):
        self.product_map   = product_map
        self.known_symbols = list(product_map.keys())

    def validate_trading_symbol(self, symbol: str) -> Tuple[Optional[str], str]:
        trading_sym = to_trading_symbol(symbol)
        if trading_sym in self.product_map:
            return trading_sym, f"OK (trading format: {trading_sym})"
        matches = difflib.get_close_matches(
            trading_sym, self.known_symbols, n=3, cutoff=0.5
        )
        if matches:
            best = matches[0]
            return best, f"CLOSEST MATCH {symbol} -> {best}"
        return None, f"NO MATCH for '{symbol}'"

    def validate_list(self, symbols: List[str]) -> List[str]:
        valid: List[str] = []
        seen: set = set()
        for raw in symbols:
            resolved, msg = self.validate_trading_symbol(raw)
            if resolved:
                _log("info", "SYMBOL", f"{raw} -> {resolved} | {msg}")
                if resolved not in seen:
                    valid.append(resolved)
                    seen.add(resolved)
            else:
                _log("warning", "SYMBOL", f"{raw}: {msg}")
        return valid


# ================================================================
#  13. TIME-RANGE HELPER
# ================================================================

def get_time_range(num_candles: int, timeframe_minutes: int) -> Tuple[int, int]:
    secs_per_candle = timeframe_minutes * 60
    now             = int(time.time())
    aligned_now     = (now // secs_per_candle) * secs_per_candle
    safe_end        = aligned_now - (CANDLE_SAFETY_SHIFT * secs_per_candle)
    start           = safe_end - (num_candles * secs_per_candle)
    start           = max(1, start)
    safe_end        = max(secs_per_candle * (CANDLE_SAFETY_SHIFT + 1), safe_end)
    if start >= safe_end:
        raise ValueError(f"start={start} >= end={safe_end}")
    _log("info", "TIME-RANGE",
         f"num_candles={num_candles} tf={timeframe_minutes}m "
         f"start={start} end={safe_end}")
    return start, safe_end


def get_time_range_with_retry_shift(
    num_candles: int, timeframe_minutes: int, shift_candles: int = 0
) -> Tuple[int, int]:
    start, end = get_time_range(num_candles, timeframe_minutes)
    if shift_candles > 0:
        secs  = shift_candles * timeframe_minutes * 60
        start = max(1, start - secs)
        end   = max(timeframe_minutes * 60, end - secs)
        _log("info", "TIME-RANGE",
             f"Retry shift +{shift_candles} candles -> start={start} end={end}")
    return start, end


# ================================================================
#  14. CANDLE PARSING HELPERS
# ================================================================

def _extract_timestamp(src: dict) -> int:
    for key in ("start", "time", "open_time", "t", "timestamp"):
        v = src.get(key)
        if v is not None:
            try:
                ts = int(float(v))
                if ts > 0:
                    return ts
            except (TypeError, ValueError):
                continue
    return 0


def _extract_price(src: dict, long_key: str, short_key: str) -> Optional[float]:
    for k in (long_key, short_key):
        v = src.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None


def _parse_rest_candle_row(row: dict, symbol: str = "") -> Optional[dict]:
    ts = _extract_timestamp(row)
    if ts <= 0:
        _log("warning", f"CANDLES [{symbol}]", f"No valid timestamp in REST row: {row}")
        return None
    o = _extract_price(row, "open",   "o")
    h = _extract_price(row, "high",   "h")
    l = _extract_price(row, "low",    "l")
    c = _extract_price(row, "close",  "c")
    v = _extract_price(row, "volume", "v") or 0.0
    if any(x is None for x in (o, h, l, c)):
        _log("warning", f"CANDLES [{symbol}]", f"Missing OHLC in REST row: {row}")
        return None
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


# ================================================================
#  15. TICK SIZE ROUNDING  [FIX: round to product tick size, not 2dp]
# ================================================================

def round_to_tick(price: float, tick_size: float) -> float:
    """
    Round price to the nearest valid tick increment.
    e.g. price=91503.7, tick_size=0.5 -> 91503.5
    """
    if tick_size <= 0:
        return round(price, 2)
    rounded = round(price / tick_size) * tick_size
    # Determine decimal places from tick_size to avoid float artifacts
    tick_str  = f"{tick_size:.10f}".rstrip("0")
    dp        = len(tick_str.split(".")[-1]) if "." in tick_str else 0
    return round(rounded, dp)


# ================================================================
#  16. DELTA REST CLIENT
# ================================================================

class DeltaREST:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.request_handler = APIRequestHandler(api_key, api_secret)
        # FIX: cache tick sizes from the product catalogue
        self._tick_sizes: Dict[str, float] = {}

    def verify_account(self) -> Optional[dict]:
        result = self.request_handler.request("GET", "/v2/profile", endpoint_type="private")
        if result and "result" in result:
            profile = result["result"]
            _log("info", "AUTH",
                 f"Authenticated: {profile.get('email','?')} | {profile.get('name','?')}")
            return profile
        return None

    def get_usd_balance(self) -> float:
        result = self.request_handler.request(
            "GET", "/v2/wallet/balances", endpoint_type="private"
        )
        if result and "result" in result:
            for asset in result.get("result", []):
                if asset.get("asset_symbol") in ("USDT", "USD"):
                    bal = float(asset.get("available_balance", 0))
                    _log("info", "BALANCE",
                         f"Balance ({asset.get('asset_symbol')}) available: {bal:,.2f}")
                    return bal
        return 0.0

    def fetch_product_map(self) -> Dict[str, int]:
        result = self.request_handler.request("GET", "/v2/products", endpoint_type="public")
        pmap: Dict[str, int] = {}
        if result and "result" in result:
            for item in result.get("result", []):
                sym      = item.get("symbol", "")
                pid      = item.get("id")
                tick_raw = item.get("tick_size", "0.01")
                if sym and pid is not None:
                    pmap[sym] = int(pid)
                    try:
                        # FIX: store tick size for every product
                        self._tick_sizes[sym] = float(tick_raw)
                    except (TypeError, ValueError):
                        self._tick_sizes[sym] = 0.01
        _log("info", "CATALOG", f"Loaded {len(pmap)} products")
        return pmap

    def get_tick_size(self, symbol: str) -> float:
        """Return tick size for symbol; defaults to 0.01 if unknown."""
        return self._tick_sizes.get(symbol, 0.01)

    def get_order(self, order_id: int) -> Optional[dict]:
        """Fetch a single order by ID — used to confirm fill."""
        result = self.request_handler.request(
            "GET", f"/v2/orders/{order_id}", endpoint_type="private"
        )
        if result and "result" in result:
            return result["result"]
        return None

    def wait_for_fill(
        self, order_id: int, symbol: str = ""
    ) -> Tuple[bool, int]:
        """
        FIX BUG #1 & #2: Poll until the entry order is fully filled.
        Returns (filled: bool, actual_filled_size: int).
        Market orders typically fill within 1–2 polls; we allow up to
        FILL_POLL_TIMEOUT seconds before giving up and rolling back.
        """
        deadline = time.time() + FILL_POLL_TIMEOUT
        _log("info", "FILL-POLL",
             f"Waiting for fill: order_id={order_id} symbol={symbol} "
             f"timeout={FILL_POLL_TIMEOUT}s")

        while time.time() < deadline:
            order = self.get_order(order_id)
            if order is None:
                _log("warning", "FILL-POLL", "Could not fetch order — retrying...")
                time.sleep(FILL_POLL_INTERVAL)
                continue

            state         = order.get("state", "")
            size          = int(order.get("size", 0))
            unfilled_size = int(order.get("unfilled_size", 0))
            filled_size   = size - unfilled_size

            _log("info", "FILL-POLL",
                 f"order_id={order_id} state={state} "
                 f"size={size} unfilled={unfilled_size} filled={filled_size}")

            if state == "closed" and unfilled_size == 0:
                _log("info", "FILL-POLL",
                     f"Order fully filled: {filled_size} contracts")
                return True, filled_size

            if state == "cancelled":
                _log("error", "FILL-POLL",
                     f"Order cancelled (order_id={order_id}). No position opened.")
                return False, 0

            if state == "rejected":
                reason = order.get("cancel_reason", "unknown")
                _log("error", "FILL-POLL",
                     f"Order rejected: {reason} (order_id={order_id})")
                return False, 0

            # "open" or "pending" — market order partially/not yet filled; keep polling
            time.sleep(FILL_POLL_INTERVAL)

        _log("error", "FILL-POLL",
             f"Fill confirmation timed out after {FILL_POLL_TIMEOUT}s "
             f"for order_id={order_id}")
        return False, 0

    def place_order(
        self,
        product_id:  int,
        side:        str,
        size:        int,
        order_type:  str            = "market_order",
        limit_price: Optional[float] = None,
    ) -> dict:
        if side not in ("buy", "sell"):
            _log("error", "ORDER", f"Invalid side '{side}'.")
            return {"error": "invalid_side"}
        if size < 1:
            _log("error", "ORDER", f"Invalid size {size}.")
            return {"error": "invalid_size"}

        body: Dict = {
            "product_id": product_id,
            "size":       size,
            "side":       side,
            "order_type": order_type,
        }
        if limit_price and order_type == "limit_order":
            body["limit_price"] = str(limit_price)

        result = self.request_handler.request(
            "POST", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def place_bracket_stop_loss(
        self,
        product_id:    int,
        stop_price:    float,
        symbol:        str = "",
    ) -> dict:
        """
        FIX: Use the dedicated bracket order endpoint to attach a stop-loss
        to an existing open position. This is the recommended approach per
        Delta Exchange docs — it auto-adjusts size and auto-cancels on close.

        bracket_stop_trigger_method = last_traded_price avoids mark-price lag.
        """
        if stop_price <= 0:
            _log("error", "BRACKET-SL", f"Invalid stop_price {stop_price}")
            return {"error": "invalid_stop_price"}

        # FIX: round stop price to the product's actual tick size
        tick_size   = self.get_tick_size(symbol) if symbol else 0.01
        rounded_sl  = round_to_tick(stop_price, tick_size)

        body: Dict = {
            "product_id": product_id,
            "stop_loss_order": {
                "order_type": "market_order",
                "stop_price": str(rounded_sl),
            },
            "bracket_stop_trigger_method": "last_traded_price",
        }
        _log("info", "BRACKET-SL",
             f"Placing bracket SL: pid={product_id} "
             f"stop={rounded_sl} (tick={tick_size}) trigger=LTP")
        result = self.request_handler.request(
            "POST", "/v2/orders/bracket", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def cancel_order(self, order_id: int, product_id: int) -> dict:
        """Cancel a specific order by ID — used for SL cleanup on close."""
        body   = {"id": order_id, "product_id": product_id}
        result = self.request_handler.request(
            "DELETE", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def get_top_symbols(
        self,
        product_map: Dict[str, int],
        mode:        str  = "volatile",
        limit:       int  = 5,
        perp_only:   bool = True,
    ) -> List[str]:
        result  = self.request_handler.request("GET", "/v2/tickers", endpoint_type="public")
        tickers = result.get("result", []) if result else []
        ranked: List[Tuple[float, str]] = []
        for t in tickers:
            sym = t.get("symbol", "")
            if perp_only and "_PERP" not in sym:
                continue
            if sym not in product_map:
                continue
            try:
                score = (
                    abs(float(t.get("change", 0) or 0))
                    if mode == "volatile"
                    else float(t.get("volume", 0) or 0)
                )
                ranked.append((score, sym))
            except (TypeError, ValueError):
                continue
        ranked.sort(reverse=True)
        top = [s for _, s in ranked[:limit]]
        _log("info", "AUTO-SYM", f"Auto-selected top {limit} by {mode}: {top}")
        return top

    def get_candles_with_retry(
        self, symbol: str, resolution: str = "1h", limit: int = CANDLE_LIMIT
    ) -> List[dict]:
        candles = self.get_candles(symbol, resolution, limit, shift_candles=0)
        if not candles:
            for shift in range(1, MAX_RETRIES + 1):
                _log("warning", "CANDLES",
                     f"Empty candles for {symbol}, retry shift={shift}")
                time.sleep(RETRY_DELAYS[shift - 1])
                candles = self.get_candles(symbol, resolution, limit, shift_candles=shift)
                if candles:
                    _log("info", "CANDLES",
                         f"Got {len(candles)} candles on retry shift={shift}")
                    break
        return candles

    def get_candles(
        self,
        symbol:        str,
        resolution:    str = "1h",
        limit:         int = CANDLE_LIMIT,
        shift_candles: int = 0,
    ) -> List[dict]:
        candle_symbol     = to_candle_symbol(symbol)
        tf_entry          = next(
            (tf for tf in TIMEFRAME_MAP.values()
             if tf["api_resolution"] == resolution or tf["resolution"] == resolution),
            TIMEFRAME_MAP["1h"],
        )
        timeframe_minutes = tf_entry["secs"] // 60
        api_resolution    = tf_entry["api_resolution"]
        safe_limit        = min(limit, 20)

        try:
            start, end = get_time_range_with_retry_shift(
                safe_limit, timeframe_minutes, shift_candles
            )
        except ValueError as exc:
            _log("error", "CANDLES", f"Timestamp validation failed: {exc}")
            return []

        params = {
            "resolution": api_resolution,
            "symbol":     candle_symbol,
            "start":      start,
            "end":        end,
        }
        _log("info", "CANDLES",
             f"Fetching {safe_limit} candles | symbol={candle_symbol} "
             f"res={api_resolution} start={start} end={end} shift={shift_candles}")

        result = self.request_handler.request(
            "GET", "/v2/history/candles", endpoint_type="public", params=params
        )
        if not result:
            _log("warning", "CANDLES", f"No response for {candle_symbol}")
            return []

        raw_result = result.get("result", [])
        if isinstance(raw_result, dict):
            for key in ("candles", "data", "ohlcv"):
                if key in raw_result:
                    raw_result = raw_result[key]
                    break
        if not isinstance(raw_result, list) or not raw_result:
            _log("warning", "CANDLES",
                 f"No candle data for {candle_symbol} start={start} end={end}")
            return []

        candles = []
        for row in raw_result:
            candle = _parse_rest_candle_row(row, candle_symbol)
            if candle and validate_candle(candle, candle_symbol):
                candles.append(candle)

        candles.sort(key=lambda x: x["time"])
        _log("info", "CANDLES", f"Loaded {len(candles)} candles for {candle_symbol}")
        return candles

    def set_leverage(self, product_id: int, leverage: int) -> bool:
        body   = {"leverage": str(leverage)}
        result = self.request_handler.request(
            "POST", f"/v2/products/{product_id}/orders/leverage",
            endpoint_type="private", body=body,
        )
        if result and "result" in result:
            _log("info", "LEVERAGE", f"product_id={product_id} {leverage}x OK")
            return True
        _log("error", "LEVERAGE",
             f"FAILED for product_id={product_id} — response: {result}")
        return False

    def close_position(self, product_id: int, size: int, side: str = "buy") -> dict:
        _log("info", "CLOSE",
             f"Closing position: product_id={product_id} size={size} side={side}")
        result = self.place_order(
            product_id=product_id, side=side,
            size=size, order_type="market_order",
        )
        if result and "error" not in result:
            _log("info", "CLOSE", "Position closed OK")
        else:
            _log("error", "CLOSE", f"Close failed: {result}")
        return result


# ================================================================
#  17. POSITION SIZER
# ================================================================

def compute_position_size(
    entry_price:     float,
    stop_loss_price: float,
    account_balance: float,
    risk_pct:        float,
    leverage:        int,
) -> Tuple[int, dict]:
    risk_amount   = account_balance * risk_pct
    stop_distance = abs(entry_price - stop_loss_price)

    if stop_distance <= 0:
        _log("warning", "SIZER", "stop_distance=0 — cannot size position")
        return 0, {}

    risk_size      = risk_amount / stop_distance
    max_by_margin  = (account_balance * leverage) / entry_price
    final_size_raw = min(risk_size, max_by_margin)
    final_size     = max(1, int(final_size_raw))

    margin_used  = (final_size * entry_price) / leverage
    max_loss_est = final_size * stop_distance

    diag = {
        "account_balance": round(account_balance, 2),
        "risk_pct":        round(risk_pct * 100, 2),
        "risk_amount":     round(risk_amount, 2),
        "entry_price":     round(entry_price, 4),
        "stop_loss_price": round(stop_loss_price, 4),
        "stop_distance":   round(stop_distance, 4),
        "risk_size_raw":   round(risk_size, 4),
        "max_by_margin":   round(max_by_margin, 4),
        "final_size":      final_size,
        "margin_used":     round(margin_used, 2),
        "max_loss_est":    round(max_loss_est, 2),
        "leverage":        leverage,
    }
    _log("info", "SIZER",
         f"entry={entry_price:.4f} sl={stop_loss_price:.4f} "
         f"dist={stop_distance:.4f} risk_size={risk_size:.2f} "
         f"cap={max_by_margin:.2f} -> final={final_size} "
         f"margin_used=${margin_used:.2f} max_loss=${max_loss_est:.2f}")
    return final_size, diag


# ================================================================
#  18. TRADING BOT
# ================================================================

class TradingBot:
    def __init__(self, config: dict):
        self.config          = config
        self.paper           = config["paper_mode"]
        self.leverage        = config["leverage"]
        self.max_trades      = config.get("max_concurrent_trades", 2)
        self.api_key         = config.get("api_key",    "")
        self.api_secret      = config.get("api_secret", "")
        self.trading_capital = float(config.get("trading_capital", 0.0))
        self.risk_pct        = config["risk_pct"] / 100.0

        self._tf            = TimeframeSafe(config.get("timeframe", "1h"))
        self.timeframe      = self._tf.key
        self.resolution     = self._tf.resolution
        self.api_resolution = self._tf.api_resolution
        self.ws_channel     = self._tf.ws_channel

        self.rest = DeltaREST(self.api_key, self.api_secret)

        self.symbols:      List[str]          = []
        self.product_map:  Dict[str, int]     = {}
        self.candle_store: Dict[str, deque]   = {}

        self._trade_lock   = threading.Lock()
        self.active_trades: Dict[str, dict]  = {}

        self.signals:   List[dict] = []
        self.sl_events: List[dict] = []

        self.running     = False
        self.ws_manager: Optional[DeltaWebSocket] = None

        self.on_signal_callback = None
        self.on_trade_callback  = None
        self.on_log_callback    = None

    def _log(self, level: str, tag: str, msg: str) -> None:
        _log(level, tag, msg)
        if self.on_log_callback:
            try:
                self.on_log_callback(f"[{tag}] {msg}", level)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Start / Stop                                                        #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        self.running = True
        self._print_banner()
        warm_up_connection()

        if not self.paper:
            self._log("info", "STARTUP", "Verifying API credentials...")
            profile = self.rest.verify_account()
            if profile is None:
                self._log("error", "STARTUP", "Authentication failed. Aborting.")
                self.running = False
                return
            print(f"  [AUTH] Account : {profile.get('email', 'unknown')}")
            print(f"  [AUTH] Name    : {profile.get('name',  'unknown')}")
            print()

        self._log("info", "STARTUP", "Loading product catalogue...")
        raw_map = self.rest.fetch_product_map()
        if not raw_map:
            self._log("error", "STARTUP", "Product catalogue empty. Aborting.")
            self.running = False
            return
        self.product_map = raw_map

        raw_syms = self.config.get("symbols", [])
        if not raw_syms:
            self._log("info", "STARTUP", "No symbols — auto-selecting top volatile perps")
            raw_syms = self.rest.get_top_symbols(
                self.product_map, mode="volatile", limit=5, perp_only=True
            ) or []

        validator    = SymbolValidator(self.product_map)
        self.symbols = validator.validate_list(raw_syms)
        if not self.symbols:
            self._log("error", "STARTUP", "No valid symbols. Aborting.")
            self.running = False
            return

        for sym in self.symbols:
            self.candle_store[sym] = deque(maxlen=200)
            self._log("info", "STARTUP", f"Tracking symbol: {sym}")

        if not self.paper:
            self._log("info", "STARTUP", f"Setting leverage {self.leverage}x...")
            for sym in self.symbols:
                pid = self.product_map.get(sym)
                if pid:
                    ok = self.rest.set_leverage(pid, self.leverage)
                    self._log("info" if ok else "warning", "LEVERAGE",
                              f"  {sym} pid={pid} {self.leverage}x {'OK' if ok else 'FAILED'}")
            print()

        self._print_startup_summary()
        self._fetch_all_historical()
        self._start_ws()

    def stop(self) -> None:
        self.running = False
        if self.ws_manager:
            self.ws_manager.stop()
        self._log("info", "BOT", "Bot stopped.")

    # ------------------------------------------------------------------ #
    #  Historical candle load                                              #
    # ------------------------------------------------------------------ #

    def _fetch_all_historical(self) -> None:
        self._log("info", "CANDLES",
                  f"Loading {CANDLE_LIMIT} x {self.timeframe} candles "
                  f"for {len(self.symbols)} symbol(s)")
        for sym in self.symbols:
            candles = self.rest.get_candles_with_retry(sym, self.api_resolution, CANDLE_LIMIT)
            if candles:
                for c in candles:
                    self.candle_store[sym].append(c)
                self._log("info", "CANDLES",
                          f"  [OK] {sym}: {len(candles)} candles loaded "
                          f"last_close={candles[-1]['close']:.4f}")
            else:
                self._log("warning", "CANDLES",
                          f"  [WARN] {sym}: 0 candles after {MAX_RETRIES} retries.")

    # ------------------------------------------------------------------ #
    #  WebSocket startup                                                   #
    # ------------------------------------------------------------------ #

    def _start_ws(self) -> None:
        self._log("info", "WS", f"Initialising WebSocket: {WSConfig().url}")
        self.ws_manager = DeltaWebSocket()
        self.ws_manager.set_candle_callback(self._on_ws_candle)
        self.ws_manager.set_connected_callback(self._on_ws_connected)
        self.ws_manager.set_disconnected_callback(self._on_ws_disconnected)
        self.ws_manager.set_error_callback(self._on_ws_error)
        self.ws_manager.set_reconnect_callback(self._on_ws_reconnect)

        ws_symbols = [to_ws_symbol(sym) for sym in self.symbols]
        self.ws_manager.subscribe(self.timeframe, ws_symbols)
        self.ws_manager.start()
        self._log("info", "WS",
                  f"Started | channel={self.ws_channel} | ws_symbols={ws_symbols}")

    # ------------------------------------------------------------------ #
    #  WebSocket callbacks                                                 #
    # ------------------------------------------------------------------ #

    def _on_ws_candle(self, symbol: str, candle: dict) -> None:
        self._log("debug", "WS-CANDLE",
                  f"{symbol} | t={candle.get('time')} "
                  f"O={candle.get('open')} H={candle.get('high')} "
                  f"L={candle.get('low')} C={candle.get('close')}")

        if symbol in self.candle_store:
            self.process_candle(symbol, candle)
            return

        for known in self.candle_store:
            if to_ws_symbol(known) == to_ws_symbol(symbol):
                self.process_candle(known, candle)
                return

        self._log("warning", "WS-CANDLE",
                  f"Untracked symbol: {symbol} (known: {list(self.candle_store.keys())})")

    def _on_ws_connected(self) -> None:
        self._log("info", "WS", "WebSocket connected")

    def _on_ws_disconnected(self) -> None:
        self._log("warning", "WS", "WebSocket disconnected (permanent — max retries reached)")

    def _on_ws_error(self, error: str) -> None:
        self._log("error", "WS", f"WebSocket error: {error}")

    def _on_ws_reconnect(self) -> None:
        self._log("info", "BACKFILL",
                  "WebSocket reconnected — backfilling missed candles...")
        for sym in self.symbols:
            self._backfill_symbol(sym)
        self._log("info", "BACKFILL", "Backfill complete.")

    def _backfill_symbol(self, symbol: str) -> None:
        store = self.candle_store.get(symbol)
        if store is None:
            return
        fresh = self.rest.get_candles_with_retry(symbol, self.api_resolution, CANDLE_LIMIT)
        if not fresh:
            self._log("warning", "BACKFILL", f"No fresh candles for {symbol}")
            return
        existing_ts = {c["time"] for c in store}
        added = 0
        for c in sorted(fresh, key=lambda x: x["time"]):
            if c["time"] not in existing_ts:
                store.append(c)
                existing_ts.add(c["time"])
                added += 1
        if added:
            self._log("info", "BACKFILL", f"{symbol}: merged {added} new candles from REST")
        else:
            self._log("info", "BACKFILL", f"{symbol}: no new candles needed")

    # ------------------------------------------------------------------ #
    #  Candle processing                                                   #
    # ------------------------------------------------------------------ #

    def process_candle(self, symbol: str, candle: dict) -> None:
        store = self.candle_store.get(symbol)
        if store is None:
            return

        if not validate_candle(candle, symbol):
            return

        # Forming candle update (same timestamp) — update in place
        if store and store[-1]["time"] == candle["time"]:
            store[-1] = candle
            if symbol in self.active_trades:
                self._check_stop_loss(symbol, candle)
            return

        # New candle arrived: store[-1] is now the JUST-CLOSED candle
        if store:
            closed = store[-1]
            self._log("info", "CANDLE-CLOSED",
                      f"{symbol} | {self.timeframe} | CLOSED | "
                      f"O={closed['open']:.2f} H={closed['high']:.2f} "
                      f"L={closed['low']:.2f} C={closed['close']:.2f} "
                      f"V={closed.get('volume', 0):.2f}")

            if symbol not in self.active_trades:
                triggered, signal_candle, strategy_name = check_short_signal(list(store))
                if triggered and signal_candle is not None:
                    self._on_signal(symbol, signal_candle, strategy_name)

        store.append(candle)

    # ------------------------------------------------------------------ #
    #  Local stop-loss fallback monitor                                    #
    # ------------------------------------------------------------------ #

    def _check_stop_loss(self, symbol: str, candle: dict) -> None:
        """
        Local SL fallback. Primary SL is the bracket order on the exchange.
        This fires only if the bracket order somehow failed to execute.
        """
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade:
            return

        if candle["high"] < trade["stop_loss"]:
            return

        entry    = trade["entry"]
        sl       = trade["stop_loss"]
        loss_pct = round((sl - entry) / entry * 100, 3)
        loss_usd = round(self.trading_capital * self.risk_pct, 2)

        print()
        print(f"  *** LOCAL SL TRIGGERED: {symbol} ***")
        print(f"      Entry       = {entry:.4f}")
        print(f"      Stop Loss   = {sl:.4f}")
        print(f"      High        = {candle['high']:.4f}")
        print(f"      Loss ~      = {loss_pct}%  (~${loss_usd:.2f})")
        print("      (Exchange bracket SL should have already fired)")
        print()

        self._log("warning", "SL",
                  f"LOCAL SL HIT | {symbol} | entry={entry:.4f} | "
                  f"sl={sl:.4f} | high={candle['high']:.4f} | loss~{loss_pct}%")

        ev = {
            "time":         datetime.now(timezone.utc).isoformat(),
            "symbol":       symbol,
            "entry":        entry,
            "stop_loss":    sl,
            "triggered_at": candle["high"],
            "loss_pct":     loss_pct,
            "loss_usd_est": loss_usd,
        }
        self.sl_events.append(ev)

        if not self.paper:
            pid  = trade.get("product_id")
            size = trade.get("size", 1)
            if pid and size > 0:
                self.rest.close_position(pid, size, side="buy")

        # FIX: Remove trade and clean up stale SL orders (if any)
        self._cleanup_trade(symbol)

    def _cleanup_trade(self, symbol: str) -> None:
        """
        FIX: Remove trade record cleanly. Bracket orders auto-cancel on
        the exchange side, so no explicit cancel needed for bracket SLs.
        """
        with self._trade_lock:
            self.active_trades.pop(symbol, None)
        self._log("info", "CLEANUP", f"Trade record removed for {symbol}")

    # ------------------------------------------------------------------ #
    #  Signal handler                                                      #
    # ------------------------------------------------------------------ #

    def _on_signal(self, symbol: str, signal_candle: dict, strategy_name: str) -> None:
        entry = signal_candle["close"]

        if strategy_name == "THREE_BLACK_CROWS" and "pattern_high" in signal_candle:
            sl = signal_candle["pattern_high"]
            self._log("info", "THREE_BLACK_CROWS",
                      f"Using pattern high {sl:.4f} as stop loss")
        else:
            sl = signal_candle["high"]

        rpu = abs(sl - entry)
        if rpu == 0:
            self._log("warning", "SIGNAL", f"risk_per_unit=0 for {symbol} — skip")
            return

        with self._trade_lock:
            if len(self.active_trades) >= self.max_trades:
                self._log("info", "SIGNAL",
                          f"Max trades ({self.max_trades}) reached — skip {symbol}")
                return
            if symbol in self.active_trades:
                self._log("info", "SIGNAL",
                          f"Already in trade on {symbol} — skip duplicate")
                return

            risk_usd = round(self.trading_capital * self.risk_pct, 2)

            signal = {
                "time":            datetime.now(timezone.utc).isoformat(),
                "symbol":          symbol,
                "direction":       "SHORT",
                "entry":           entry,
                "stop_loss":       sl,
                "timeframe":       self.timeframe,
                "mode":            "PAPER" if self.paper else "LIVE",
                "executed":        False,
                "product_id":      self.product_map.get(symbol),
                "risk_usd":        risk_usd,
                "trading_capital": self.trading_capital,
                "strategy":        strategy_name,
            }
            self.signals.append(signal)

            if self.paper:
                self.active_trades[symbol] = {
                    "entry":      entry,
                    "stop_loss":  sl,
                    "size":       0,
                    "product_id": self.product_map.get(symbol),
                    "open_time":  datetime.now(timezone.utc).isoformat(),
                    "strategy":   strategy_name,
                }

        # Print signal details
        print()
        print(f"  [SIGNAL] {symbol}  SHORT  [{self.timeframe}] — {strategy_name}")
        print(f"           Entry     : {entry:.4f}  (close of signal candle)")
        print(f"           Stop Loss : {sl:.4f}")
        print(f"           Risk      : ${risk_usd:,.2f}  ({self.config['risk_pct']}%)")
        print(f"           Mode      : {signal['mode']}")
        if strategy_name == "STRATEGY_2":
            body  = candle_body(signal_candle)
            wick  = upper_wick(signal_candle)
            ratio = wick / body if body > 0 else 0
            print(f"           Body / Wick: {body:.4f} / {wick:.4f} ({ratio:.2f}x)")
        elif strategy_name == "STRATEGY_3":
            print(f"           O/C: {signal_candle['open']:.4f} → {signal_candle['close']:.4f}")
        elif strategy_name == "THREE_BLACK_CROWS":
            print(f"           SL = highest high of the 3 candles")
        print()

        self._log("info", "SIGNAL",
                  f"{symbol} SHORT | {strategy_name} | tf={self.timeframe} | "
                  f"entry={entry:.4f} | sl={sl:.4f} | risk=${risk_usd:.2f}")

        if not self.paper:
            self._execute_trade(symbol, signal)

        if self.on_signal_callback:
            try:
                self.on_signal_callback(signal)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Live trade execution — ALL BUGS FIXED HERE                          #
    # ------------------------------------------------------------------ #

    def _execute_trade(self, symbol: str, signal: dict) -> None:
        try:
            # --- 1. Duplicate guard ---
            with self._trade_lock:
                if symbol in self.active_trades:
                    self._log("info", "TRADE", f"Duplicate entry blocked for {symbol}")
                    return
                self.active_trades[symbol] = {"_reserved": True}

            # --- 2. Live balance ---
            account_balance = self.rest.get_usd_balance() or 0.0
            capital         = self.trading_capital

            if capital <= 0:
                self._log("error", "TRADE", "trading_capital=0 — skip")
                self._cleanup_trade(symbol)
                return

            if account_balance < capital:
                self._log("warning", "TRADE",
                          f"Balance ${account_balance:.2f} < capital ${capital:.2f} "
                          f"— capping to balance")
                capital = account_balance

            if capital <= 0:
                self._log("warning", "TRADE", "Effective capital=0 — skip")
                self._cleanup_trade(symbol)
                return

            # --- 3. Risk-based sizing ---
            entry = signal["entry"]
            sl    = signal["stop_loss"]
            pid   = self.product_map.get(symbol)

            if not pid:
                self._log("error", "TRADE",
                          f"product_id not found for {symbol} — skip")
                self._cleanup_trade(symbol)
                return

            position_size, size_diag = compute_position_size(
                entry_price     = entry,
                stop_loss_price = sl,
                account_balance = capital,
                risk_pct        = self.risk_pct,
                leverage        = self.leverage,
            )

            if position_size < 1:
                self._log("error", "TRADE", f"Computed size < 1 for {symbol} — skip")
                self._cleanup_trade(symbol)
                return

            print()
            print(f"  [TRADE] {symbol}  SHORT  @ {entry:.4f}")
            print(f"          product_id       : {pid}")
            print(f"          leverage         : {self.leverage}x")
            print(f"          stop_loss        : {sl:.4f}  (distance={size_diag['stop_distance']:.4f})")
            print(f"          risk_amount      : ${size_diag['risk_amount']:,.2f}  ({size_diag['risk_pct']}%)")
            print(f"          risk_size_raw    : {size_diag['risk_size_raw']:.4f} contracts")
            print(f"          max_by_margin    : {size_diag['max_by_margin']:.4f} contracts")
            print(f"          final_size       : {position_size} contracts")
            print(f"          margin_used      : ${size_diag['margin_used']:,.2f}")
            print(f"          max_loss_if_SL   : ${size_diag['max_loss_est']:,.2f}")
            print(f"          account_balance  : ${account_balance:,.2f}")
            print()

            # --- 4. Place entry (market) order ---
            entry_result = self.rest.place_order(
                product_id=pid, side="sell",
                size=position_size, order_type="market_order",
            )

            if not entry_result or "error" in entry_result:
                self._log("error", "TRADE",
                          f"Entry order FAILED for {symbol}: {entry_result}")
                print(f"  [FAILED] {symbol} entry order: {entry_result}")
                self._cleanup_trade(symbol)
                return

            order_result = entry_result.get("result", {})
            order_id     = order_result.get("id")
            order_state  = order_result.get("state", "")

            if order_state == "rejected":
                reject_reason = order_result.get("cancel_reason", "unknown")
                self._log("error", "TRADE",
                          f"Entry order REJECTED by exchange: {reject_reason}")
                self._cleanup_trade(symbol)
                return

            if not order_id:
                self._log("error", "TRADE",
                          f"No order_id returned for {symbol} — cannot confirm fill")
                self._cleanup_trade(symbol)
                return

            _log("info", "TRADE",
                 f"Entry order submitted | {symbol} | id={order_id} | "
                 f"requested_size={position_size} | state={order_state}")

            # --- 5. FIX BUG #1 & #2: Poll for confirmed fill + get actual filled size ---
            filled, actual_filled_size = self.rest.wait_for_fill(order_id, symbol)

            if not filled or actual_filled_size < 1:
                self._log("error", "TRADE",
                          f"Entry not confirmed filled for {symbol}. "
                          f"filled={filled} size={actual_filled_size}. "
                          f"Attempting to cancel entry order and aborting SL.")
                # Try to cancel if still open (may already be filled but poll failed)
                self.rest.cancel_order(order_id, pid)
                self._cleanup_trade(symbol)
                return

            print(f"  [FILLED] {symbol} SHORT | order_id={order_id} | "
                  f"filled={actual_filled_size} contracts")

            if actual_filled_size != position_size:
                _log("warning", "TRADE",
                     f"Partial fill: requested={position_size} "
                     f"actual={actual_filled_size}. SL will use actual size.")

            # --- 6. FIX: Place bracket SL using ACTUAL filled size ---
            #         Bracket order auto-manages size and auto-cancels on close
            bracket_result = self.rest.place_bracket_stop_loss(
                product_id=pid,
                stop_price=sl,
                symbol=symbol,
            )

            bracket_ok = bracket_result and "error" not in bracket_result

            if bracket_ok:
                self._log("info", "BRACKET-SL",
                          f"Bracket SL placed | {symbol} | sl={sl:.4f} | "
                          f"trigger=last_traded_price")
                print(f"  [SL-OK] Bracket stop-loss placed @ {sl:.4f}  (auto-cancels on close)")
            else:
                self._log("warning", "BRACKET-SL",
                          f"Bracket SL FAILED for {symbol}: {bracket_result}  "
                          f"— local SL monitoring active as fallback")
                print(f"  [SL-WARN] Bracket SL failed — local monitoring active: {bracket_result}")
            print()

            # --- 7. Record trade with ACTUAL filled size ---
            signal["executed"]        = True
            signal["size"]            = actual_filled_size
            signal["capital_used"]    = capital
            signal["risk_amount"]     = size_diag["risk_amount"]
            signal["margin_used"]     = size_diag["margin_used"]
            signal["max_loss_est"]    = size_diag["max_loss_est"]
            signal["account_balance"] = account_balance
            signal["order_result"]    = entry_result
            signal["order_id"]        = order_id
            signal["bracket_result"]  = bracket_result

            with self._trade_lock:
                self.active_trades[symbol] = {
                    "entry":          entry,
                    "stop_loss":      sl,
                    "size":           actual_filled_size,   # FIX: actual, not requested
                    "product_id":     pid,
                    "order_id":       order_id,
                    "open_time":      datetime.now(timezone.utc).isoformat(),
                    "capital":        capital,
                    "risk_usd":       size_diag["risk_amount"],
                    "margin_used":    size_diag["margin_used"],
                    "strategy":       signal.get("strategy", "UNKNOWN"),
                    "bracket_sl_ok":  bracket_ok,
                }

            if self.on_trade_callback:
                try:
                    self.on_trade_callback(signal)
                except Exception:
                    pass

        except Exception as exc:
            self._cleanup_trade(symbol)
            self._log("error", "TRADE", f"Execution error for {symbol}: {exc}")

    # ------------------------------------------------------------------ #
    #  UI helpers                                                          #
    # ------------------------------------------------------------------ #

    def _print_banner(self) -> None:
        print()
        print("+========================================================+")
        print("|   DELTA EXCHANGE INDIA — TRADING BOT  v10.0           |")
        print("|   Strategies (OR combination):                        |")
        print("|     1. Breakout + Open=PrevClose & Close=PrevOpen     |")
        print("|     2. 3-candle breakout + upper wick >= 1.8x body    |")
        print("|     3. Bearish Engulfing + Uptrend                    |")
        print("|     4. Three Black Crows Pattern                      |")
        print("|   SL FIX #1  : Fill confirmed before SL placed        |")
        print("|   SL FIX #2  : Actual filled size used for SL         |")
        print("|   SL FIX #3  : Bracket order /orders/bracket          |")
        print("|   SL FIX #4  : Tick-size rounding (not 2dp)           |")
        print("|   AUTH FIX   : Fresh timestamp on every sign() call   |")
        print("|   Endpoint   : wss://socket.india.delta.exchange       |")
        print("+========================================================+")
        print()

    def _print_startup_summary(self) -> None:
        mode_str = "PAPER (signals only)" if self.paper else "LIVE TRADING"
        risk_usd = self.trading_capital * self.risk_pct
        print("+--------------------------------------------------------+")
        print(f"  Mode            : {mode_str}")
        print(f"  Timeframe       : {self.timeframe}  "
              f"(api_resolution={self.api_resolution} | WS={self.ws_channel})")
        print(f"  Trading capital : ${self.trading_capital:,.2f} USD")
        print(f"  Risk / trade    : {self.config['risk_pct']}%  =  ~${risk_usd:,.2f} USD")
        print(f"  Leverage        : {self.leverage}x")
        print(f"  Max open trades : {self.max_trades}")
        print(f"  SL method       : Bracket order (POST /v2/orders/bracket)")
        print(f"  SL trigger      : last_traded_price")
        print(f"  SL size         : Actual filled contracts (polled)")
        print(f"  Strategies (OR) :")
        print(f"    1. Breakout + Open=PrevClose & Close=PrevOpen")
        print(f"    2. 3-candle breakout + upper wick >= 1.8x body")
        print(f"    3. Bearish Engulfing + Uptrend")
        print(f"    4. Three Black Crows Pattern")
        print(f"  Active symbols  : {len(self.symbols)}")
        for sym in self.symbols:
            pid      = self.product_map.get(sym, "???")
            tick_sz  = self.rest.get_tick_size(sym)
            print(f"    - {sym:<22} product_id={pid}  tick={tick_sz}")
            print(f"      candle_api: {to_candle_symbol(sym):<16} ws_sub: {to_ws_symbol(sym)}")
        print("+--------------------------------------------------------+")
        print()


# ================================================================
#  19. USER INPUT HELPERS
# ================================================================

def _divider(title: str = "") -> None:
    if title:
        pad = (56 - len(title) - 2) // 2
        print(f"\n  {'=' * pad} {title} {'=' * pad}")
    else:
        print(f"\n  {'=' * 58}")


def ask_timeframe() -> str:
    _divider("TIMEFRAME")
    print("  Options : 1m  |  5m  |  15m  |  1h")
    raw = input("  Enter timeframe (default = 1h) : ").strip().lower()
    if raw in TIMEFRAME_MAP:
        tf = TIMEFRAME_MAP[raw]
        print(f"  Selected : {raw} (api_resolution={tf['api_resolution']} | WS={tf['ws_channel']})")
        return raw
    print(f"  '{raw}' not valid — defaulting to 1h")
    return "1h"


def ask_mode() -> Tuple[bool, str, str]:
    _divider("MODE")
    print("  [1]  Paper Mode   — signals only, no real orders")
    print("  [2]  Live Trading — real orders on Delta Exchange India")
    raw = input("  Enter 1 or 2 (default = 1) : ").strip()
    if raw == "2":
        _divider("API CREDENTIALS")
        api_key    = input("  API Key    : ").strip()
        api_secret = input("  API Secret : ").strip()
        if not api_key or not api_secret:
            print("  [ERROR] Both required. Falling back to paper.")
            return True, "", ""
        print("  [LIVE] Mode confirmed")
        return False, api_key, api_secret
    print("  [PAPER] Mode confirmed")
    return True, "", ""


def ask_symbols(product_map: Dict[str, int]) -> List[str]:
    _divider("SYMBOLS")
    print("  Enter symbols (any format, space/comma separated).")
    print("  Press Enter to AUTO-SELECT top 5 by volatility.")
    raw = input("  Symbols : ").strip().upper()
    if not raw:
        return []
    return [p.strip() for p in raw.replace(",", " ").split() if p.strip()]


def ask_leverage() -> int:
    _divider("LEVERAGE")
    print("  Common values: 1 | 2 | 5 | 10 | 20 | 50 | 100")
    try:
        lev = int(input("  Leverage x (default = 5) : ").strip() or 5)
        lev = max(1, min(lev, 200))
    except ValueError:
        lev = 5
    print(f"  Leverage : {lev}x")
    return lev


def ask_trading_capital(account_balance: float) -> float:
    _divider("TRADING CAPITAL")
    print(f"  Account balance : ${account_balance:,.2f} USD")
    raw = input(
        f"  Trading capital (default = ${account_balance:,.0f}) : "
    ).strip()
    if not raw:
        return account_balance
    try:
        capital = float(raw)
    except ValueError:
        capital = account_balance
    if capital <= 0:
        print("  [WARN] Must be > 0. Using balance.")
        capital = account_balance
    if capital > account_balance:
        print(f"  [WARN] Capping to balance ${account_balance:,.2f}")
        capital = account_balance
    print(f"  Trading capital : ${capital:,.2f} USD")
    return capital


def ask_risk_params() -> Tuple[float, int]:
    _divider("RISK MANAGEMENT")
    try:
        risk = float(input("  Risk per trade % (default = 2) : ").strip() or 2)
        risk = max(0.01, min(risk, 100.0))
    except ValueError:
        risk = 2.0
    try:
        mx = int(input("  Max concurrent open trades (default = 2) : ").strip() or 2)
        mx = max(1, min(mx, 20))
    except ValueError:
        mx = 2
    print(f"  Risk/trade : {risk}%   Max open trades : {mx}")
    return risk, mx


# ================================================================
#  20. ENTRY POINT
# ================================================================

def main() -> None:
    print()
    print("  +======================================================+")
    print("  |   DELTA EXCHANGE INDIA  —  TRADING BOT  v10.0       |")
    print("  |   Strategies (OR combination):                      |")
    print("  |     1. Breakout + Open=PrevClose & Close=PrevOpen   |")
    print("  |     2. 3-candle breakout + upper wick >= 1.8x body  |")
    print("  |     3. Bearish Engulfing + Uptrend                  |")
    print("  |     4. Three Black Crows Pattern                    |")
    print("  |   SL FIX #1  : Fill confirmed before SL placed      |")
    print("  |   SL FIX #2  : Actual filled size used for SL       |")
    print("  |   SL FIX #3  : Bracket order /orders/bracket        |")
    print("  |   SL FIX #4  : Tick-size rounding per product       |")
    print("  |   AUTH FIX   : Fresh timestamp on every sign call   |")
    print("  |   Endpoint   : wss://socket.india.delta.exchange    |")
    print("  +======================================================+")

    timeframe              = ask_timeframe()
    paper, api_key, api_secret = ask_mode()

    _divider("CONNECTING TO DELTA EXCHANGE INDIA")
    rest_tmp = DeltaREST(api_key, api_secret)

    print("  Loading product catalogue...")
    product_map = rest_tmp.fetch_product_map()
    if not product_map:
        print("  [ERROR] Could not load product catalogue. Check network.")
        return
    print(f"  {len(product_map)} products loaded.")

    raw_symbols = ask_symbols(product_map)
    leverage    = ask_leverage()

    account_balance = 0.0
    if not paper:
        print("\n  Fetching account balance...")
        account_balance = rest_tmp.get_usd_balance() or 0.0
        if account_balance <= 0:
            print("  [WARN] Balance = 0. Check API key permissions.")
    else:
        _divider("PAPER MODE CAPITAL")
        try:
            account_balance = float(
                input("  Notional capital USD (default = 1000) : ").strip() or 1000
            )
        except ValueError:
            account_balance = 1000.0
        print(f"  Notional capital : ${account_balance:,.2f}")

    trading_capital      = ask_trading_capital(account_balance)
    risk_pct, max_trades = ask_risk_params()

    _divider("CONFIRM")
    risk_usd = trading_capital * risk_pct / 100
    print(f"  Mode            : {'PAPER' if paper else 'LIVE TRADING'}")
    print(f"  Timeframe       : {timeframe}")
    print(f"  Symbols         : {raw_symbols if raw_symbols else 'AUTO-SELECT'}")
    print(f"  Leverage        : {leverage}x")
    print(f"  Trading capital : ${trading_capital:,.2f}")
    print(f"  Risk / trade    : {risk_pct}%  =  ~${risk_usd:,.2f}")
    print(f"  Max open trades : {max_trades}")
    print(f"  SL method       : Bracket order (POST /v2/orders/bracket)")
    print(f"  SL trigger      : last_traded_price")
    print(f"  Strategies (OR) :")
    print(f"    1. Breakout + Open=PrevClose & Close=PrevOpen")
    print(f"    2. 3-candle breakout + upper wick >= 1.8x body")
    print(f"    3. Bearish Engulfing + Uptrend")
    print(f"    4. Three Black Crows Pattern")
    print()
    confirm = input("  Type YES to start the bot : ").strip().upper()
    if confirm != "YES":
        print("  Cancelled.")
        return

    cfg = {
        "paper_mode":            paper,
        "symbols":               raw_symbols,
        "risk_pct":              risk_pct,
        "leverage":              leverage,
        "trading_capital":       trading_capital,
        "max_concurrent_trades": max_trades,
        "timeframe":             timeframe,
        "api_key":               api_key,
        "api_secret":            api_secret,
    }

    bot = TradingBot(cfg)
    bot.start()

    if not bot.running or not bot.symbols:
        print("  [ERROR] Bot failed to start. See log above.")
        return

    print()
    print("  Bot is running. Press Ctrl+C to stop.")
    print()

    try:
        while True:
            time.sleep(60)
            open_n    = len(bot.active_trades)
            open_syms = list(bot.active_trades.keys())
            ws_state  = bot.ws_manager.get_state() if bot.ws_manager else "N/A"
            print(
                f"  [STATUS] "
                f"Open={open_n}/{max_trades}  "
                f"Signals={len(bot.signals)}  "
                f"SL_hits={len(bot.sl_events)}  "
                f"Capital=${trading_capital:,.0f}  "
                f"WS={ws_state}  "
                + (f"Trades={open_syms}" if open_syms else "NoOpenTrades")
            )
    except KeyboardInterrupt:
        bot.stop()
        print()
        print("  Bot stopped. Goodbye.")
        print()


if __name__ == "__main__":
    main()
