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
    pass  # already wrapped (pytest / IDE / Linux)

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
from dataclasses import dataclass
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
#  4.  WEBSOCKET MANAGER - PRODUCTION GRADE
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

        # Reconnect tracking — reset ONLY after confirmed _on_open
        self._reconnect_attempts = 0
        self._reconnect_delay    = self.config.reconnect_base_delay
        self._should_stop        = threading.Event()

        # Callbacks
        self._on_candle_callback:       Optional[callable] = None
        self._on_connected_callback:    Optional[callable] = None
        self._on_disconnected_callback: Optional[callable] = None
        self._on_error_callback:        Optional[callable] = None
        # NEW: called after every successful reconnect so bot can backfill
        self._on_reconnect_callback:    Optional[callable] = None

    # ---- public setters ----

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

    # ---- internals ----

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
                    # After the first disconnect, fire reconnect callback so
                    # the bot can backfill any missed candles.
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
        """Establish connection. Do NOT reset reconnect counters here."""
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
        """Reset counters only here — after confirmed handshake."""
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
        """
        Delta India flat candlestick payload:
        { "type": "candlestick_1m", "symbol": "BTCUSD", "open": ...,
          "high": ..., "low": ..., "close": ..., "volume": ...,
          "candle_start_time": <nanoseconds> }
        """
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
    """Bare symbol for WS subscriptions (no _PERP)."""
    s = symbol.upper().strip()
    if s.endswith("_PERP"):
        s = s[:-5]
    if s.endswith("USDT"):
        s = s[:-4] + "USD"
    return s


def to_candle_symbol(symbol: str) -> str:
    """REST candle endpoint requires no _PERP suffix."""
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
REST_BASE_GLOBAL = "https://api.delta.exchange"   # reference only

CANDLE_LIMIT        = 20
MAX_RETRIES         = 3
RETRY_DELAYS        = [5, 10, 15]
TIMEOUT             = 30
CANDLE_SAFETY_SHIFT = 1

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
    """Centralised REST request handler with retry logic."""

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.session    = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent":   "DeltaBot/9.0",
            "Accept":       "application/json",
            "Connection":   "keep-alive",
        })
        self.session.mount("https://", requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=40, max_retries=0, pool_block=False
        ))

    def _get_base_url(self, endpoint_type: str) -> str:
        # Both public and private always use India endpoint
        return REST_BASE_INDIA

    def _sign_request(
        self, method: str, path: str,
        params: dict = None, body: dict = None
    ) -> dict:
        """
        Delta docs signature format:
          METHOD + TIMESTAMP + PATH + ?QUERY_STRING + BODY_STRING
        The '?' prefix is required when query params are present.
        """
        if not self.api_key or not self.api_secret:
            raise ValueError("API key/secret missing")
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


def check_short_signal(candles: List[dict]) -> bool:
    """
    4-candle breakout + wick-rejection short entry.

    Index  Role
    ----   ----
    [-1]   currently forming — IGNORED
    [-2]   i   last CLOSED candle (signal candle)
    [-3]   i1  previous closed
    [-4]   i2  previous closed
    [-5]   i3  base candle

    Conditions:
      1. i2.close > i3.high   (1st breakout)
      2. i1.close > i2.high   (2nd breakout)
      3. i .close > i1.high   (3rd breakout)
      4. upper_wick(i) > body(i)  (rejection wick)
    """
    if len(candles) < 5:
        return False
    i  = candles[-2]
    i1 = candles[-3]
    i2 = candles[-4]
    i3 = candles[-5]
    breakout  = (
        i["close"]  > i1["high"] and
        i1["close"] > i2["high"] and
        i2["close"] > i3["high"]
    )
    rejection = upper_wick(i) > candle_body(i)
    return breakout and rejection


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
        secs = shift_candles * timeframe_minutes * 60
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
#  15. DELTA REST CLIENT
# ================================================================

class DeltaREST:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.request_handler = APIRequestHandler(api_key, api_secret)

    def verify_account(self) -> Optional[dict]:
        result = self.request_handler.request("GET", "/v2/profile", endpoint_type="private")
        if result and "result" in result:
            profile = result["result"]
            _log("info", "AUTH",
                 f"Authenticated: {profile.get('email','?')} | {profile.get('name','?')}")
            return profile
        return None

    def get_usd_balance(self) -> float:
        """
        Fetch available trading balance.

        NOTE (Problem 2 kept unfixed per user instruction):
        Checks USDT and USD asset symbols only.
        Delta India perps settle in INR — if live balance returns 0.0,
        change the check below to "INR" to match your account.
        """
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

    def place_order(
        self,
        product_id:  int,
        side:        str,
        size:        int,
        order_type:  str           = "market_order",
        limit_price: Optional[float] = None,
    ) -> dict:
        """
        Place a market or limit order.
        Returns the full API response dict (never None — caller checks 'error' key).
        """
        if side not in ("buy", "sell"):
            _log("error", "ORDER", f"Invalid side '{side}'. Must be 'buy' or 'sell'.")
            return {"error": "invalid_side"}
        if size < 1:
            _log("error", "ORDER", f"Invalid size {size}. Must be >= 1.")
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

    def place_stop_market_order(
        self,
        product_id:   int,
        side:         str,
        size:         int,
        stop_price:   float,
    ) -> dict:
        """
        Place an exchange-side stop-market order that survives bot crashes,
        internet drops, and PC sleep.

        BUG FIX #2a — Add reduce_only: True
          Delta India deprecated bracket orders (May 2024). The modern
          replacement is a reduce-only stop-market order. WITHOUT reduce_only,
          if the short is already closed when this SL fires, it opens a brand
          new LONG position — the exact opposite of what you want. With
          reduce_only=True the exchange cancels the order instead of flipping.

        BUG FIX #2b — Add stop_trigger_method: "last_traded_price"
          Delta defaults to mark price as the trigger index. Mark price lags
          last traded price (LTP) by design to prevent manipulation. On a fast
          BTC move the gap can be $100-$300. Using LTP ensures the SL triggers
          at the actual market price, not the smoothed mark price.

        Correct modern body for a short position SL (closes via BUY):
          product_id        : <id>
          size              : <contracts>
          side              : "buy"
          order_type        : "market_order"
          stop_order_type   : "stop_loss_order"
          stop_price        : "<price>"  (string)
          stop_trigger_method : "last_traded_price"   ← BUG FIX #2b
          reduce_only       : true                    ← BUG FIX #2a
        """
        if stop_price <= 0:
            _log("error", "SL-ORDER", f"Invalid stop_price {stop_price}")
            return {"error": "invalid_stop_price"}
        if side not in ("buy", "sell"):
            _log("error", "SL-ORDER", f"Invalid side '{side}'")
            return {"error": "invalid_side"}

        body: Dict = {
            "product_id":          product_id,
            "size":                size,
            "side":                side,
            "order_type":          "market_order",
            "stop_order_type":     "stop_loss_order",
            "stop_price":          str(round(stop_price, 2)),
            "stop_trigger_method": "last_traded_price",  # BUG FIX #2b
            "reduce_only":         True,                  # BUG FIX #2a
        }
        _log("info", "SL-ORDER",
             f"Placing exchange SL: side={side} size={size} "
             f"stop={stop_price:.4f} pid={product_id} "
             f"reduce_only=True trigger=last_traded_price")
        result = self.request_handler.request(
            "POST", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def fetch_product_map(self) -> Dict[str, int]:
        result = self.request_handler.request("GET", "/v2/products", endpoint_type="public")
        pmap: Dict[str, int] = {}
        if result and "result" in result:
            for item in result.get("result", []):
                sym = item.get("symbol", "")
                pid = item.get("id")
                if sym and pid is not None:
                    pmap[sym] = int(pid)
        _log("info", "CATALOG", f"Loaded {len(pmap)} products")
        return pmap

    def get_top_symbols(
        self,
        product_map: Dict[str, int],
        mode: str = "volatile",
        limit: int = 5,
        perp_only: bool = True,
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
        symbol: str,
        resolution: str = "1h",
        limit: int = CANDLE_LIMIT,
        shift_candles: int = 0,
    ) -> List[dict]:
        candle_symbol = to_candle_symbol(symbol)
        tf_entry = next(
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
        """
        BUG FIX #1 — correct Delta India leverage endpoint:
          WRONG (old): POST /v2/products/{product_id}/leverage
          RIGHT (new): POST /v2/products/{product_id}/orders/leverage
        The old endpoint returns 404 silently, leaving leverage at account
        default (1x), which makes the margin sizing formula wrong.
        """
        body   = {"leverage": str(leverage)}
        result = self.request_handler.request(
            "POST", f"/v2/products/{product_id}/orders/leverage",  # FIXED
            endpoint_type="private", body=body,
        )
        if result and "result" in result:
            _log("info", "LEVERAGE", f"product_id={product_id} {leverage}x OK")
            return True
        _log("error", "LEVERAGE",
             f"FAILED for product_id={product_id} — response: {result}")
        return False

    def close_position(self, product_id: int, size: int, side: str = "buy") -> dict:
        """Close a position with a market order. side='buy' to close a short."""
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
#  16. POSITION SIZER  (risk-based, leverage-capped)
# ================================================================

def compute_position_size(
    entry_price:     float,
    stop_loss_price: float,
    account_balance: float,
    risk_pct:        float,   # 0.0-1.0 fraction (e.g. 0.02 for 2 %)
    leverage:        int,
) -> Tuple[int, dict]:
    """
    Correct risk-based position sizing that considers stop-loss distance.

    Step 1  risk_amount   = account_balance * risk_pct
    Step 2  stop_distance = abs(entry_price - stop_loss_price)
    Step 3  risk_size     = risk_amount / stop_distance
    Step 4  max_by_margin = (account_balance * leverage) / entry_price
    Step 5  final_size    = min(risk_size, max_by_margin)  rounded down to int >= 1

    Returns (final_size, diagnostics_dict).
    """
    risk_amount   = account_balance * risk_pct
    stop_distance = abs(entry_price - stop_loss_price)

    if stop_distance <= 0:
        _log("warning", "SIZER", "stop_distance=0 — cannot size position")
        return 0, {}

    risk_size      = risk_amount / stop_distance
    max_by_margin  = (account_balance * leverage) / entry_price
    final_size_raw = min(risk_size, max_by_margin)
    final_size     = max(1, int(final_size_raw))

    margin_used    = (final_size * entry_price) / leverage
    max_loss_est   = final_size * stop_distance

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
#  17. TRADING BOT
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
        self.risk_pct        = config["risk_pct"] / 100.0   # stored as fraction

        self._tf            = TimeframeSafe(config.get("timeframe", "1h"))
        self.timeframe      = self._tf.key
        self.resolution     = self._tf.resolution
        self.api_resolution = self._tf.api_resolution
        self.ws_channel     = self._tf.ws_channel

        self.rest = DeltaREST(self.api_key, self.api_secret)

        self.symbols:     List[str]          = []
        self.product_map: Dict[str, int]     = {}
        self.candle_store: Dict[str, deque]  = {}

        self._trade_lock   = threading.Lock()
        self.active_trades: Dict[str, dict] = {}

        self.signals:   List[dict] = []
        self.sl_events: List[dict] = []

        self.running    = False
        self.ws_manager: Optional[DeltaWebSocket] = None

        # Optional UI callbacks
        self.on_signal_callback = None
        self.on_trade_callback  = None
        self.on_log_callback    = None

    # ------------------------------------------------------------------ #
    #  Internal log helper                                                 #
    # ------------------------------------------------------------------ #

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
        # Backfill missed candles after every reconnect
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
        """
        Called by WS manager with trading-format symbol (e.g. BTCUSD_PERP).
        """
        self._log("debug", "WS-CANDLE",
                  f"{symbol} | t={candle.get('time')} "
                  f"O={candle.get('open')} H={candle.get('high')} "
                  f"L={candle.get('low')} C={candle.get('close')}")

        if symbol in self.candle_store:
            self.process_candle(symbol, candle)
            return

        # Fuzzy match for edge cases
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
        """
        Called every time the WebSocket reconnects after a drop.
        Backfills any candles that arrived while we were offline,
        then re-evaluates signals on each symbol to avoid gaps.
        """
        self._log("info", "BACKFILL",
                  "WebSocket reconnected — backfilling missed candles...")
        for sym in self.symbols:
            self._backfill_symbol(sym)
        self._log("info", "BACKFILL", "Backfill complete.")

    def _backfill_symbol(self, symbol: str) -> None:
        """
        Fetch the latest REST candles and merge any new ones into the
        in-memory store without creating duplicates.
        """
        store = self.candle_store.get(symbol)
        if store is None:
            return

        fresh = self.rest.get_candles_with_retry(symbol, self.api_resolution, CANDLE_LIMIT)
        if not fresh:
            self._log("warning", "BACKFILL", f"No fresh candles for {symbol}")
            return

        # Build set of timestamps already in store
        existing_ts = {c["time"] for c in store}
        added       = 0
        for c in sorted(fresh, key=lambda x: x["time"]):
            if c["time"] not in existing_ts:
                store.append(c)
                existing_ts.add(c["time"])
                added += 1

        if added:
            self._log("info", "BACKFILL",
                      f"{symbol}: merged {added} new candles from REST")
        else:
            self._log("info", "BACKFILL", f"{symbol}: no new candles needed")

    # ------------------------------------------------------------------ #
    #  Candle processing — FIX: correctly indented inside TradingBot      #
    # ------------------------------------------------------------------ #

    def process_candle(self, symbol: str, candle: dict) -> None:
        """
        Main candle dispatch:
          - If timestamp == last stored  → update forming candle, check local SL
          - If timestamp > last stored   → close previous candle, run strategy,
                                           then append new forming candle
        """
        store = self.candle_store.get(symbol)
        if store is None:
            return

        if not validate_candle(candle, symbol):
            return

        # --- Forming candle update ---
        if store and store[-1]["time"] == candle["time"]:
            store[-1] = candle
            if symbol in self.active_trades:
                self._check_stop_loss(symbol, candle)
            return

        # --- New candle: previous candle just closed ---
        if store:
            closed = store[-1]
            self._log("info", "CANDLE-CLOSED",
                      f"{symbol} | {self.timeframe} | CLOSED | "
                      f"O={closed['open']:.2f} H={closed['high']:.2f} "
                      f"L={closed['low']:.2f} C={closed['close']:.2f} "
                      f"V={closed.get('volume', 0):.2f}")

            if symbol not in self.active_trades:
                if check_short_signal(list(store)):
                    self._on_signal(symbol, closed)

        # Append new forming candle
        store.append(candle)

    # ------------------------------------------------------------------ #
    #  Stop-loss monitoring (local)                                        #
    # ------------------------------------------------------------------ #

    def _check_stop_loss(self, symbol: str, candle: dict) -> None:
        """
        Local stop-loss monitor. Acts as fallback only — the primary
        stop-loss is the exchange-side stop-market order placed at entry.
        """
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade:
            return
        # For a short, SL triggers when HIGH crosses above stop_loss price
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
        print("      (Exchange SL order should have already fired)")
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

        # Attempt to close via REST as belt-and-braces
        if not self.paper:
            pid  = trade.get("product_id")
            size = trade.get("size", 1)
            if pid:
                self.rest.close_position(pid, size, side="buy")

        with self._trade_lock:
            self.active_trades.pop(symbol, None)
        self._log("info", "SL", f"Trade record removed for {symbol}")

    # ------------------------------------------------------------------ #
    #  Signal handler                                                      #
    # ------------------------------------------------------------------ #

    def _on_signal(self, symbol: str, signal_candle: dict) -> None:
        entry = signal_candle["close"]
        sl    = signal_candle["high"]
        rpu   = abs(sl - entry)

        if rpu == 0:
            self._log("warning", "SIGNAL",
                      f"risk_per_unit=0 for {symbol} — skip")
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

            wick     = round(upper_wick(signal_candle), 6)
            body     = round(candle_body(signal_candle), 6)
            risk_usd = round(self.trading_capital * self.risk_pct, 2)

            signal = {
                "time":            datetime.now(timezone.utc).isoformat(),
                "symbol":          symbol,
                "direction":       "SHORT",
                "entry":           entry,
                "stop_loss":       sl,
                "upper_wick":      wick,
                "body":            body,
                "timeframe":       self.timeframe,
                "mode":            "PAPER" if self.paper else "LIVE",
                "executed":        False,
                "product_id":      self.product_map.get(symbol),
                "risk_usd":        risk_usd,
                "trading_capital": self.trading_capital,
            }
            self.signals.append(signal)

            if self.paper:
                self.active_trades[symbol] = {
                    "entry":      entry,
                    "stop_loss":  sl,
                    "size":       0,
                    "product_id": self.product_map.get(symbol),
                    "open_time":  datetime.now(timezone.utc).isoformat(),
                }

        print()
        print(f"  [SIGNAL] {symbol}  SHORT  [{self.timeframe}]")
        print(f"           Entry     : {entry:.4f}")
        print(f"           Stop Loss : {sl:.4f}")
        print(f"           Wick/Body : {wick:.6f}/{body:.6f}"
              + (f"  ({wick/body:.2f}x)" if body > 0 else ""))
        print(f"           Risk      : ${risk_usd:,.2f}  ({self.config['risk_pct']}%)")
        print(f"           Mode      : {signal['mode']}")
        print()

        self._log("info", "SIGNAL",
                  f"{symbol} SHORT | tf={self.timeframe} | "
                  f"entry={entry:.4f} | sl={sl:.4f} | risk=${risk_usd:.2f}")

        if not self.paper:
            self._execute_trade(symbol, signal)

        if self.on_signal_callback:
            try:
                self.on_signal_callback(signal)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Live trade execution  (patched: correct sizing + exchange SL)      #
    # ------------------------------------------------------------------ #

    def _execute_trade(self, symbol: str, signal: dict) -> None:
        """
        Full live trade execution:
          1. Guard against duplicates
          2. Fetch live balance
          3. Risk-based contract sizing (stop-distance formula)
          4. Place entry market order
          5. Confirm fill (check result structure)
          6. Place exchange-side stop-loss order
          7. Record trade
        """
        try:
            # --- 1. Duplicate guard ---
            with self._trade_lock:
                if symbol in self.active_trades:
                    self._log("info", "TRADE",
                              f"Duplicate entry blocked for {symbol}")
                    return
                self.active_trades[symbol] = {"_reserved": True}

            # --- 2. Live balance ---
            account_balance = self.rest.get_usd_balance() or 0.0
            capital         = self.trading_capital

            if capital <= 0:
                self._log("error", "TRADE", "trading_capital=0 — skip")
                with self._trade_lock:
                    self.active_trades.pop(symbol, None)
                return

            if account_balance < capital:
                self._log("warning", "TRADE",
                          f"Balance ${account_balance:.2f} < capital ${capital:.2f} "
                          f"— capping to balance")
                capital = account_balance

            if capital <= 0:
                self._log("warning", "TRADE", "Effective capital=0 — skip")
                with self._trade_lock:
                    self.active_trades.pop(symbol, None)
                return

            # --- 3. Risk-based sizing ---
            entry = signal["entry"]
            sl    = signal["stop_loss"]
            pid   = self.product_map.get(symbol)

            if not pid:
                self._log("error", "TRADE",
                          f"product_id not found for {symbol} — skip")
                with self._trade_lock:
                    self.active_trades.pop(symbol, None)
                return

            position_size, size_diag = compute_position_size(
                entry_price     = entry,
                stop_loss_price = sl,
                account_balance = capital,
                risk_pct        = self.risk_pct,
                leverage        = self.leverage,
            )

            if position_size < 1:
                self._log("error", "TRADE",
                          f"Computed size < 1 for {symbol} — skip")
                with self._trade_lock:
                    self.active_trades.pop(symbol, None)
                return

            # --- Print sizing summary ---
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

            # --- 4. Place entry order ---
            entry_result = self.rest.place_order(
                product_id=pid, side="sell",
                size=position_size, order_type="market_order",
            )

            # --- 5. Confirm fill ---
            order_ok   = False
            order_state = entry_result.get("result", {}).get("state", "") if entry_result else ""
            order_id    = entry_result.get("result", {}).get("id")        if entry_result else None

            if entry_result and "error" not in entry_result:
                if order_state in ("open", "filled", ""):
                    order_ok = True
                elif order_state == "rejected":
                    reject_reason = entry_result.get("result", {}).get("cancel_reason", "unknown")
                    self._log("error", "TRADE",
                              f"Order REJECTED by exchange: {reject_reason}")
                    order_ok = False
                else:
                    # Treat unknown state as ok (partial / queued)
                    order_ok = True

            if not order_ok:
                self._log("error", "TRADE", f"Entry order FAILED for {symbol}: {entry_result}")
                print(f"  [FAILED] {symbol} entry order: {entry_result}")
                with self._trade_lock:
                    self.active_trades.pop(symbol, None)
                return

            self._log("info", "TRADE",
                      f"ORDER PLACED | {symbol} | pid={pid} | id={order_id} | "
                      f"size={position_size} | entry={entry:.4f} | state={order_state}")
            print(f"  [OK] Entry order placed: {symbol} SHORT x{position_size} @ {entry:.4f}")
            if order_id:
                print(f"       Order ID: {order_id}")
            print()

            # --- 6. Exchange-side stop-loss ---
            # For a short, closing order is a BUY at stop_price above entry
            sl_result   = self.rest.place_stop_market_order(
                product_id=pid,
                side="buy",
                size=position_size,
                stop_price=sl,
            )
            sl_order_id = sl_result.get("result", {}).get("id") if sl_result else None
            sl_ok       = sl_result and "error" not in sl_result

            if sl_ok:
                self._log("info", "SL-ORDER",
                          f"Exchange SL placed | {symbol} | id={sl_order_id} | sl={sl:.4f}")
                print(f"  [SL-OK] Exchange stop-loss placed @ {sl:.4f}  (id={sl_order_id})")
            else:
                self._log("warning", "SL-ORDER",
                          f"Exchange SL FAILED for {symbol}: {sl_result}  "
                          f"(local SL monitoring still active)")
                print(f"  [SL-WARN] Exchange SL failed — local SL monitoring active: {sl_result}")
            print()

            # --- 7. Record trade ---
            signal["executed"]        = True
            signal["size"]            = position_size
            signal["capital_used"]    = capital
            signal["risk_amount"]     = size_diag["risk_amount"]
            signal["margin_used"]     = size_diag["margin_used"]
            signal["max_loss_est"]    = size_diag["max_loss_est"]
            signal["account_balance"] = account_balance
            signal["order_result"]    = entry_result
            signal["order_id"]        = order_id
            signal["sl_order_id"]     = sl_order_id

            with self._trade_lock:
                self.active_trades[symbol] = {
                    "entry":        entry,
                    "stop_loss":    sl,
                    "size":         position_size,
                    "product_id":   pid,
                    "order_id":     order_id,
                    "sl_order_id":  sl_order_id,
                    "open_time":    datetime.now(timezone.utc).isoformat(),
                    "capital":      capital,
                    "risk_usd":     size_diag["risk_amount"],
                    "margin_used":  size_diag["margin_used"],
                }

            if self.on_trade_callback:
                try:
                    self.on_trade_callback(signal)
                except Exception:
                    pass

        except Exception as exc:
            with self._trade_lock:
                self.active_trades.pop(symbol, None)
            self._log("error", "TRADE", f"Execution error for {symbol}: {exc}")

    # ------------------------------------------------------------------ #
    #  UI helpers                                                          #
    # ------------------------------------------------------------------ #

    def _print_banner(self) -> None:
        print()
        print("+========================================================+")
        print("|   DELTA EXCHANGE INDIA — TRADING BOT  v9.0            |")
        print("|   Strategy : SHORT Breakout + Wick Rejection           |")
        print("|   Sizing   : Risk-based (stop-distance formula)        |")
        print("|   SL       : Exchange-side + reduce_only + LTP trigger |")
        print("|   Fix #1   : Leverage  /orders/leverage endpoint       |")
        print("|   Fix #2   : SL reduce_only=True + stop_trigger=LTP    |")
        print("|   Fix #3   : No /v2/time — local time.time() only      |")
        print("|   Fix #4   : process_candle appends AFTER strategy     |")
        print("|   Note     : Balance uses USDT/USD (Problem 2 open)    |")
        print("|   Endpoint : wss://socket.india.delta.exchange         |")
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
        print(f"  Active symbols  : {len(self.symbols)}")
        for sym in self.symbols:
            pid = self.product_map.get(sym, "???")
            print(f"    - {sym:<22} product_id={pid}")
            print(f"      candle_api: {to_candle_symbol(sym):<16} ws_sub: {to_ws_symbol(sym)}")
        print("+--------------------------------------------------------+")
        print()


# ================================================================
#  18. USER INPUT HELPERS
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
#  19. ENTRY POINT
# ================================================================

def main() -> None:
    print()
    print("  +======================================================+")
    print("  |   DELTA EXCHANGE INDIA  —  TRADING BOT  v9.0        |")
    print("  |   Strategy : SHORT Breakout + Wick Rejection         |")
    print("  |   Sizing   : Risk-based (stop-distance formula)      |")
    print("  |   SL       : reduce_only=True + last_traded_price    |")
    print("  |   Fix #1   : Leverage  /orders/leverage endpoint     |")
    print("  |   Fix #2   : SL reduce_only=True + LTP trigger       |")
    print("  |   Fix #3   : No /v2/time — uses time.time() only     |")
    print("  |   Fix #4   : process_candle appends AFTER strategy   |")
    print("  |   Note     : Balance uses USDT/USD (Problem 2 open)  |")
    print("  |   Endpoint : wss://socket.india.delta.exchange       |")
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
    print(f"  Risk / trade    : {risk_pct}%  =  ~${risk_usd:,.2f}  (exact varies by SL dist)")
    print(f"  Max open trades : {max_trades}")
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
