import sys
import io

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
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
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
            "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
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
#  3b.  PRICE FORMATTING UTILITY
# ================================================================

def smart_fmt(price: float) -> str:
    """
    Dynamically selects decimal places based on price magnitude so that
    small-priced altcoins (e.g. 0.13498, 1.05928, 0.00142) are displayed
    with full precision while large prices (BTC at 65 000) stay clean.
    """
    if price == 0:
        return "0"
    abs_p = abs(price)
    if abs_p >= 10_000:  decimals = 2
    elif abs_p >= 1_000: decimals = 3
    elif abs_p >= 100:   decimals = 4
    elif abs_p >= 10:    decimals = 5
    elif abs_p >= 1:     decimals = 6
    elif abs_p >= 0.1:   decimals = 7
    elif abs_p >= 0.01:  decimals = 8
    else:                decimals = 10

    formatted = f"{price:.{decimals}f}"
    if "." in formatted:
        stripped = formatted.rstrip("0")
        if stripped.endswith("."):
            stripped += "00"
        elif len(stripped.split(".")[1]) < 2:
            stripped += "0" * (2 - len(stripped.split(".")[1]))
        return stripped
    return formatted


# ================================================================
#  4.  GMAIL NOTIFIER - SIMPLIFIED PROFESSIONAL STYLE
# ================================================================

class GmailNotifier:
    """
    Gmail notification handler for trading signals and events.
    Uses Gmail App Password (not regular password) for security.
    """

    def __init__(
        self,
        sender_email: str,
        gmail_app_password: str,
        recipient_emails: List[str],
        enabled: bool = True,
    ):
        self.sender_email       = sender_email
        self.gmail_app_password = gmail_app_password
        self.recipient_emails   = recipient_emails
        self.enabled            = enabled

    def _send_email(self, subject: str, body: str) -> bool:
        if not self.enabled:
            return False
        try:
            msg = MIMEMultipart()
            msg["From"]    = self.sender_email
            msg["To"]      = ", ".join(self.recipient_emails)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain", "utf-8"))
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                server.login(self.sender_email, self.gmail_app_password)
                server.send_message(msg)
            _log("info", "GMAIL", f"Email sent: {subject}")
            return True
        except Exception as e:
            _log("error", "GMAIL", f"Failed to send email: {e}")
            return False

    def send_signal(self, signal: dict) -> bool:
        if not self.enabled:
            return False
        direction       = signal.get("direction", "UNKNOWN")
        symbol          = signal.get("symbol", "UNKNOWN")
        strategy        = signal.get("strategy", "UNKNOWN")
        timeframe       = signal.get("timeframe", "UNKNOWN")
        entry           = signal.get("entry", 0)
        stop_loss       = signal.get("stop_loss", 0)
        take_profit     = signal.get("take_profit", 0)
        rsi             = signal.get("rsi", "N/A")
        mode            = signal.get("mode", "PAPER")
        risk_usd        = signal.get("risk_usd", 0)
        trading_capital = signal.get("trading_capital", 0)
        signal_time     = signal.get("time", datetime.now(timezone.utc).isoformat())
        no_rsi          = signal.get("no_rsi", False)
        
        direction_text = "SHORT" if direction == "SHORT" else "LONG"
        direction_emoji = "🔴" if direction == "SHORT" else "🟢"

        risk_dist   = abs(entry - stop_loss)
        reward_dist = abs(take_profit - entry)
        rr          = (reward_dist / risk_dist) if risk_dist > 0 else 0
        rsi_str     = "N/A (no RSI)" if no_rsi else (f"{rsi:.2f}" if isinstance(rsi, float) else str(rsi))
        risk_pct    = (risk_usd / trading_capital * 100) if trading_capital > 0 else 0

        subject = f"[{mode}] {direction_emoji} {symbol} {direction_text} - {strategy}"

        body = f"""TRADING SIGNAL ALERT
─────────────────────────────
Time       : {signal_time}
Mode       : {mode}
Symbol     : {symbol}
Direction  : {direction_text}
Strategy   : {strategy}
Timeframe  : {timeframe}
RSI(14)    : {rsi_str}

ENTRY & EXIT LEVELS
─────────────────────────────
Entry      : {smart_fmt(entry)}
Stop Loss  : {smart_fmt(stop_loss)}
Take Profit: {smart_fmt(take_profit)}
Risk Dist  : {smart_fmt(risk_dist)}
Reward Dist: {smart_fmt(reward_dist)}
Risk:Reward: {rr:.2f}:1

RISK MANAGEMENT
─────────────────────────────
Risk $     : ${risk_usd:.2f}
Capital    : ${trading_capital:,.2f}
Risk %     : {risk_pct:.2f}%

STRATEGY NOTES
─────────────────────────────
Stop Loss  : Triggers on CANDLE CLOSE
Take Profit: Triggers on PRICE TOUCH
SuperTrend : Monitored post-entry"""
        
        if no_rsi:
            body += "\nRSI Filter : DISABLED"
        if strategy in ("BEARISH_HARAMI", "BULLISH_HARAMI"):
            body += f"\nHarami Tol : {signal.get('harami_tolerance', 0.001)*100:.2f}%"
        
        return self._send_email(subject, body)

    def send_supertrend_strong(self, symbol: str, direction: str, entry: float,
                                new_tp: float, st1: float, st2: float,
                                timeframe: str, mode: str) -> bool:
        if not self.enabled:
            return False
        if direction == "SHORT":
            trend_emoji = "🔴"
            trend_desc  = "BEARISH - Both SuperTrends turned RED"
        else:
            trend_emoji = "🟢"
            trend_desc  = "BULLISH - Both SuperTrends turned GREEN"

        subject = f"[{mode}] {trend_emoji} {symbol} SuperTrend Confirmed - {direction}"

        body = f"""SUPERTREND STRONG TREND CONFIRMATION
─────────────────────────────
Symbol     : {symbol}
Direction  : {direction}
Trend      : {trend_desc}
Timeframe  : {timeframe}
Mode       : {mode}

TRADE DETAILS
─────────────────────────────
Entry Price: {smart_fmt(entry)}
Exit Mode  : SuperTrend-based (no fixed TP)

SUPERTREND VALUES
─────────────────────────────
ST(14,2)   : {smart_fmt(st1)}
ST(21,1)   : {smart_fmt(st2)}

EXIT STRATEGY
─────────────────────────────
Will exit when BOTH SuperTrends reverse direction:
- {'Flip to GREEN for exit' if direction == 'SHORT' else 'Flip to RED for exit'}"""
        
        return self._send_email(subject, body)

    def send_supertrend_exit(self, symbol: str, direction: str, entry: float,
                              exit_price: float, realized_pnl: float,
                              timeframe: str, mode: str) -> bool:
        if not self.enabled:
            return False
        is_profit   = realized_pnl > 0
        emoji       = "✅" if is_profit else "❌"
        pnl_text    = f"+${realized_pnl:.2f}" if is_profit else f"-${abs(realized_pnl):.2f}"
        result_text = "PROFIT" if is_profit else "LOSS"
        
        if direction == "SHORT":
            flip_desc = "Both SuperTrends flipped GREEN - bearish trend ended"
        else:
            flip_desc = "Both SuperTrends flipped RED - bullish trend ended"

        subject = f"[ST-EXIT] {emoji} {symbol} {direction} - {pnl_text}"

        body = f"""SUPERTREND EXIT - TREND REVERSAL
─────────────────────────────
Symbol      : {symbol}
Direction   : {direction}
Timeframe   : {timeframe}
Mode        : {mode}

EXIT DETAILS
─────────────────────────────
Exit Reason : {flip_desc}
Entry       : {smart_fmt(entry)}
Exit Price  : {smart_fmt(exit_price)}

RESULT
─────────────────────────────
Realized PnL: {pnl_text}
Result      : {result_text} {'🎉' if is_profit else '😢'}"""
        
        return self._send_email(subject, body)

    def send_trade_executed(self, trade: dict) -> bool:
        if not self.enabled:
            return False
        direction   = trade.get("direction", "UNKNOWN")
        symbol      = trade.get("symbol", "UNKNOWN")
        entry       = trade.get("entry", 0)
        stop_loss   = trade.get("stop_loss", 0)
        take_profit = trade.get("take_profit", 0)
        size        = trade.get("size", 0)
        strategy    = trade.get("strategy", "UNKNOWN")
        no_rsi      = trade.get("no_rsi", False)
        
        direction_emoji = "🔴" if direction == "SHORT" else "🟢"
        subject = f"[EXECUTED] {direction_emoji} {symbol} {direction} - {size} contracts"

        body = f"""TRADE EXECUTED SUCCESSFULLY
─────────────────────────────
Symbol     : {symbol}
Direction  : {direction}
Strategy   : {strategy}
Size       : {size} contracts

ENTRY & EXIT LEVELS
─────────────────────────────
Entry      : {smart_fmt(entry)}
Stop Loss  : {smart_fmt(stop_loss)}
Take Profit: {smart_fmt(take_profit)}

EXECUTION NOTES
─────────────────────────────
Stop Loss  : Triggers on CANDLE CLOSE
Take Profit: Triggers on PRICE TOUCH
SuperTrend : Monitoring post-entry"""
        
        if no_rsi:
            body += "\nRSI Filter : DISABLED"
        
        return self._send_email(subject, body)

    def send_trade_closed(self, trade: dict, close_reason: str, pnl_usd: float) -> bool:
        if not self.enabled:
            return False
        direction  = trade.get("direction", "UNKNOWN")
        symbol     = trade.get("symbol", "UNKNOWN")
        entry      = trade.get("entry", 0)
        is_profit  = pnl_usd > 0
        emoji      = "✅" if is_profit else "❌"
        pnl_text   = f"+${pnl_usd:.2f}" if is_profit else f"-${abs(pnl_usd):.2f}"
        subject    = f"[CLOSED] {emoji} {symbol} - {pnl_text} - {close_reason}"

        body = f"""TRADE CLOSED
─────────────────────────────
Symbol      : {symbol}
Direction   : {direction}
Close Reason: {close_reason}

TRADE DETAILS
─────────────────────────────
Entry       : {smart_fmt(entry)}
Exit Price  : {trade.get('exit_price', entry)}

RESULT
─────────────────────────────
Realized PnL: {pnl_text}
Result      : {'PROFIT 🎉' if is_profit else 'LOSS 😢'}"""
        
        return self._send_email(subject, body)

    def send_daily_loss_warning(self, daily_loss_usd: float, daily_limit_usd: float) -> bool:
        if not self.enabled:
            return False
        percent = (daily_loss_usd / daily_limit_usd) * 100
        subject = f"⚠️ DAILY LOSS WARNING - {percent:.1f}% of limit"

        body = f"""DAILY LOSS LIMIT WARNING
─────────────────────────────
Current Loss : ${daily_loss_usd:.2f}
Daily Limit  : ${daily_limit_usd:.2f}
Percentage   : {percent:.1f}%
Status       : {'⚠️ NEAR LIMIT - Caution!' if percent >= 80 else 'Monitoring'}"""
        
        return self._send_email(subject, body)

    def send_daily_limit_hit(self, daily_loss_usd: float, daily_limit_usd: float) -> bool:
        if not self.enabled:
            return False
        subject = "🛑 DAILY LOSS LIMIT HIT - TRADING STOPPED"

        body = f"""DAILY LOSS LIMIT REACHED - TRADING HALTED
─────────────────────────────
Current Loss : ${daily_loss_usd:.2f}
Daily Limit  : ${daily_limit_usd:.2f}
Status       : TRADING HALTED UNTIL TOMORROW (UTC)
Action       : No new trades will be placed"""
        
        return self._send_email(subject, body)

    def send_startup_report(self, config: dict, symbols: List[str], harami_tolerance: float) -> bool:
        if not self.enabled:
            return False
        mode         = "LIVE TRADING" if not config.get("paper_mode") else "PAPER MODE"
        subject      = f"🤖 TRADING BOT STARTED - {mode}"
        
        symbols_list = "\n".join([f"  • {sym}" for sym in symbols[:10]])
        if len(symbols) > 10:
            symbols_list += f"\n  • ... and {len(symbols) - 10} more"

        body = f"""TRADING BOT STARTED - {mode}
─────────────────────────────
Timeframe      : {config.get('timeframe', '1h')}
Leverage       : {config.get('leverage', 5)}x
Risk/Trade     : {config.get('risk_pct', 2)}%
Max Trades     : {config.get('max_concurrent_trades', 2)}
Daily Loss Cap : {config.get('daily_loss_limit_pct', 0.05) * 100:.0f}%
Capital        : ${config.get('trading_capital', 0):,.2f}

TRADE DIRECTIONS
─────────────────────────────
SHORT TRADES   : {'ENABLED ✅' if config.get('enable_short', True) else 'DISABLED ❌'}
LONG TRADES    : {'ENABLED ✅' if config.get('enable_long', True) else 'DISABLED ❌'}

RSI FILTERS
─────────────────────────────
SHORT RSI      : > 55
LONG RSI       : < 40
RSI LONG BLOCK : < 24 (extreme oversold)
NO RSI         : Range Break, Small→Large Body

STRATEGY PARAMETERS
─────────────────────────────
Take Profit    : 2:1 R:R (or SuperTrend exit if both ST confirm)
Stop Loss      : Triggers on CANDLE CLOSE
SuperTrend 1   : Length=14, Factor=2.0
SuperTrend 2   : Length=21, Factor=1.0
Harami Tolerance: {harami_tolerance*100:.2f}%
Small Body Thresh: 25% of range
Large Body Factor: 2x avg
Price Display  : Auto-precision (up to 10 dp for micro-price alts)

MONITORED SYMBOLS ({len(symbols)})
─────────────────────────────
{symbols_list}"""
        
        return self._send_email(subject, body)


# ================================================================
#  5.  WEBSOCKET MANAGER
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
    ping_timeout: int  = 10
    max_reconnect_attempts: int    = 10
    reconnect_base_delay: int      = 5
    reconnect_max_delay: int       = 30
    reconnect_backoff_multiplier: int = 2


class DeltaWebSocket:
    def __init__(self, config: Optional[WSConfig] = None):
        self.config   = config or WSConfig()
        self._state   = WSState.DISCONNECTED
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

    def set_candle_callback(self, cb: callable)       -> None: self._on_candle_callback       = cb
    def set_connected_callback(self, cb: callable)    -> None: self._on_connected_callback    = cb
    def set_disconnected_callback(self, cb: callable) -> None: self._on_disconnected_callback = cb
    def set_error_callback(self, cb: callable)        -> None: self._on_error_callback        = cb
    def set_reconnect_callback(self, cb: callable)    -> None: self._on_reconnect_callback    = cb

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

    def start(self) -> None:
        if self._state == WSState.STOPPED:
            _log("error", "WS", "Cannot restart stopped WebSocket. Create new instance.")
            return
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                return
            self._should_stop.clear()
            self._state  = WSState.CONNECTING
            self._thread = threading.Thread(
                target=self._run_forever, daemon=True, name="DeltaWebSocket"
            )
            self._thread.start()

    def stop(self) -> None:
        self._should_stop.set()
        self._state = WSState.STOPPED
        if self._ws:
            try: self._ws.close()
            except Exception: pass
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)

    def is_connected(self) -> bool: return self._state == WSState.CONNECTED
    def get_state(self) -> str:     return self._state.value

    def _get_channel_name(self, timeframe: str) -> str:
        return {
            "1m": "candlestick_1m", "5m": "candlestick_5m",
            "15m": "candlestick_15m", "1h": "candlestick_1h",
        }.get(timeframe, f"candlestick_{timeframe}")

    def _run_forever(self) -> None:
        first_connect = True
        while not self._should_stop.is_set():
            try:
                self._connect()
                if not self._should_stop.is_set() and self._state != WSState.STOPPED:
                    if not first_connect and self._on_reconnect_callback:
                        try: self._on_reconnect_callback()
                        except Exception as e: _log("error", "WS", f"Reconnect callback error: {e}")
                    first_connect = False
                    self._reconnect()
            except Exception as e:
                _log("error", "WS", f"Unexpected error in WS loop: {e}")
                if not self._should_stop.is_set():
                    first_connect = False
                    self._reconnect()

    def _connect(self) -> None:
        self._state = WSState.CONNECTING
        self._ws = websocket.WebSocketApp(
            self.config.url,
            on_open=self._on_open, on_message=self._on_message,
            on_error=self._on_error, on_close=self._on_close,
        )
        self._ws.run_forever(
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            reconnect=0,
        )

    def _reconnect(self) -> None:
        if self._reconnect_attempts >= self.config.max_reconnect_attempts:
            self._state = WSState.DISCONNECTED
            if self._on_disconnected_callback:
                try: self._on_disconnected_callback()
                except Exception: pass
            return
        self._reconnect_attempts += 1
        self._state = WSState.RECONNECTING
        for _ in range(self._reconnect_delay):
            if self._should_stop.is_set(): return
            time.sleep(1)
        self._reconnect_delay = min(
            self._reconnect_delay * self.config.reconnect_backoff_multiplier,
            self.config.reconnect_max_delay,
        )

    def _on_open(self, ws) -> None:
        self._state              = WSState.CONNECTED
        self._reconnect_attempts = 0
        self._reconnect_delay    = self.config.reconnect_base_delay
        with self._subscription_lock:
            for channel, symbols in self._subscriptions.items():
                if symbols: self._send_subscription(channel, symbols)
        if self._on_connected_callback:
            try: self._on_connected_callback()
            except Exception: pass

    def _send_subscription(self, channel: str, symbols: List[str]) -> None:
        if not self._ws: return
        msg = {"type": "subscribe", "payload": {"channels": [{"name": channel, "symbols": symbols}]}}
        try: self._ws.send(json.dumps(msg))
        except Exception as e: _log("error", "WS", f"Failed to send subscription: {e}")

    def _on_message(self, ws, message: str) -> None:
        try:
            data     = json.loads(message)
            msg_type = data.get("type", "")
            if msg_type in ("subscribe", "error"): return
            candle_data = self._parse_candle_message(data)
            if candle_data and self._on_candle_callback:
                symbol = candle_data.get("symbol")
                candle = candle_data.get("candle")
                if symbol and candle:
                    try: self._on_candle_callback(symbol, candle)
                    except Exception as e: _log("error", "WS", f"Candle callback error: {e}")
        except Exception: pass

    def _parse_candle_message(self, data: dict) -> Optional[dict]:
        msg_type = data.get("type", "")
        if not msg_type.startswith("candlestick_"): return None
        ws_symbol = data.get("symbol", "")
        if not ws_symbol: return None
        trading_symbol = ws_symbol if ws_symbol.endswith("_PERP") else ws_symbol + "_PERP"
        candle = self._normalize_candle_flat(data)
        if candle is None: return None
        return {"symbol": trading_symbol, "candle": candle}

    def _normalize_candle_flat(self, data: dict) -> Optional[dict]:
        try:
            ts_raw = data.get("candle_start_time")
            if ts_raw is not None:
                ts = int(ts_raw)
                if ts > 1_000_000_000_000_000: ts = ts // 1_000_000_000
                elif ts > 10_000_000_000:      ts = ts // 1_000
            else:
                for key in ("time", "start", "t"):
                    v = data.get(key)
                    if v is not None:
                        ts = int(float(v))
                        if ts > 10_000_000_000: ts = ts // 1_000
                        break
                else: return None
            if ts <= 0: return None
            return {
                "time":   ts,
                "open":   float(data.get("open",   0)),
                "high":   float(data.get("high",   0)),
                "low":    float(data.get("low",    0)),
                "close":  float(data.get("close",  0)),
                "volume": float(data.get("volume", 0)),
            }
        except (TypeError, ValueError): return None

    def _on_error(self, ws, error) -> None:
        error_msg = str(error)
        if "10054" not in error_msg and "Connection reset" not in error_msg:
            _log("error", "WS", f"Error: {error_msg}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._state = WSState.DISCONNECTED


# ================================================================
#  6.  SYMBOL HANDLING
# ================================================================

def to_trading_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"): s = s[:-5]
    if s.endswith("USDT"):  s = s[:-4] + "USD"
    return s + "_PERP"

def to_ws_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"): s = s[:-5]
    if s.endswith("USDT"):  s = s[:-4] + "USD"
    return s

def to_candle_symbol(symbol: str) -> str:
    s = symbol.upper().strip()
    if s.endswith("_PERP"): s = s[:-5]
    if s.endswith("USDT"):  s = s[:-4] + "USD"
    return s


# ================================================================
#  7.  CONSTANTS
# ================================================================
REST_BASE_INDIA  = "https://api.india.delta.exchange"
REST_BASE_GLOBAL = "https://api.delta.exchange"

CANDLE_LIMIT        = 200
MAX_RETRIES         = 3
RETRY_DELAYS        = [5, 10, 15]
TIMEOUT             = 30
CANDLE_SAFETY_SHIFT = 1

RSI_PERIOD      = 14
RSI_OVERBOUGHT  = 55.0
RSI_OVERSOLD    = 40.0
RSI_MIN_CANDLES = RSI_PERIOD + 1

FILL_POLL_INTERVAL = 0.5
FILL_POLL_TIMEOUT  = 15

DOJI_BODY_RATIO_MAX = 0.10
TP_RR_RATIO         = 2.0
TP_MAX_PCT          = 0.05
DAILY_LOSS_LIMIT_PCT = 0.05

BREAKOUT_BODY_OVERLAP_MIN_PCT = 0.50
MIN_ENGULF_BODY_PCT           = 0.30

# ── Harami pattern tolerance (default, can be overridden by user input) ──
HARAMI_BODY_TOLERANCE = 0.001   # 0.1% default

# ── Range Break Strategy Constants ──────────────────────────────
RANGE_BREAK_LOOKBACK = 7          # Number of candles to identify range

# ── Small Body → Large Body Strategy Constants ──────────────────
SMALL_BODY_LOOKBACK = 7           # Number of candles to check for small bodies
LARGE_BODY_MULTIPLIER = 2.0       # Current body must be at least 2x the average of previous bodies
SMALL_BODY_THRESHOLD = 0.25       # 25% - Body must be less than 25% of range (UPDATED)

# ── SuperTrend parameters ─────────────────────────────────────
ST1_LENGTH = 14    # Accurate SuperTrend
ST1_FACTOR = 2.0
ST2_LENGTH = 21    # Trend SuperTrend
ST2_FACTOR = 1.0

TIMEFRAME_MAP: Dict[str, Dict] = {
    "1m":  {"resolution": "1m",  "api_resolution": "1m",  "ws_channel": "candlestick_1m",  "secs": 60},
    "5m":  {"resolution": "5m",  "api_resolution": "5m",  "ws_channel": "candlestick_5m",  "secs": 300},
    "15m": {"resolution": "15m", "api_resolution": "15m", "ws_channel": "candlestick_15m", "secs": 900},
    "1h":  {"resolution": "1h",  "api_resolution": "1h",  "ws_channel": "candlestick_1h",  "secs": 3600},
}


# ================================================================
#  8.  CONNECTION WARM-UP
# ================================================================

def warm_up_connection() -> None:
    try:
        resp = requests.get(REST_BASE_INDIA, timeout=5)
        _log("info", "WARM-UP", f"Warmed up (status={resp.status_code})")
    except Exception as exc:
        _log("warning", "WARM-UP", f"Warm-up failed (non-critical): {exc}")


# ================================================================
#  9.  API REQUEST HANDLER
# ================================================================

class APIRequestHandler:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.session    = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent":   "python-DeltaBot/12.2",
            "Accept":       "application/json",
            "Connection":   "keep-alive",
        })
        self.session.mount("https://", requests.adapters.HTTPAdapter(
            pool_connections=20, pool_maxsize=40, max_retries=0, pool_block=False
        ))

    def _get_base_url(self, endpoint_type: str) -> str:
        return REST_BASE_INDIA

    def _sign_request(self, method: str, path: str,
                      params: dict = None, body: dict = None) -> dict:
        if not self.api_key or not self.api_secret:
            raise ValueError("API key/secret missing")
        timestamp    = str(int(time.time()))
        query_string = ""
        if params:
            sorted_params = sorted(params.items())
            query_string  = "?" + urlencode(sorted_params)
        body_string = ""
        if body and method.upper() != "GET":
            body_string = json.dumps(body)
        message = method.upper() + timestamp + path + query_string + body_string
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {"api-key": self.api_key, "timestamp": timestamp, "signature": signature}

    def request(self, method: str, endpoint: str, endpoint_type: str = "public",
                params: dict = None, body: dict = None,
                retry_count: int = 0) -> Optional[Dict]:
        base_url = self._get_base_url(endpoint_type)
        url      = base_url + endpoint
        headers  = {}
        if endpoint_type == "private":
            headers = self._sign_request(method, endpoint, params, body)
        if body and method.upper() != "GET":
            headers["Content-Type"] = "application/json"
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
            try: response_data = response.json()
            except Exception: response_data = {"error": response.text}
            if response.status_code == 401:
                _log("error", "AUTH", f"Authentication failed (401). Response: {response_data}")
                return None
            response.raise_for_status()
            return response_data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if retry_count < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAYS[retry_count])
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            return None
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else 0
            if status == 429 and retry_count < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAYS[retry_count])
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            return None
        except Exception as exc:
            _log("error", "API-REQ", f"Unexpected error: {exc}")
            return None


# ================================================================
#  10. TIMEFRAME SAFETY
# ================================================================

class TimeframeSafe:
    def __init__(self, key: str):
        k = key.strip().lower()
        if k not in TIMEFRAME_MAP:
            raise ValueError(f"[TIMEFRAME] '{k}' not valid. Choose from: {list(TIMEFRAME_MAP)}")
        entry                = TIMEFRAME_MAP[k]
        self._key            = k
        self._resolution     = entry["resolution"]
        self._api_resolution = entry["api_resolution"]
        self._ws_channel     = entry["ws_channel"]
        self._secs           = entry["secs"]

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
#  11. CANDLE VALIDATOR
# ================================================================

_CANDLE_FIELDS = ("time", "open", "high", "low", "close", "volume")


def validate_candle(candle: dict, symbol: str = "") -> bool:
    for f in _CANDLE_FIELDS:
        if f not in candle or candle[f] is None: return False
    for f in ("open", "high", "low", "close", "volume"):
        try:
            v = float(candle[f])
        except (TypeError, ValueError): return False
        if math.isnan(v) or math.isinf(v): return False
    if candle["time"] <= 0:           return False
    if candle["high"] < candle["low"]: return False
    for f in ("open", "high", "low", "close"):
        if candle[f] <= 0: return False
    return True


# ================================================================
#  12. RSI CALCULATION
# ================================================================

def compute_rsi(closed_candles: List[dict], period: int = RSI_PERIOD) -> Optional[float]:
    if len(closed_candles) < period + 1:
        return None
    closes  = [float(c["close"]) for c in closed_candles]
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    seed    = changes[:period]
    avg_gain = sum(max(ch, 0.0) for ch in seed) / period
    avg_loss = sum(abs(min(ch, 0.0)) for ch in seed) / period
    for ch in changes[period:]:
        avg_gain = (avg_gain * (period - 1) + max(ch, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + abs(min(ch, 0.0))) / period
    if avg_loss == 0:
        return 100.0
    rs  = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def check_rsi_filter(closed_candles: List[dict], symbol: str = "",
                     period: int = RSI_PERIOD,
                     threshold: float = RSI_OVERBOUGHT) -> Tuple[bool, Optional[float]]:
    rsi = compute_rsi(closed_candles, period)
    if rsi is None:
        _log("warning", f"RSI [{symbol}]",
             f"Insufficient candles for RSI({period}): have {len(closed_candles)}, "
             f"need {period + 1} — BLOCKING")
        return False, None
    _log("info", f"RSI [{symbol}]", f"RSI({period}) = {rsi:.2f}  (threshold > {threshold})")
    return (rsi > threshold), rsi


# ================================================================
#  12b. SUPERTREND CALCULATION
# ================================================================

def compute_atr(candles: List[dict], period: int) -> List[float]:
    n = len(candles)
    if n < 2:
        return [0.0] * n

    trs = [0.0]
    for i in range(1, n):
        h  = candles[i]["high"]
        l  = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    atrs = [0.0] * n
    if n < period + 1:
        return atrs

    seed = sum(trs[1 : period + 1]) / period
    atrs[period] = seed
    alpha = 1.0 / period
    for i in range(period + 1, n):
        atrs[i] = atrs[i - 1] * (1 - alpha) + trs[i] * alpha

    return atrs


def compute_supertrend(candles: List[dict],
                        length: int, factor: float) -> List[Optional[bool]]:
    n    = len(candles)
    atrs = compute_atr(candles, length)

    directions: List[Optional[bool]] = [None] * n
    upper_bands  = [0.0] * n
    lower_bands  = [0.0] * n

    start = length

    for i in range(start, n):
        hl2 = (candles[i]["high"] + candles[i]["low"]) / 2.0
        atr = atrs[i]

        raw_upper = hl2 + factor * atr
        raw_lower = hl2 - factor * atr

        if i == start:
            upper_bands[i] = raw_upper
            lower_bands[i] = raw_lower
            directions[i]  = candles[i]["close"] >= hl2
        else:
            prev_upper = upper_bands[i - 1]
            prev_lower = lower_bands[i - 1]
            prev_close = candles[i - 1]["close"]

            lower_bands[i] = (
                raw_lower if raw_lower > prev_lower or prev_close < prev_lower
                else prev_lower
            )
            upper_bands[i] = (
                raw_upper if raw_upper < prev_upper or prev_close > prev_upper
                else prev_upper
            )

            prev_dir = directions[i - 1]
            close    = candles[i]["close"]

            if prev_dir is False:
                directions[i] = close > upper_bands[i]
            elif prev_dir is True:
                directions[i] = close >= lower_bands[i]
            else:
                directions[i] = close >= hl2

    return directions


def get_supertrend_state(candles: List[dict],
                          length: int, factor: float) -> Optional[bool]:
    if len(candles) < length + 2:
        return None
    dirs = compute_supertrend(candles, length, factor)
    for d in reversed(dirs):
        if d is not None:
            return d
    return None


def both_supertrends_bullish(candles: List[dict]) -> bool:
    st1 = get_supertrend_state(candles, ST1_LENGTH, ST1_FACTOR)
    st2 = get_supertrend_state(candles, ST2_LENGTH, ST2_FACTOR)
    return st1 is True and st2 is True


def both_supertrends_bearish(candles: List[dict]) -> bool:
    st1 = get_supertrend_state(candles, ST1_LENGTH, ST1_FACTOR)
    st2 = get_supertrend_state(candles, ST2_LENGTH, ST2_FACTOR)
    return st1 is False and st2 is False


def get_supertrend_values(candles: List[dict]) -> Tuple[Optional[float], Optional[float]]:
    def _get_last_band(cndls, length, factor):
        n    = len(cndls)
        atrs = compute_atr(cndls, length)
        if n < length + 1:
            return None
        upper_bands = [0.0] * n
        lower_bands = [0.0] * n
        dirs: List[Optional[bool]] = [None] * n
        start = length
        for i in range(start, n):
            hl2       = (cndls[i]["high"] + cndls[i]["low"]) / 2.0
            atr       = atrs[i]
            raw_upper = hl2 + factor * atr
            raw_lower = hl2 - factor * atr
            if i == start:
                upper_bands[i] = raw_upper
                lower_bands[i] = raw_lower
                dirs[i]        = cndls[i]["close"] >= hl2
            else:
                prev_upper = upper_bands[i - 1]
                prev_lower = lower_bands[i - 1]
                prev_close = cndls[i - 1]["close"]
                lower_bands[i] = (
                    raw_lower if raw_lower > prev_lower or prev_close < prev_lower
                    else prev_lower
                )
                upper_bands[i] = (
                    raw_upper if raw_upper < prev_upper or prev_close > prev_upper
                    else prev_upper
                )
                prev_dir   = dirs[i - 1]
                close      = cndls[i]["close"]
                if prev_dir is False:
                    dirs[i] = close > upper_bands[i]
                elif prev_dir is True:
                    dirs[i] = close >= lower_bands[i]
                else:
                    dirs[i] = close >= hl2
        if dirs[n - 1] is True:
            return lower_bands[n - 1]
        elif dirs[n - 1] is False:
            return upper_bands[n - 1]
        return None

    v1 = _get_last_band(candles, ST1_LENGTH, ST1_FACTOR)
    v2 = _get_last_band(candles, ST2_LENGTH, ST2_FACTOR)
    return v1, v2


# ================================================================
#  13. STRATEGY HELPERS
# ================================================================

def candle_body(c: dict)  -> float: return abs(c["close"] - c["open"])
def upper_wick(c: dict)   -> float: return c["high"] - max(c["open"], c["close"])
def lower_wick(c: dict)   -> float: return min(c["open"], c["close"]) - c["low"]
def candle_range(c: dict) -> float: return c["high"] - c["low"]
def is_bullish(c: dict)   -> bool:  return c["close"] > c["open"]
def is_bearish(c: dict)   -> bool:  return c["close"] < c["open"]

def is_doji(c: dict, body_ratio_max: float = DOJI_BODY_RATIO_MAX) -> bool:
    r = candle_range(c)
    if r <= 0: return False
    return (candle_body(c) / r) <= body_ratio_max

def body_pct_of_range(c: dict) -> float:
    r = candle_range(c)
    if r <= 0: return 0.0
    return candle_body(c) / r


# ================================================================
#  13a. SHORT STRATEGIES
# ================================================================

def check_short_signal_strategy_1(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 5: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]
    i3     = candles[-4]; i4 = candles[-5]
    if not (i3["close"] > i4["high"] and i2["close"] > i3["high"]): return False, None, ""
    i1_body_top    = max(i1["open"], i1["close"])
    i1_body_bottom = min(i1["open"], i1["close"])
    i2_body_top    = max(i2["open"], i2["close"])
    i2_body_bottom = min(i2["open"], i2["close"])
    overlap        = max(0.0, min(i1_body_top, i2_body_top) - max(i1_body_bottom, i2_body_bottom))
    i1_body_size   = candle_body(i1)
    if i1_body_size <= 0 or (overlap / i1_body_size) < BREAKOUT_BODY_OVERLAP_MIN_PCT:
        return False, None, ""
    if not is_bearish(i1):     return False, None, ""
    if not is_bearish(signal): return False, None, ""
    if signal["close"] >= i1["close"]: return False, None, ""
    sc = signal.copy()
    sc["pattern_high"] = max(i2["high"], i1["high"], signal["high"])
    _log("info", "STRATEGY_1_SHORT", "Double Breakout + reversal")
    return True, sc, "STRATEGY_1_SHORT"


def check_short_signal_strategy_3(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]; i3 = candles[-4]
    if i2["close"] <= i3["close"]: return False, None, ""
    if not is_bullish(i2):         return False, None, ""
    if not is_bearish(i1):         return False, None, ""
    if body_pct_of_range(i1) < MIN_ENGULF_BODY_PCT: return False, None, ""
    if not (i1["open"] > i2["close"] and i1["close"] < i2["open"]): return False, None, ""
    if not is_bearish(signal):              return False, None, ""
    if signal["close"] >= i1["close"]:     return False, None, ""
    sc = signal.copy()
    sc["pattern_high"] = max(i1["high"], signal["high"])
    _log("info", "STRATEGY_3_SHORT", "Bearish Engulfing confirmed")
    return True, sc, "STRATEGY_3_SHORT"


def check_short_signal_strategy_5(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Doji Short Strategy - UPDATED:
    Signal candle must close BELOW the doji candle's LOW.
    """
    if len(candles) < 4: return False, None, ""
    signal_c = candles[-1]; doji_c = candles[-2]; i1 = candles[-3]; i2 = candles[-4]
    if not is_bullish(i2):             return False, None, ""
    if not is_bullish(i1):             return False, None, ""
    if not is_doji(doji_c):            return False, None, ""
    if doji_c["high"] <= i1["high"]:   return False, None, ""
    if not is_bearish(signal_c):       return False, None, ""
    # UPDATED: Signal candle must close BELOW doji low
    if signal_c["close"] >= doji_c["low"]: return False, None, ""
    result = signal_c.copy()
    result["doji_low"] = doji_c["low"]      # Use doji low as reference
    _log("info", "BEARISH_DOJI", f"Signal close {signal_c['close']} < doji low {doji_c['low']}")
    return True, result, "BEARISH_DOJI"


def check_short_signal_strategy_6(candles: List[dict], harami_tolerance: float = HARAMI_BODY_TOLERANCE) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]; i3 = candles[-4]
    if not is_bullish(i3):             return False, None, ""
    if not is_bullish(i2):             return False, None, ""
    if i2["close"] <= i3["close"]:     return False, None, ""
    if not is_bearish(i1):             return False, None, ""
    i2_body_top    = max(i2["open"], i2["close"])
    i2_body_bottom = min(i2["open"], i2["close"])
    i1_body_top    = max(i1["open"], i1["close"])
    i1_body_bottom = min(i1["open"], i1["close"])
    
    i1_body_size = candle_body(i1)
    tolerance = max(i1_body_size * harami_tolerance, 0.0001)
    
    if not (i1_body_top >= i2_body_top - tolerance and i1_body_bottom <= i2_body_bottom + tolerance):
        return False, None, ""
    
    if not is_bearish(signal):             return False, None, ""
    if signal["close"] >= i1["close"]:     return False, None, ""
    sc = signal.copy()
    sc["pattern_high"] = max(i2["high"], i1["high"])
    _log("info", "BEARISH_HARAMI", f"Bearish Harami with {harami_tolerance*100:.1f}% tolerance")
    return True, sc, "BEARISH_HARAMI"


# ─── RANGE BREAK SHORT STRATEGY ────────────────────────────────

def check_short_signal_range_break(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Range Break Short Strategy - UPDATED:
    Signal candle must close BELOW the break candle's CLOSE.
    """
    lookback = RANGE_BREAK_LOOKBACK
    if len(candles) < lookback + 2:
        return False, None, ""
    
    # Look at lookback candles for range (excluding the break candle)
    range_candles = candles[-(lookback + 2):-2]
    if len(range_candles) < lookback:
        return False, None, ""
    
    # Calculate range high and low
    range_high = max(c["high"] for c in range_candles)
    range_low = min(c["low"] for c in range_candles)
    range_size = range_high - range_low
    
    if range_size <= 0:
        return False, None, ""
    
    # Break candle is the second-to-last candle (index -2)
    break_candle = candles[-2]
    signal_candle = candles[-1]
    
    # Check if break candle broke below the range
    if break_candle["low"] >= range_low:
        return False, None, ""
    
    # Check if break candle's close is below range low (confirmation of break)
    if break_candle["close"] >= range_low:
        return False, None, ""
    
    # Signal candle must be bearish
    if not is_bearish(signal_candle):
        return False, None, ""
    
    # UPDATED: Signal candle must close BELOW break candle's CLOSE
    if signal_candle["close"] >= break_candle["close"]:
        return False, None, ""
    
    # Set stop loss at the break candle's high (or range high if higher)
    signal_candle_copy = signal_candle.copy()
    signal_candle_copy["pattern_high"] = max(break_candle["high"], range_high)
    
    _log("info", "RANGE_BREAK_SHORT", 
         f"Range {smart_fmt(range_low)} - {smart_fmt(range_high)} broken below at {smart_fmt(break_candle['low'])}, "
         f"signal close {signal_candle['close']} < break close {break_candle['close']}")
    return True, signal_candle_copy, "RANGE_BREAK_SHORT"


# ─── SMALL BODY → LARGE BODY SHORT STRATEGY ─────────────────────

def check_short_signal_small_to_large_body(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Small Body → Large Body Short Strategy (NO RSI):
    1. Check previous 7 candles have small bodies (ignore wicks)
    2. Current candle has a large body (at least 2x average of previous)
    3. Current candle is RED (bearish) → take SHORT trade immediately
    """
    lookback = SMALL_BODY_LOOKBACK
    if len(candles) < lookback + 1:
        return False, None, ""
    
    # Get the previous lookback candles (excluding current)
    prev_candles = candles[-(lookback + 1):-1]
    if len(prev_candles) < lookback:
        return False, None, ""
    
    current = candles[-1]
    
    # Calculate average body size of previous candles (ignoring wicks)
    avg_body = sum(candle_body(c) for c in prev_candles) / lookback
    
    if avg_body <= 0:
        return False, None, ""
    
    current_body = candle_body(current)
    
    # Current body must be at least LARGE_BODY_MULTIPLIER times the average
    if current_body < avg_body * LARGE_BODY_MULTIPLIER:
        return False, None, ""
    
    # Check if previous candles have small bodies (body is small relative to their range)
    # UPDATED: Now using SMALL_BODY_THRESHOLD = 0.25 (25%)
    for c in prev_candles:
        if body_pct_of_range(c) >= SMALL_BODY_THRESHOLD:
            return False, None, ""
    
    # Current candle must be bearish (RED) for short
    if not is_bearish(current):
        return False, None, ""
    
    # Set stop loss at current candle's high
    current_copy = current.copy()
    current_copy["pattern_high"] = current["high"]
    current_copy["small_body_lookback"] = lookback
    current_copy["avg_body"] = avg_body
    
    _log("info", "SMALL_TO_LARGE_SHORT", 
         f"Small body avg {smart_fmt(avg_body)} → large body {smart_fmt(current_body)} "
         f"({current_body/avg_body:.1f}x) - RED candle")
    return True, current_copy, "SMALL_TO_LARGE_SHORT"


# ================================================================
#  13b. LONG STRATEGIES
# ================================================================

def check_long_signal_strategy_1(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 5: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]
    i3     = candles[-4]; i4 = candles[-5]
    if not (i3["close"] < i4["low"] and i2["close"] < i3["low"]): return False, None, ""
    i1_body_top    = max(i1["open"], i1["close"])
    i1_body_bottom = min(i1["open"], i1["close"])
    i2_body_top    = max(i2["open"], i2["close"])
    i2_body_bottom = min(i2["open"], i2["close"])
    overlap        = max(0.0, min(i1_body_top, i2_body_top) - max(i1_body_bottom, i2_body_bottom))
    i1_body_size   = candle_body(i1)
    if i1_body_size <= 0 or (overlap / i1_body_size) < BREAKOUT_BODY_OVERLAP_MIN_PCT:
        return False, None, ""
    if not is_bullish(i1):              return False, None, ""
    if not is_bullish(signal):          return False, None, ""
    if signal["close"] <= i1["close"]:  return False, None, ""
    sc = signal.copy()
    sc["pattern_low"] = min(i2["low"], i1["low"], signal["low"])
    _log("info", "STRATEGY_1_LONG", "Double Downward Breakout + bullish reversal")
    return True, sc, "STRATEGY_1_LONG"


def check_long_signal_strategy_3(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]; i3 = candles[-4]
    if i2["close"] >= i3["close"]:     return False, None, ""
    if not is_bearish(i2):             return False, None, ""
    if not is_bullish(i1):             return False, None, ""
    if body_pct_of_range(i1) < MIN_ENGULF_BODY_PCT: return False, None, ""
    if not (i1["open"] < i2["close"] and i1["close"] > i2["open"]): return False, None, ""
    if not is_bullish(signal):             return False, None, ""
    if signal["close"] <= i1["close"]:    return False, None, ""
    sc = signal.copy()
    sc["pattern_low"] = min(i1["low"], signal["low"])
    _log("info", "STRATEGY_3_LONG", "Bullish Engulfing confirmed")
    return True, sc, "BULLISH_ENGULFING"


def check_long_signal_strategy_5(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Doji Long Strategy - UPDATED:
    Signal candle must close ABOVE the doji candle's HIGH.
    """
    if len(candles) < 4: return False, None, ""
    signal_c = candles[-1]; doji_c = candles[-2]; i1 = candles[-3]; i2 = candles[-4]
    if not is_bearish(i2):             return False, None, ""
    if not is_bearish(i1):             return False, None, ""
    if not is_doji(doji_c):            return False, None, ""
    if doji_c["low"] >= i1["low"]:     return False, None, ""
    if not is_bullish(signal_c):       return False, None, ""
    # UPDATED: Signal candle must close ABOVE doji high
    if signal_c["close"] <= doji_c["high"]: return False, None, ""
    result = signal_c.copy()
    result["doji_high"] = doji_c["high"]    # Use doji high as reference
    _log("info", "BULLISH_DOJI", f"Signal close {signal_c['close']} > doji high {doji_c['high']}")
    return True, result, "BULLISH_DOJI"


def check_long_signal_strategy_6(candles: List[dict], harami_tolerance: float = HARAMI_BODY_TOLERANCE) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4: return False, None, ""
    signal = candles[-1]; i1 = candles[-2]; i2 = candles[-3]; i3 = candles[-4]
    if not is_bearish(i3):             return False, None, ""
    if not is_bearish(i2):             return False, None, ""
    if i2["close"] >= i3["close"]:     return False, None, ""
    if not is_bullish(i1):             return False, None, ""
    i2_body_top    = max(i2["open"], i2["close"])
    i2_body_bottom = min(i2["open"], i2["close"])
    i1_body_top    = max(i1["open"], i1["close"])
    i1_body_bottom = min(i1["open"], i1["close"])
    
    i1_body_size = candle_body(i1)
    tolerance = max(i1_body_size * harami_tolerance, 0.0001)
    
    if not (i1_body_top >= i2_body_top - tolerance and i1_body_bottom <= i2_body_bottom + tolerance):
        return False, None, ""
    
    if not is_bullish(signal):             return False, None, ""
    if signal["close"] <= i1["close"]:     return False, None, ""
    sc = signal.copy()
    sc["pattern_low"] = min(i2["low"], i1["low"])
    _log("info", "BULLISH_HARAMI", f"Bullish Harami with {harami_tolerance*100:.1f}% tolerance")
    return True, sc, "BULLISH_HARAMI"


# ─── RANGE BREAK LONG STRATEGY ────────────────────────────────

def check_long_signal_range_break(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Range Break Long Strategy - UPDATED:
    Signal candle must close ABOVE the break candle's CLOSE.
    """
    lookback = RANGE_BREAK_LOOKBACK
    if len(candles) < lookback + 2:
        return False, None, ""
    
    # Look at lookback candles for range (excluding the break candle)
    range_candles = candles[-(lookback + 2):-2]
    if len(range_candles) < lookback:
        return False, None, ""
    
    # Calculate range high and low
    range_high = max(c["high"] for c in range_candles)
    range_low = min(c["low"] for c in range_candles)
    range_size = range_high - range_low
    
    if range_size <= 0:
        return False, None, ""
    
    # Break candle is the second-to-last candle (index -2)
    break_candle = candles[-2]
    signal_candle = candles[-1]
    
    # Check if break candle broke above the range
    if break_candle["high"] <= range_high:
        return False, None, ""
    
    # Check if break candle's close is above range high (confirmation of break)
    if break_candle["close"] <= range_high:
        return False, None, ""
    
    # Signal candle must be bullish
    if not is_bullish(signal_candle):
        return False, None, ""
    
    # UPDATED: Signal candle must close ABOVE break candle's CLOSE
    if signal_candle["close"] <= break_candle["close"]:
        return False, None, ""
    
    # Set stop loss at the break candle's low (or range low if lower)
    signal_candle_copy = signal_candle.copy()
    signal_candle_copy["pattern_low"] = min(break_candle["low"], range_low)
    
    _log("info", "RANGE_BREAK_LONG", 
         f"Range {smart_fmt(range_low)} - {smart_fmt(range_high)} broken above at {smart_fmt(break_candle['high'])}, "
         f"signal close {signal_candle['close']} > break close {break_candle['close']}")
    return True, signal_candle_copy, "RANGE_BREAK_LONG"


# ─── SMALL BODY → LARGE BODY LONG STRATEGY ─────────────────────

def check_long_signal_small_to_large_body(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    """
    Small Body → Large Body Long Strategy (NO RSI):
    1. Check previous 7 candles have small bodies (ignore wicks)
    2. Current candle has a large body (at least 2x average of previous)
    3. Current candle is GREEN (bullish) → take LONG trade immediately
    """
    lookback = SMALL_BODY_LOOKBACK
    if len(candles) < lookback + 1:
        return False, None, ""
    
    # Get the previous lookback candles (excluding current)
    prev_candles = candles[-(lookback + 1):-1]
    if len(prev_candles) < lookback:
        return False, None, ""
    
    current = candles[-1]
    
    # Calculate average body size of previous candles (ignoring wicks)
    avg_body = sum(candle_body(c) for c in prev_candles) / lookback
    
    if avg_body <= 0:
        return False, None, ""
    
    current_body = candle_body(current)
    
    # Current body must be at least LARGE_BODY_MULTIPLIER times the average
    if current_body < avg_body * LARGE_BODY_MULTIPLIER:
        return False, None, ""
    
    # Check if previous candles have small bodies (body is small relative to their range)
    # UPDATED: Now using SMALL_BODY_THRESHOLD = 0.25 (25%)
    for c in prev_candles:
        if body_pct_of_range(c) >= SMALL_BODY_THRESHOLD:
            return False, None, ""
    
    # Current candle must be bullish (GREEN) for long
    if not is_bullish(current):
        return False, None, ""
    
    # Set stop loss at current candle's low
    current_copy = current.copy()
    current_copy["pattern_low"] = current["low"]
    current_copy["small_body_lookback"] = lookback
    current_copy["avg_body"] = avg_body
    
    _log("info", "SMALL_TO_LARGE_LONG", 
         f"Small body avg {smart_fmt(avg_body)} → large body {smart_fmt(current_body)} "
         f"({current_body/avg_body:.1f}x) - GREEN candle")
    return True, current_copy, "SMALL_TO_LARGE_LONG"


# ================================================================
#  13c. SIGNAL CHECKERS
# ================================================================

def check_short_signal(
    candles: List[dict], symbol: str = "", harami_tolerance: float = HARAMI_BODY_TOLERANCE
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    closed_candles = candles[:-1]
    rsi_passes, rsi_value = check_rsi_filter(
        closed_candles, symbol=symbol, period=RSI_PERIOD, threshold=RSI_OVERBOUGHT
    )
    if not rsi_passes:
        return False, None, "", rsi_value
    for checker in (
        check_short_signal_strategy_1,
        check_short_signal_strategy_3,
        check_short_signal_strategy_5,  # Updated Doji strategy
    ):
        triggered, signal_candle, strategy = checker(candles)
        if triggered:
            _log("info", "SIGNAL",
                 f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} — CONFIRMED")
            return True, signal_candle, strategy, rsi_value
    
    # Check Harami with tolerance
    triggered, signal_candle, strategy = check_short_signal_strategy_6(candles, harami_tolerance)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} — CONFIRMED")
        return True, signal_candle, strategy, rsi_value
    
    return False, None, "", rsi_value


def check_short_signal_no_rsi(
    candles: List[dict], symbol: str = "", harami_tolerance: float = HARAMI_BODY_TOLERANCE
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    """
    Check short signals WITHOUT RSI filter.
    Strategies: Range Break, Small→Large Body
    """
    # Check Range Break first (no RSI required)
    triggered, signal_candle, strategy = check_short_signal_range_break(candles)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] SHORT {strategy} | NO RSI FILTER — CONFIRMED")
        return True, signal_candle, strategy, None
    
    # Check Small→Large Body (no RSI required)
    triggered, signal_candle, strategy = check_short_signal_small_to_large_body(candles)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] SHORT {strategy} | NO RSI FILTER — CONFIRMED")
        return True, signal_candle, strategy, None
    
    return False, None, "", None


def check_long_signal(
    candles: List[dict], symbol: str = "", harami_tolerance: float = HARAMI_BODY_TOLERANCE
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    closed_candles = candles[:-1]
    rsi = compute_rsi(closed_candles, RSI_PERIOD)
    if rsi is None:
        return False, None, "", None
    if rsi < 24.0:
        _log("warning", f"RSI [{symbol}]",
             f"RSI={rsi:.2f} < 24 — BLOCKING LONG TRADE (extreme oversold)")
        return False, None, "", rsi
    if rsi >= RSI_OVERSOLD:
        return False, None, "", rsi
    _log("info", f"RSI [{symbol}]", f"RSI={rsi:.2f} < {RSI_OVERSOLD} — LONG filter passes")
    for checker in (
        check_long_signal_strategy_1,
        check_long_signal_strategy_3,
        check_long_signal_strategy_5,  # Updated Doji strategy
    ):
        triggered, signal_candle, strategy = checker(candles)
        if triggered:
            _log("info", "SIGNAL",
                 f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} — CONFIRMED")
            return True, signal_candle, strategy, rsi
    
    # Check Harami with tolerance
    triggered, signal_candle, strategy = check_long_signal_strategy_6(candles, harami_tolerance)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} — CONFIRMED")
        return True, signal_candle, strategy, rsi
    
    return False, None, "", rsi


def check_long_signal_no_rsi(
    candles: List[dict], symbol: str = "", harami_tolerance: float = HARAMI_BODY_TOLERANCE
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    """
    Check long signals WITHOUT RSI filter.
    Strategies: Range Break, Small→Large Body
    """
    # Check Range Break first (no RSI required)
    triggered, signal_candle, strategy = check_long_signal_range_break(candles)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] LONG {strategy} | NO RSI FILTER — CONFIRMED")
        return True, signal_candle, strategy, None
    
    # Check Small→Large Body (no RSI required)
    triggered, signal_candle, strategy = check_long_signal_small_to_large_body(candles)
    if triggered:
        _log("info", "SIGNAL",
             f"[{symbol}] LONG {strategy} | NO RSI FILTER — CONFIRMED")
        return True, signal_candle, strategy, None
    
    return False, None, "", None


# ================================================================
#  14. SYMBOL VALIDATOR
# ================================================================

class SymbolValidator:
    def __init__(self, product_map: Dict[str, int]):
        self.product_map   = product_map
        self.known_symbols = list(product_map.keys())

    def validate_trading_symbol(self, symbol: str) -> Tuple[Optional[str], str]:
        trading_sym = to_trading_symbol(symbol)
        if trading_sym in self.product_map:
            return trading_sym, "OK"
        matches = difflib.get_close_matches(trading_sym, self.known_symbols, n=3, cutoff=0.5)
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
#  15. TIME-RANGE HELPER
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
    return start, safe_end


def get_time_range_with_retry_shift(
    num_candles: int, timeframe_minutes: int, shift_candles: int = 0,
) -> Tuple[int, int]:
    start, end = get_time_range(num_candles, timeframe_minutes)
    if shift_candles > 0:
        secs  = shift_candles * timeframe_minutes * 60
        start = max(1, start - secs)
        end   = max(timeframe_minutes * 60, end - secs)
    return start, end


# ================================================================
#  16. CANDLE PARSING HELPERS
# ================================================================

def _extract_timestamp(src: dict) -> int:
    for key in ("start", "time", "open_time", "t", "timestamp"):
        v = src.get(key)
        if v is not None:
            try:
                ts = int(float(v))
                if ts > 0: return ts
            except (TypeError, ValueError): continue
    return 0


def _extract_price(src: dict, long_key: str, short_key: str) -> Optional[float]:
    for k in (long_key, short_key):
        v = src.get(k)
        if v is not None:
            try: return float(v)
            except (TypeError, ValueError): pass
    return None


def _parse_rest_candle_row(row: dict, symbol: str = "") -> Optional[dict]:
    ts = _extract_timestamp(row)
    if ts <= 0: return None
    o  = _extract_price(row, "open",   "o")
    h  = _extract_price(row, "high",   "h")
    l  = _extract_price(row, "low",    "l")
    c  = _extract_price(row, "close",  "c")
    v  = _extract_price(row, "volume", "v") or 0.0
    if any(x is None for x in (o, h, l, c)): return None
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


# ================================================================
#  17. TICK SIZE ROUNDING
# ================================================================

def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 8)
    rounded  = round(price / tick_size) * tick_size
    tick_str = f"{tick_size:.10f}".rstrip("0")
    dp       = len(tick_str.split(".")[-1]) if "." in tick_str else 0
    return round(rounded, max(dp, 2))


# ================================================================
#  18. DELTA REST CLIENT
# ================================================================

class DeltaREST:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key         = api_key
        self.api_secret      = api_secret
        self.request_handler = APIRequestHandler(api_key, api_secret)
        self._tick_sizes: Dict[str, float] = {}

    def verify_account(self) -> Optional[dict]:
        result = self.request_handler.request("GET", "/v2/profile", endpoint_type="private")
        if result and "result" in result:
            profile = result["result"]
            _log("info", "AUTH", f"Authenticated: {profile.get('email','?')}")
            return profile
        return None

    def get_usd_balance(self) -> float:
        result = self.request_handler.request("GET", "/v2/wallet/balances", endpoint_type="private")
        if result and "result" in result:
            for asset in result.get("result", []):
                if asset.get("asset_symbol") in ("USDT", "USD"):
                    bal = float(asset.get("available_balance", 0))
                    _log("info", "BALANCE", f"Balance ({asset.get('asset_symbol')}) available: {bal:,.2f}")
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
                        self._tick_sizes[sym] = float(tick_raw)
                    except (TypeError, ValueError):
                        self._tick_sizes[sym] = 0.01
        return pmap

    def get_tick_size(self, symbol: str) -> float:
        return self._tick_sizes.get(symbol, 0.01)

    def get_order(self, order_id: int) -> Optional[dict]:
        result = self.request_handler.request(
            "GET", f"/v2/orders/{order_id}", endpoint_type="private"
        )
        if result and "result" in result:
            return result["result"]
        return None

    def wait_for_fill(self, order_id: int, symbol: str = "") -> Tuple[bool, int]:
        deadline = time.time() + FILL_POLL_TIMEOUT
        while time.time() < deadline:
            order = self.get_order(order_id)
            if order is None:
                time.sleep(FILL_POLL_INTERVAL)
                continue
            state         = order.get("state", "")
            size          = int(order.get("size", 0))
            unfilled_size = int(order.get("unfilled_size", 0))
            filled_size   = size - unfilled_size
            if state == "closed" and unfilled_size == 0:
                return True, filled_size
            if state in ("cancelled", "rejected"):
                return False, 0
            time.sleep(FILL_POLL_INTERVAL)
        return False, 0

    def place_order(self, product_id: int, side: str, size: int,
                    order_type: str = "market_order",
                    limit_price: Optional[float] = None) -> dict:
        if side not in ("buy", "sell"): return {"error": "invalid_side"}
        if size < 1:                    return {"error": "invalid_size"}
        body: Dict = {"product_id": product_id, "size": size,
                      "side": side, "order_type": order_type}
        if limit_price and order_type == "limit_order":
            body["limit_price"] = str(limit_price)
        result = self.request_handler.request(
            "POST", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def place_take_profit_only(self, product_id: int, tp_price: float, symbol: str = "") -> dict:
        if tp_price <= 0:
            return {"error": "invalid_tp_price"}
        tick_size  = self.get_tick_size(symbol) if symbol else 0.01
        rounded_tp = round_to_tick(tp_price, tick_size)
        body: Dict = {
            "product_id": product_id,
            "take_profit_order": {
                "order_type":  "limit_order",
                "limit_price": str(rounded_tp),
            },
            "bracket_take_profit_trigger_method": "last_traded_price",
        }
        _log("info", "BRACKET-TP",
             f"Placing bracket TP only: pid={product_id} tp={smart_fmt(rounded_tp)}")
        result = self.request_handler.request(
            "POST", "/v2/orders/bracket", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def cancel_bracket_tp(self, product_id: int) -> bool:
        try:
            result = self.request_handler.request(
                "DELETE", "/v2/orders/bracket",
                endpoint_type="private",
                body={"product_id": product_id},
            )
            if result and "error" not in result:
                _log("info", "BRACKET-CANCEL",
                     f"Bracket TP cancelled for product_id={product_id}")
                return True
        except Exception as e:
            _log("warning", "BRACKET-CANCEL", f"Failed to cancel bracket TP: {e}")
        return False

    def cancel_order(self, order_id: int, product_id: int) -> dict:
        body   = {"id": order_id, "product_id": product_id}
        result = self.request_handler.request(
            "DELETE", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def get_top_symbols(self, product_map: Dict[str, int], mode: str = "volatile",
                        limit: int = 5, perp_only: bool = True) -> List[str]:
        result  = self.request_handler.request("GET", "/v2/tickers", endpoint_type="public")
        tickers = result.get("result", []) if result else []
        ranked: List[Tuple[float, str]] = []
        for t in tickers:
            sym = t.get("symbol", "")
            if perp_only and "_PERP" not in sym: continue
            if sym not in product_map:           continue
            try:
                score = (abs(float(t.get("change", 0) or 0)) if mode == "volatile"
                         else float(t.get("volume", 0) or 0))
                ranked.append((score, sym))
            except (TypeError, ValueError): continue
        ranked.sort(reverse=True)
        return [s for _, s in ranked[:limit]]

    def get_candles_with_retry(self, symbol: str, resolution: str = "1h",
                                limit: int = CANDLE_LIMIT) -> List[dict]:
        candles = self.get_candles(symbol, resolution, limit, shift_candles=0)
        if not candles:
            for shift in range(1, MAX_RETRIES + 1):
                time.sleep(RETRY_DELAYS[shift - 1])
                candles = self.get_candles(symbol, resolution, limit, shift_candles=shift)
                if candles: break
        return candles

    def get_candles(self, symbol: str, resolution: str = "1h",
                    limit: int = CANDLE_LIMIT, shift_candles: int = 0) -> List[dict]:
        candle_symbol     = to_candle_symbol(symbol)
        tf_entry          = next(
            (tf for tf in TIMEFRAME_MAP.values()
             if tf["api_resolution"] == resolution or tf["resolution"] == resolution),
            TIMEFRAME_MAP["1h"],
        )
        timeframe_minutes = tf_entry["secs"] // 60
        api_resolution    = tf_entry["api_resolution"]
        safe_limit        = min(limit, 500)
        try:
            start, end = get_time_range_with_retry_shift(
                safe_limit, timeframe_minutes, shift_candles
            )
        except ValueError as exc:
            _log("error", "CANDLES", f"Timestamp validation failed: {exc}")
            return []
        params = {
            "resolution": api_resolution, "symbol": candle_symbol,
            "start": start, "end": end,
        }
        result = self.request_handler.request(
            "GET", "/v2/history/candles", endpoint_type="public", params=params
        )
        if not result: return []
        raw_result = result.get("result", [])
        if isinstance(raw_result, dict):
            for key in ("candles", "data", "ohlcv"):
                if key in raw_result:
                    raw_result = raw_result[key]
                    break
        if not isinstance(raw_result, list) or not raw_result: return []
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
        return bool(result and "result" in result)

    def close_position(self, product_id: int, size: int, side: str = "buy") -> dict:
        return self.place_order(
            product_id=product_id, side=side, size=size, order_type="market_order"
        )

    def get_position_realized_pnl(self, product_id: int) -> float:
        try:
            result = self.request_handler.request(
                "GET", "/v2/positions", endpoint_type="private"
            )
            if result and "result" in result:
                for pos in result["result"]:
                    if pos.get("product_id") == product_id:
                        realized_pnl = float(pos.get("realized_pnl", 0))
                        _log("info", "PNL",
                             f"Fetched realized PnL for product_id={product_id}: ${realized_pnl:.2f}")
                        return realized_pnl
                _log("warning", "PNL", f"No position found for product_id={product_id}")
        except Exception as e:
            _log("error", "PNL", f"Error fetching realized PnL: {e}")
        return 0.0

    def get_order_realized_pnl(self, order_id: int) -> float:
        try:
            result = self.request_handler.request(
                "GET", f"/v2/orders/{order_id}", endpoint_type="private"
            )
            if result and "result" in result:
                order        = result["result"]
                realized_pnl = float(order.get("realized_pnl", 0))
                _log("info", "PNL",
                     f"Fetched realized PnL for order_id={order_id}: ${realized_pnl:.2f}")
                return realized_pnl
        except Exception as e:
            _log("error", "PNL", f"Error fetching order realized PnL: {e}")
        return 0.0


# ================================================================
#  19. POSITION SIZER
# ================================================================

def compute_position_size(entry_price: float, stop_loss_price: float,
                           account_balance: float, risk_pct: float,
                           leverage: int) -> Tuple[int, dict]:
    risk_amount    = account_balance * risk_pct
    stop_distance  = abs(entry_price - stop_loss_price)
    if stop_distance <= 0:
        return 0, {}
    risk_size      = risk_amount / stop_distance
    max_by_margin  = (account_balance * leverage) / entry_price
    final_size_raw = min(risk_size, max_by_margin)
    final_size     = max(1, int(final_size_raw))
    margin_used    = (final_size * entry_price) / leverage
    max_loss_est   = final_size * stop_distance
    diag = {
        "account_balance":  round(account_balance, 2),
        "risk_pct":         round(risk_pct * 100, 2),
        "risk_amount":      round(risk_amount, 2),
        "entry_price":      entry_price,
        "stop_loss_price":  stop_loss_price,
        "stop_distance":    stop_distance,
        "risk_size_raw":    round(risk_size, 6),
        "max_by_margin":    round(max_by_margin, 6),
        "final_size":       final_size,
        "margin_used":      round(margin_used, 2),
        "max_loss_est":     round(max_loss_est, 2),
        "leverage":         leverage,
    }
    return final_size, diag


def compute_take_profit(entry_price: float, stop_loss_price: float,
                         direction: str = "SHORT") -> float:
    stop_distance   = abs(entry_price - stop_loss_price)
    raw_tp_distance = stop_distance * TP_RR_RATIO
    max_tp_distance = entry_price * TP_MAX_PCT
    tp_distance     = min(raw_tp_distance, max_tp_distance)
    if direction == "SHORT":
        tp = entry_price - tp_distance
    else:
        tp = entry_price + tp_distance
    return tp


# ================================================================
#  20. DAILY LOSS TRACKER
# ================================================================

class DailyLossTracker:
    def __init__(self, trading_capital: float, limit_pct: float,
                 notifier: Optional[GmailNotifier] = None):
        self.trading_capital = trading_capital
        self.limit_pct       = limit_pct
        self.daily_loss_usd  = 0.0
        self._day            = datetime.now(timezone.utc).date()
        self._lock           = threading.Lock()
        self.notifier        = notifier

    def _check_day_rollover(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._day:
            _log("info", "DAILY-LOSS", f"New day {today} — resetting daily loss counter")
            self.daily_loss_usd = 0.0
            self._day           = today

    def update_with_realized_pnl(self, realized_pnl: float) -> None:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            if realized_pnl < 0:
                loss_amount = abs(realized_pnl)
                self.daily_loss_usd += loss_amount
                _log("warning", "DAILY-LOSS",
                     f"Realized loss: ${loss_amount:.2f} | "
                     f"Daily loss total: ${self.daily_loss_usd:.2f} / ${limit:.2f}")
            else:
                profit_amount = realized_pnl
                self.daily_loss_usd = max(0.0, self.daily_loss_usd - profit_amount)
                _log("info", "DAILY-LOSS",
                     f"Realized profit: ${profit_amount:.2f} | "
                     f"Daily loss total reduced to: ${self.daily_loss_usd:.2f} / ${limit:.2f}")
            if self.notifier and self.daily_loss_usd >= limit * 0.8:
                self.notifier.send_daily_loss_warning(self.daily_loss_usd, limit)
            if self.notifier and self.daily_loss_usd >= limit:
                self.notifier.send_daily_limit_hit(self.daily_loss_usd, limit)

    def is_limit_reached(self) -> bool:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            if self.daily_loss_usd >= limit:
                _log("error", "DAILY-LOSS",
                     f"DAILY LOSS LIMIT REACHED: ${self.daily_loss_usd:.2f} >= ${limit:.2f}")
                return True
            return False

    def status(self) -> str:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            pct   = (self.daily_loss_usd / limit * 100) if limit > 0 else 0
            return f"Daily loss: ${self.daily_loss_usd:.2f} / ${limit:.2f} ({pct:.1f}%)"


# ================================================================
#  21. TRADING BOT  (v12.2 — Added Range Break & Small→Large Body)
# ================================================================

class TradingBot:
    """
    v12.2: Added Range Break strategy (with close-based confirmation) and
    Small Body → Large Body strategy. Updated Doji strategy to use close
    vs doji low/high. Both new strategies have NO RSI filter.
    Small Body threshold updated to 25%.
    """

    def __init__(self, config: dict, notifier: Optional[GmailNotifier] = None):
        self.config          = config
        self.notifier        = notifier
        self.paper           = config["paper_mode"]
        self.leverage        = config["leverage"]
        self.max_trades      = config.get("max_concurrent_trades", 2)
        self.api_key         = config.get("api_key",    "")
        self.api_secret      = config.get("api_secret", "")
        self.trading_capital = float(config.get("trading_capital", 0.0))
        self.risk_pct        = config["risk_pct"] / 100.0
        self.daily_loss_limit_pct = config.get("daily_loss_limit_pct", DAILY_LOSS_LIMIT_PCT)
        self.enable_short    = config.get("enable_short", True)
        self.enable_long     = config.get("enable_long", True)
        
        # Get Harami tolerance from config, fallback to default
        self.harami_tolerance = config.get("harami_tolerance", HARAMI_BODY_TOLERANCE)

        self._tf            = TimeframeSafe(config.get("timeframe", "1h"))
        self.timeframe      = self._tf.key
        self.resolution     = self._tf.resolution
        self.api_resolution = self._tf.api_resolution
        self.ws_channel     = self._tf.ws_channel

        self.rest = DeltaREST(self.api_key, self.api_secret)

        self.symbols:      List[str]        = []
        self.product_map:  Dict[str, int]   = {}
        self.candle_store: Dict[str, deque] = {}

        self._trade_lock    = threading.Lock()
        self.active_trades: Dict[str, dict] = {}

        self.signals:   List[dict] = []
        self.sl_events: List[dict] = []
        self.tp_events: List[dict] = []

        self.daily_loss_tracker = DailyLossTracker(
            trading_capital=self.trading_capital,
            limit_pct=self.daily_loss_limit_pct,
            notifier=notifier,
        )

        self.running     = False
        self.ws_manager: Optional[DeltaWebSocket] = None

        self.on_signal_callback = None
        self.on_trade_callback  = None
        self.on_log_callback    = None

    def _log(self, level: str, tag: str, msg: str) -> None:
        _log(level, tag, msg)
        if self.on_log_callback:
            try: self.on_log_callback(f"[{tag}] {msg}", level)
            except Exception: pass

    # ─── PnL helpers ───────────────────────────────────────────

    def _get_realized_pnl_for_trade(self, trade: dict,
                                     max_retries: int = 5,
                                     retry_delay: float = 1.0) -> float:
        if self.paper:
            exit_price = trade.get("exit_price")
            entry      = trade.get("entry")
            size       = trade.get("size", 0)
            direction  = trade.get("direction", "SHORT")
            if exit_price and entry and size > 0:
                return (entry - exit_price) * size if direction == "SHORT" \
                       else (exit_price - entry) * size
            return 0.0

        order_id   = trade.get("order_id")
        product_id = trade.get("product_id")

        if order_id:
            for attempt in range(1, max_retries + 1):
                pnl = self.rest.get_order_realized_pnl(order_id)
                if pnl != 0:
                    _log("info", "PNL",
                         f"Retrieved realized PnL from order_id {order_id} (attempt {attempt}): ${pnl:.2f}")
                    return pnl
                if attempt < max_retries:
                    _log("warning", "PNL",
                         f"Order_id {order_id} returned PnL=0 (attempt {attempt}/{max_retries}), "
                         f"retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
            _log("warning", "PNL",
                 f"Could not fetch realized PnL from order_id {order_id} after {max_retries} attempts")

        if product_id:
            for attempt in range(1, max_retries + 1):
                pnl = self.rest.get_position_realized_pnl(product_id)
                if pnl != 0:
                    _log("info", "PNL",
                         f"Retrieved realized PnL from product_id {product_id} "
                         f"(fallback, attempt {attempt}): ${pnl:.2f}")
                    return pnl
                if attempt < max_retries:
                    _log("warning", "PNL",
                         f"Product_id {product_id} returned PnL=0 (attempt {attempt}/{max_retries}), "
                         f"retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
            _log("warning", "PNL",
                 f"Could not fetch realized PnL from product_id {product_id} after {max_retries} attempts")

        _log("error", "PNL",
             f"Could not fetch realized PnL for trade: order_id={order_id}, product_id={product_id}")
        return 0.0

    # ─── Trade close helper ────────────────────────────────────

    def _close_trade(self, symbol: str, trade: dict, reason: str,
                     exit_price: Optional[float] = None) -> None:
        entry     = trade["entry"]
        direction = trade.get("direction", "SHORT")

        _log("info", "TRADE-CLOSE",
             f"Closing trade: {symbol} {direction} | Reason: {reason}")

        if not self.paper:
            pid  = trade.get("product_id")
            size = trade.get("size", 1)
            if pid and size > 0:
                close_side = "buy" if direction == "SHORT" else "sell"
                self.rest.close_position(pid, size, side=close_side)
                _log("info", "TRADE-CLOSE",
                     f"Closed position on Delta: product_id={pid}, size={size}, side={close_side}")
                time.sleep(1)

        realized_pnl = self._get_realized_pnl_for_trade(trade)

        trade["close_reason"] = reason
        trade["close_time"]   = datetime.now(timezone.utc).isoformat()
        trade["realized_pnl"] = realized_pnl

        if reason in ("TAKE_PROFIT", "ST_EXIT"):
            tp = exit_price or trade.get("take_profit")
            trade["exit_price"] = tp
            self.tp_events.append({
                "time":         datetime.now(timezone.utc).isoformat(),
                "symbol":       symbol, "entry": entry,
                "take_profit":  tp,     "direction": direction,
                "realized_pnl": realized_pnl, "reason": reason,
            })
        else:  # STOP_LOSS
            sl = trade["stop_loss"]
            trade["exit_price"] = sl
            self.sl_events.append({
                "time":         datetime.now(timezone.utc).isoformat(),
                "symbol":       symbol, "entry": entry,
                "stop_loss":    sl,     "direction": direction,
                "realized_pnl": realized_pnl,
            })

        self.daily_loss_tracker.update_with_realized_pnl(realized_pnl)

        limit = self.trading_capital * self.daily_loss_limit_pct
        _log("info", "TRADE-CLOSE",
             f"Trade Closed: {symbol} {direction} | Reason: {reason} | "
             f"Realized PnL: ${realized_pnl:.2f} | "
             f"Daily Loss Total: ${self.daily_loss_tracker.daily_loss_usd:.2f} | "
             f"Daily Loss Limit: ${limit:.2f}")

        if self.notifier:
            if reason == "ST_EXIT":
                ep = trade.get("exit_price", entry)
                self.notifier.send_supertrend_exit(
                    symbol=symbol, direction=direction, entry=entry,
                    exit_price=ep if ep else entry,
                    realized_pnl=realized_pnl,
                    timeframe=self.timeframe,
                    mode="PAPER" if self.paper else "LIVE",
                )
            else:
                trade_copy = trade.copy()
                trade_copy["symbol"] = symbol
                self.notifier.send_trade_closed(trade_copy, reason, abs(realized_pnl))

        self._cleanup_trade(symbol)

    def _cleanup_trade(self, symbol: str) -> None:
        with self._trade_lock:
            self.active_trades.pop(symbol, None)
        self._log("info", "CLEANUP", f"Trade record removed for {symbol}")

    # ─── SuperTrend evaluation on closed candle ────────────────

    def _check_supertrend_conditions(self, symbol: str, closed_candles: List[dict]) -> None:
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade:
            return

        direction = trade.get("direction", "SHORT")
        entry     = trade.get("entry", 0)
        st_mode   = trade.get("st_mode", False)

        if len(closed_candles) < ST2_LENGTH + 2:
            return

        bearish_now = both_supertrends_bearish(closed_candles)
        bullish_now = both_supertrends_bullish(closed_candles)

        if not st_mode:
            confirmed = (
                (direction == "SHORT" and bearish_now) or
                (direction == "LONG"  and bullish_now)
            )
            if confirmed:
                st1_val, st2_val = get_supertrend_values(closed_candles)
                trade["st_mode"]         = True
                trade["st_confirmed_at"] = datetime.now(timezone.utc).isoformat()

                if not self.paper:
                    pid = trade.get("product_id")
                    if pid:
                        self.rest.cancel_bracket_tp(pid)

                trend_label = "📉 STRONG DOWN TREND" if direction == "SHORT" else "📈 STRONG UP TREND"
                print()
                print(f"  [SUPERTREND CONFIRMED] {symbol} {trend_label}")
                print(f"    Both ST(14,2) + ST(21,1) {'RED' if direction=='SHORT' else 'GREEN'}")
                print(f"    Entry        : {smart_fmt(entry)}")
                print(f"    ST(14,2) val : {smart_fmt(st1_val) if st1_val else 'N/A'}")
                print(f"    ST(21,1) val : {smart_fmt(st2_val) if st2_val else 'N/A'}")
                print(f"    TP CHANGED → Will exit when BOTH SuperTrends reverse direction")
                print()

                _log("info", "ST-CONFIRM",
                     f"[{symbol}] {direction} trade — Both SuperTrends confirmed. "
                     f"Switched to ST-mode exit.")

                if self.notifier:
                    self.notifier.send_supertrend_strong(
                        symbol=symbol, direction=direction, entry=entry,
                        new_tp=0.0,
                        st1=st1_val or 0.0, st2=st2_val or 0.0,
                        timeframe=self.timeframe,
                        mode="PAPER" if self.paper else "LIVE",
                    )

        else:
            reversed_trend = (
                (direction == "SHORT" and bullish_now) or
                (direction == "LONG"  and bearish_now)
            )
            if reversed_trend:
                _log("info", "ST-EXIT",
                     f"[{symbol}] {direction} — Both SuperTrends REVERSED. "
                     "Closing trade via ST exit.")
                print()
                print(f"  [ST REVERSAL EXIT] {symbol} {direction}")
                flip_label = "both GREEN" if direction == "SHORT" else "both RED"
                print(f"    SuperTrends flipped {flip_label} — closing position")
                print()
                self._close_trade(symbol, trade, "ST_EXIT")

    # ─── Start / Stop ──────────────────────────────────────────

    def start(self) -> None:
        self.running = True
        self._print_banner()
        warm_up_connection()

        if not self.paper:
            profile = self.rest.verify_account()
            if profile is None:
                self._log("error", "STARTUP", "Authentication failed. Aborting.")
                self.running = False
                return

        self._log("info", "STARTUP", "Loading product catalogue...")
        raw_map = self.rest.fetch_product_map()
        if not raw_map:
            self._log("error", "STARTUP", "Product catalogue empty. Aborting.")
            self.running = False
            return
        self.product_map = raw_map

        raw_syms = self.config.get("symbols", [])
        if not raw_syms:
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
            self.candle_store[sym] = deque(maxlen=500)

        if not self.paper:
            for sym in self.symbols:
                pid = self.product_map.get(sym)
                if pid: self.rest.set_leverage(pid, self.leverage)

        self._print_startup_summary()

        if self.notifier:
            self.notifier.send_startup_report(self.config, self.symbols, self.harami_tolerance)

        self._fetch_all_historical()
        self._start_ws()

    def stop(self) -> None:
        self.running = False
        if self.ws_manager: self.ws_manager.stop()
        self._log("info", "BOT", "Bot stopped.")

    # ─── Historical load ───────────────────────────────────────

    def _fetch_all_historical(self) -> None:
        self._log("info", "CANDLES",
                  f"Loading {CANDLE_LIMIT} candles x {self.timeframe} "
                  f"for {len(self.symbols)} symbol(s)")
        for sym in self.symbols:
            candles = self.rest.get_candles_with_retry(sym, self.api_resolution, CANDLE_LIMIT)
            if candles:
                for c in candles:
                    self.candle_store[sym].append(c)
                self._log("info", "CANDLES",
                          f"  [OK] {sym}: {len(candles)} candles loaded | "
                          f"last_close={smart_fmt(candles[-1]['close'])}")
            else:
                self._log("warning", "CANDLES", f"  [WARN] {sym}: 0 candles after retries.")

    # ─── WebSocket ─────────────────────────────────────────────

    def _start_ws(self) -> None:
        self.ws_manager = DeltaWebSocket()
        self.ws_manager.set_candle_callback(self._on_ws_candle)
        self.ws_manager.set_connected_callback(self._on_ws_connected)
        self.ws_manager.set_disconnected_callback(self._on_ws_disconnected)
        self.ws_manager.set_error_callback(self._on_ws_error)
        self.ws_manager.set_reconnect_callback(self._on_ws_reconnect)
        ws_symbols = [to_ws_symbol(sym) for sym in self.symbols]
        self.ws_manager.subscribe(self.timeframe, ws_symbols)
        self.ws_manager.start()

    def _on_ws_candle(self, symbol: str, candle: dict) -> None:
        if symbol in self.candle_store:
            self.process_candle(symbol, candle)
            return
        for known in self.candle_store:
            if to_ws_symbol(known) == to_ws_symbol(symbol):
                self.process_candle(known, candle)
                return

    def _on_ws_connected(self)    -> None: self._log("info",    "WS", "Connected")
    def _on_ws_disconnected(self) -> None: self._log("warning", "WS", "Disconnected (max retries)")
    def _on_ws_error(self, err)   -> None: self._log("error",   "WS", f"Error: {err}")

    def _on_ws_reconnect(self) -> None:
        for sym in self.symbols: self._backfill_symbol(sym)

    def _backfill_symbol(self, symbol: str) -> None:
        store = self.candle_store.get(symbol)
        if store is None: return
        fresh = self.rest.get_candles_with_retry(symbol, self.api_resolution, CANDLE_LIMIT)
        if not fresh: return
        existing_ts = {c["time"] for c in store}
        for c in sorted(fresh, key=lambda x: x["time"]):
            if c["time"] not in existing_ts:
                store.append(c)
                existing_ts.add(c["time"])

    # ─── Candle processing ─────────────────────────────────────

    def process_candle(self, symbol: str, candle: dict) -> None:
        store = self.candle_store.get(symbol)
        if store is None: return
        if not validate_candle(candle, symbol): return

        if store and store[-1]["time"] == candle["time"]:
            store[-1] = candle
            if symbol in self.active_trades:
                trade = self.active_trades.get(symbol)
                if trade and not trade.get("st_mode", False):
                    self._check_take_profit(symbol, candle)
            return

        if store:
            closed_candle  = store[-1]
            closed_candles = list(store)

            self._log("info", "CANDLE-CLOSED",
                      f"{symbol} [{self.timeframe}] "
                      f"O={smart_fmt(closed_candle['open'])} "
                      f"H={smart_fmt(closed_candle['high'])} "
                      f"L={smart_fmt(closed_candle['low'])} "
                      f"C={smart_fmt(closed_candle['close'])}")

            if symbol in self.active_trades:
                trade = self.active_trades.get(symbol)
                if trade and not trade.get("_reserved"):
                    self._check_supertrend_conditions(symbol, closed_candles)

                    trade = self.active_trades.get(symbol)
                    if trade and not trade.get("_reserved"):
                        st_mode = trade.get("st_mode", False)
                        if not st_mode:
                            self._check_stop_loss_on_close(symbol, closed_candle)

            if symbol not in self.active_trades:
                if self.daily_loss_tracker.is_limit_reached():
                    self._log("warning", "DAILY-LOSS",
                              f"[{symbol}] Skipping signal check — daily loss limit hit")
                else:
                    candle_list = list(store) + [candle]

                    # Check SHORT signals
                    if self.enable_short:
                        # First check no-RSI strategies (Range Break, Small→Large Body)
                        triggered, signal_candle, strategy_name, rsi_value = check_short_signal_no_rsi(
                            candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                        )
                        if triggered and signal_candle is not None:
                            self._on_signal(symbol, signal_candle, strategy_name,
                                            rsi_value, "SHORT", no_rsi=True)
                        else:
                            # If no-RSI didn't trigger, check regular strategies with RSI
                            triggered, signal_candle, strategy_name, rsi_value = check_short_signal(
                                candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                            )
                            if triggered and signal_candle is not None:
                                self._on_signal(symbol, signal_candle, strategy_name,
                                                rsi_value, "SHORT", no_rsi=False)

                    # Check LONG signals
                    if self.enable_long and symbol not in self.active_trades:
                        # First check no-RSI strategies (Range Break, Small→Large Body)
                        triggered, signal_candle, strategy_name, rsi_value = check_long_signal_no_rsi(
                            candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                        )
                        if triggered and signal_candle is not None:
                            self._on_signal(symbol, signal_candle, strategy_name,
                                            rsi_value, "LONG", no_rsi=True)
                        else:
                            # If no-RSI didn't trigger, check regular strategies with RSI
                            triggered, signal_candle, strategy_name, rsi_value = check_long_signal(
                                candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                            )
                            if triggered and signal_candle is not None:
                                self._on_signal(symbol, signal_candle, strategy_name,
                                                rsi_value, "LONG", no_rsi=False)

        store.append(candle)

    def _check_take_profit(self, symbol: str, candle: dict) -> None:
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade: return
        if trade.get("st_mode", False): return

        tp        = trade.get("take_profit")
        direction = trade.get("direction", "SHORT")
        entry     = trade["entry"]
        if tp is None: return

        if direction == "SHORT" and candle["low"] <= tp:
            self._log("info", "TP-HIT",
                      f"TAKE PROFIT HIT (intra-candle) | {symbol} | "
                      f"entry={smart_fmt(entry)} tp={smart_fmt(tp)} low={smart_fmt(candle['low'])}")
            self._close_trade(symbol, trade, "TAKE_PROFIT", exit_price=tp)

        elif direction == "LONG" and candle["high"] >= tp:
            self._log("info", "TP-HIT",
                      f"TAKE PROFIT HIT (intra-candle) | {symbol} | "
                      f"entry={smart_fmt(entry)} tp={smart_fmt(tp)} high={smart_fmt(candle['high'])}")
            self._close_trade(symbol, trade, "TAKE_PROFIT", exit_price=tp)

    def _check_stop_loss_on_close(self, symbol: str, closed_candle: dict) -> None:
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade: return
        if trade.get("st_mode", False): return

        sl          = trade["stop_loss"]
        direction   = trade.get("direction", "SHORT")
        entry       = trade["entry"]
        close_price = closed_candle["close"]

        if direction == "SHORT" and close_price >= sl:
            self._log("info", "SL-CLOSE",
                      f"STOP LOSS HIT (candle close) | {symbol} SHORT | "
                      f"close={smart_fmt(close_price)} >= sl={smart_fmt(sl)}")
            self._close_trade(symbol, trade, "STOP_LOSS")

        elif direction == "LONG" and close_price <= sl:
            self._log("info", "SL-CLOSE",
                      f"STOP LOSS HIT (candle close) | {symbol} LONG | "
                      f"close={smart_fmt(close_price)} <= sl={smart_fmt(sl)}")
            self._close_trade(symbol, trade, "STOP_LOSS")

    # ─── Signal handler ────────────────────────────────────────

    def _on_signal(self, symbol: str, signal_candle: dict, strategy_name: str,
                   rsi_value: Optional[float], direction: str = "SHORT", 
                   no_rsi: bool = False) -> None:
        entry = signal_candle["close"]

        if direction == "SHORT":
            if strategy_name == "BEARISH_DOJI" and "doji_low" in signal_candle:
                sl = signal_candle["doji_low"]
            elif strategy_name in ("STRATEGY_1_SHORT", "STRATEGY_3_SHORT",
                                    "BEARISH_HARAMI", "RANGE_BREAK_SHORT",
                                    "SMALL_TO_LARGE_SHORT") and "pattern_high" in signal_candle:
                sl = signal_candle["pattern_high"]
            else:
                store = self.candle_store.get(symbol)
                sl    = list(store)[-2]["high"] if store and len(store) >= 2 \
                        else signal_candle["high"]
        else:
            if strategy_name == "BULLISH_DOJI" and "doji_high" in signal_candle:
                sl = signal_candle["doji_high"]
            elif strategy_name in ("STRATEGY_1_LONG", "BULLISH_ENGULFING",
                                    "BULLISH_HARAMI", "RANGE_BREAK_LONG",
                                    "SMALL_TO_LARGE_LONG") and "pattern_low" in signal_candle:
                sl = signal_candle["pattern_low"]
            else:
                store = self.candle_store.get(symbol)
                sl    = list(store)[-2]["low"] if store and len(store) >= 2 \
                        else signal_candle["low"]

        if abs(sl - entry) == 0:
            self._log("warning", "SIGNAL", f"risk_per_unit=0 for {symbol} — skip")
            return

        tp = compute_take_profit(entry, sl, direction)

        with self._trade_lock:
            if len(self.active_trades) >= self.max_trades: return
            if symbol in self.active_trades:               return

            risk_usd = round(self.trading_capital * self.risk_pct, 2)
            signal = {
                "time":            datetime.now(timezone.utc).isoformat(),
                "symbol":          symbol,
                "direction":       direction,
                "entry":           entry,
                "stop_loss":       sl,
                "take_profit":     tp,
                "timeframe":       self.timeframe,
                "mode":            "PAPER" if self.paper else "LIVE",
                "executed":        False,
                "product_id":      self.product_map.get(symbol),
                "risk_usd":        risk_usd,
                "trading_capital": self.trading_capital,
                "strategy":        strategy_name,
                "rsi":             rsi_value,
                "no_rsi":          no_rsi,
                "harami_tolerance": self.harami_tolerance if strategy_name in ("BEARISH_HARAMI", "BULLISH_HARAMI") else None,
            }
            self.signals.append(signal)

            if self.paper:
                self.active_trades[symbol] = {
                    "entry":      entry, "stop_loss": sl, "take_profit": tp,
                    "direction":  direction, "size": 0,
                    "product_id": self.product_map.get(symbol),
                    "open_time":  datetime.now(timezone.utc).isoformat(),
                    "strategy":   strategy_name, "rsi": rsi_value,
                    "st_mode":    False,
                    "no_rsi":     no_rsi,
                }

        rsi_str    = "N/A (no RSI)" if no_rsi else (f"{rsi_value:.2f}" if rsi_value is not None else "N/A")
        stop_dist  = abs(entry - sl)
        rr_actual  = abs(entry - tp) / stop_dist if stop_dist > 0 else 0
        direction_arrow = "↓ SHORT" if direction == "SHORT" else "↑ LONG"

        print()
        print(f"  [SIGNAL] {symbol}  {direction_arrow}  [{self.timeframe}] — {strategy_name}")
        print(f"           Entry       : {smart_fmt(entry)}")
        print(f"           Stop Loss   : {smart_fmt(sl)}  "
              f"(distance={smart_fmt(stop_dist)}) [triggers on CANDLE CLOSE]")
        print(f"           Take Profit : {smart_fmt(tp)}  "
              f"(R:R = 1:{rr_actual:.2f}) [triggers on PRICE TOUCH]")
        print(f"           Risk        : ${risk_usd:,.2f}  ({self.config['risk_pct']}%)")
        print(f"           RSI(14)     : {rsi_str}")
        if no_rsi:
            print(f"           RSI Filter  : DISABLED")
        print(f"           Mode        : {signal['mode']}")
        print(f"           SuperTrend  : Monitoring ST(14,2) + ST(21,1) post-entry")
        if strategy_name in ("BEARISH_HARAMI", "BULLISH_HARAMI"):
            print(f"           Harami Tol : {self.harami_tolerance*100:.2f}%")
        if strategy_name in ("SMALL_TO_LARGE_SHORT", "SMALL_TO_LARGE_LONG"):
            avg_body = signal_candle.get("avg_body", 0)
            if avg_body > 0:
                print(f"           Avg Body    : {smart_fmt(avg_body)}")
                print(f"           Body Ratio  : {candle_body(signal_candle) / avg_body:.1f}x")
        print()

        if self.notifier:
            self.notifier.send_signal(signal)

        if not self.paper:
            self._execute_trade(symbol, signal)

        if self.on_signal_callback:
            try: self.on_signal_callback(signal)
            except Exception: pass

    # ─── Live trade execution ──────────────────────────────────

    def _execute_trade(self, symbol: str, signal: dict) -> None:
        try:
            with self._trade_lock:
                if symbol in self.active_trades: return
                self.active_trades[symbol] = {"_reserved": True}

            account_balance = self.rest.get_usd_balance() or 0.0
            capital         = self.trading_capital
            if capital <= 0:              self._cleanup_trade(symbol); return
            if account_balance < capital: capital = account_balance
            if capital <= 0:              self._cleanup_trade(symbol); return

            entry     = signal["entry"]
            sl        = signal["stop_loss"]
            tp        = signal["take_profit"]
            direction = signal["direction"]
            pid       = self.product_map.get(symbol)
            if not pid:                   self._cleanup_trade(symbol); return

            position_size, _ = compute_position_size(
                entry_price=entry, stop_loss_price=sl,
                account_balance=capital, risk_pct=self.risk_pct, leverage=self.leverage,
            )
            if position_size < 1:         self._cleanup_trade(symbol); return

            side         = "sell" if direction == "SHORT" else "buy"
            entry_result = self.rest.place_order(
                product_id=pid, side=side, size=position_size, order_type="market_order",
            )

            if not entry_result or "error" in entry_result:
                self._cleanup_trade(symbol); return

            order_result = entry_result.get("result", {})
            order_id     = order_result.get("id")
            order_state  = order_result.get("state", "")

            if order_state == "rejected" or not order_id:
                self._cleanup_trade(symbol); return

            filled, actual_filled_size = self.rest.wait_for_fill(order_id, symbol)

            if not filled or actual_filled_size < 1:
                self._log("error", "TRADE",
                          f"Fill not confirmed for {symbol} — attempting cancel...")
                self.rest.cancel_order(order_id, pid)
                self._log("error", "TRADE",
                          f"IMPORTANT: Check {symbol} position manually. order_id={order_id}")
                self._cleanup_trade(symbol)
                return

            print(f"  [FILLED] {symbol} {direction} | order_id={order_id} | "
                  f"filled={actual_filled_size} contracts")

            bracket_result = self.rest.place_take_profit_only(
                product_id=pid, tp_price=tp, symbol=symbol
            )
            bracket_ok = bracket_result and "error" not in bracket_result

            if bracket_ok:
                print(f"  [TP BRACKET OK] Take profit bracket placed at {smart_fmt(tp)}")
                self._log("info", "BRACKET", f"TP bracket placed for {symbol} at {smart_fmt(tp)}")
            else:
                self._log("warning", "BRACKET",
                          f"TP bracket FAILED for {symbol}: {bracket_result}")

            self._log("info", "LOCAL-SL",
                      f"Stop loss managed locally for {symbol} at {smart_fmt(sl)} (candle close)")

            signal["executed"]      = True
            signal["size"]          = actual_filled_size
            signal["order_id"]      = order_id
            signal["bracket_tp_ok"] = bracket_ok

            trade_record = {
                "entry":         entry, "stop_loss": sl, "take_profit": tp,
                "direction":     direction, "size": actual_filled_size,
                "product_id":    pid, "order_id": order_id,
                "open_time":     datetime.now(timezone.utc).isoformat(),
                "strategy":      signal.get("strategy", "UNKNOWN"),
                "rsi":           signal.get("rsi"),
                "bracket_tp_ok": bracket_ok,
                "st_mode":       False,
                "no_rsi":        signal.get("no_rsi", False),
            }

            with self._trade_lock:
                self.active_trades[symbol] = trade_record

            if self.notifier:
                trade_copy           = trade_record.copy()
                trade_copy["symbol"] = symbol
                self.notifier.send_trade_executed(trade_copy)

            if self.on_trade_callback:
                try: self.on_trade_callback(signal)
                except Exception: pass

        except Exception as exc:
            self._cleanup_trade(symbol)
            self._log("error", "TRADE", f"Execution error for {symbol}: {exc}")

    # ─── UI helpers ────────────────────────────────────────────

    def _print_banner(self) -> None:
        print()
        print("+========================================================+")
        print("|   DELTA EXCHANGE INDIA — TRADING BOT  v12.2           |")
        print("|   ADDED: RANGE BREAK (close-based) & SMALL→LARGE BODY  |")
        print("|   UPDATED: DOJI uses close vs doji low/high           |")
        print("|   UPDATED: Small body threshold → 25%                 |")
        print("|                                                        |")
        print("|   STOP LOSS : Triggers ONLY on CANDLE CLOSE            |")
        print("|   TAKE PROFIT (normal)  : 2:1 R:R on price touch       |")
        print("|   TAKE PROFIT (ST mode) : Both SuperTrends REVERSE     |")
        print("|                                                        |")
        print("|   SuperTrend 1 : Length=14, Factor=2.0  (accurate)     |")
        print("|   SuperTrend 2 : Length=21, Factor=1.0  (trend)        |")
        print("|                                                        |")
        print("|   RSI SHORT filter : RSI(14) > 55                      |")
        print("|   RSI LONG  filter : RSI(14) < 40                      |")
        print("|   RSI LONG  BLOCK   : RSI(14) < 24 (extreme oversold)  |")
        print("|   NO RSI FILTER    : Range Break, Small→Large Body     |")
        print("|   Daily loss limit : based on REALIZED PnL             |")
        print("|   Precision        : auto dp — altcoin micro-prices OK  |")
        print(f"|   Harami Tolerance : {self.harami_tolerance*100:.2f}% body tolerance  |")
        print(f"|   Small→Large Body  : 7 small bodies (≤25%) → 1 large body    |")
        print("+========================================================+")
        print()

    def _print_startup_summary(self) -> None:
        mode_str        = "PAPER (signals only)" if self.paper else "LIVE TRADING"
        risk_usd        = self.trading_capital * self.risk_pct
        daily_limit_usd = self.trading_capital * self.daily_loss_limit_pct
        print("+--------------------------------------------------------+")
        print(f"  Mode              : {mode_str}")
        print(f"  Timeframe         : {self.timeframe}")
        print(f"  Trading capital   : ${self.trading_capital:,.2f} USD")
        print(f"  Risk / trade      : {self.config['risk_pct']}%  =  ~${risk_usd:,.2f} USD")
        print(f"  Take-Profit (def) : {TP_RR_RATIO:.1f}:1 (triggers on PRICE TOUCH)")
        print(f"  Take-Profit (ST)  : Both SuperTrends reverse direction")
        print(f"  Stop Loss         : Triggers on CANDLE CLOSE only (disabled in ST mode)")
        print(f"  Daily loss cap    : {self.daily_loss_limit_pct*100:.0f}%  =  ~${daily_limit_usd:,.2f} USD")
        print(f"  Leverage          : {self.leverage}x")
        print(f"  Max open trades   : {self.max_trades}")
        print(f"  SHORT TRADES      : {'ENABLED' if self.enable_short else 'DISABLED'}")
        print(f"  LONG TRADES       : {'ENABLED' if self.enable_long else 'DISABLED'}")
        print(f"  RSI SHORT filter  : RSI(14) > {RSI_OVERBOUGHT}")
        print(f"  RSI LONG  filter  : RSI(14) < {RSI_OVERSOLD}")
        print(f"  RSI LONG  BLOCK   : RSI(14) < 24 (extreme oversold — no trades)")
        print(f"  NO RSI FILTER     : Range Break, Small→Large Body")
        print(f"  Doji confirmation : close vs doji low/high")
        print(f"  Range Break conf  : close vs break candle close")
        print(f"  SuperTrend 1      : Length={ST1_LENGTH}, Factor={ST1_FACTOR}")
        print(f"  SuperTrend 2      : Length={ST2_LENGTH}, Factor={ST2_FACTOR}")
        print(f"  Harami Tolerance  : {self.harami_tolerance*100:.2f}% body tolerance")
        print(f"  Small Body lookback: {SMALL_BODY_LOOKBACK} candles")
        print(f"  Small Body threshold: {SMALL_BODY_THRESHOLD*100:.0f}% of range")
        print(f"  Large Body factor : {LARGE_BODY_MULTIPLIER}x avg")
        print(f"  Price Precision   : auto dp via smart_fmt() — supports micro-price alts")
        print(f"  GMAIL             : {'ENABLED' if self.notifier and self.notifier.enabled else 'DISABLED'}")
        print(f"  PnL Fetch         : order_id → product_id (5 retries, 1s delay)")
        print(f"  Symbols ({len(self.symbols)}):")
        for sym in self.symbols:
            pid     = self.product_map.get(sym, "???")
            tick_sz = self.rest.get_tick_size(sym)
            print(f"    - {sym:<22} pid={pid}  tick={smart_fmt(tick_sz)}")
        print("+--------------------------------------------------------+")
        print()


# ================================================================
#  22. USER INPUT HELPERS
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
    return raw if raw in TIMEFRAME_MAP else "1h"


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
        return False, api_key, api_secret
    return True, "", ""


def ask_trade_directions() -> Tuple[bool, bool]:
    _divider("TRADE DIRECTIONS")
    print("  Select which types of trades the bot should execute:")
    print()
    enable_short = input("  Enable SHORT trades? (Y/n) : ").strip().lower() != 'n'
    enable_long  = input("  Enable LONG trades? (Y/n) : ").strip().lower()  != 'n'
    if not enable_short and not enable_long:
        print("\n  ⚠️  WARNING: Both short and long trades are disabled!")
        print("  The bot will monitor markets but will NOT execute any trades.")
        proceed = input("  Do you want to continue anyway? (y/N) : ").strip().lower()
        if proceed != 'y':
            print("  Restart and select at least one direction.")
            exit(0)
    print()
    print(f"  Short trades: {'ENABLED ✅' if enable_short else 'DISABLED ❌'}")
    print(f"  Long trades : {'ENABLED ✅' if enable_long  else 'DISABLED ❌'}")
    print()
    return enable_short, enable_long


def ask_gmail_config() -> Optional[GmailNotifier]:
    _divider("GMAIL NOTIFICATIONS")
    print("  Get alerts for signals, trade executions, SuperTrend events, and daily limits.")
    print("  You'll need a Gmail App Password (not your regular password).")
    enable = input("  Enable Gmail notifications? (y/N) : ").strip().lower()
    if enable != 'y':
        print("  Gmail notifications DISABLED")
        return None
    print()
    sender = input("  Your Gmail address (e.g., yourname@gmail.com): ").strip()
    if not sender:
        print("  Invalid email. Notifications disabled.")
        return None
    app_password = input("  Gmail App Password (16 chars, no spaces): ").strip()
    if not app_password or len(app_password) < 10:
        print("  Invalid password. Notifications disabled.")
        return None
    recipients_raw = input("  Recipient emails (comma separated, default = your email): ").strip()
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()] \
                 if recipients_raw else [sender]
    print(f"  Gmail notifications ENABLED for {', '.join(recipients)}")
    return GmailNotifier(
        sender_email=sender,
        gmail_app_password=app_password,
        recipient_emails=recipients,
        enabled=True,
    )


def ask_symbols(product_map: Dict[str, int]) -> List[str]:
    _divider("SYMBOLS")
    print("  Enter symbols (space/comma separated).")
    print("  Press Enter to AUTO-SELECT top 5 by volatility.")
    raw = input("  Symbols : ").strip().upper()
    if not raw: return []
    return [p.strip() for p in raw.replace(",", " ").split() if p.strip()]


def ask_leverage() -> int:
    _divider("LEVERAGE")
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
    raw = input(f"  Trading capital (default = ${account_balance:,.0f}) : ").strip()
    if not raw: return account_balance
    try: capital = float(raw)
    except ValueError: capital = account_balance
    if capital <= 0: return account_balance
    if capital > account_balance:
        print(f"  [WARN] Capping to balance ${account_balance:,.2f}")
        capital = account_balance
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


def ask_daily_loss_limit() -> float:
    _divider("DAILY LOSS LIMIT")
    print("  Daily loss limit stops trading if cumulative REALIZED losses exceed this % of capital.")
    try:
        daily_loss = float(input("  Daily loss limit % (default = 5) : ").strip() or 5)
        daily_loss = max(0.1, min(daily_loss, 50.0))
    except ValueError:
        daily_loss = 5.0
    print(f"  Daily loss limit : {daily_loss}% of trading capital (based on REALIZED PnL)")
    return daily_loss


def ask_harami_tolerance() -> float:
    _divider("HARAMI TOLERANCE")
    print("  Harami tolerance allows the second candle's body to be")
    print("  slightly outside the first candle's body boundaries.")
    print()
    print("  Examples:")
    print("    0.001  = 0.1% tolerance  (very strict - default)")
    print("    0.005  = 0.5% tolerance  (some flexibility)")
    print("    0.01   = 1.0% tolerance  (moderate flexibility)")
    print("    0.05   = 5.0% tolerance  (very flexible)")
    print("    0.10   = 10.0% tolerance (highly flexible)")
    print("    0.20   = 20.0% tolerance (almost any pattern)")
    print()
    try:
        tolerance = float(input("  Harami body tolerance (default = 0.001) : ").strip() or 0.001)
        if tolerance > 0.20:
            print(f"  ⚠️  WARNING: {tolerance*100:.1f}% is very permissive!")
            print("  This may detect patterns that aren't true Harami.")
            proceed = input("  Continue with this value? (y/N) : ").strip().lower()
            if proceed != 'y':
                return ask_harami_tolerance()
        tolerance = max(0.0001, tolerance)
    except ValueError:
        tolerance = 0.001
    print(f"  Harami tolerance : {tolerance*100:.2f}%")
    return tolerance


# ================================================================
#  23. TEST FUNCTION
# ================================================================

def test_gmail():
    print("\n  📧 TESTING GMAIL NOTIFICATIONS")
    print("  " + "=" * 50)
    sender = input("  Your Gmail address: ").strip()
    if not sender:
        print("  ❌ Invalid email. Test cancelled.")
        return
    app_password = input("  Gmail App Password: ").strip()
    if not app_password:
        print("  ❌ Invalid password. Test cancelled.")
        return
    notifier = GmailNotifier(
        sender_email=sender,
        gmail_app_password=app_password,
        recipient_emails=[sender],
        enabled=True,
    )
    print("\n  📤 Sending test signal notification...")
    success = notifier.send_signal({
        "direction": "LONG",
        "symbol":    "BTCUSD_PERP",
        "strategy":  "TEST_SIGNAL",
        "timeframe": "1h",
        "entry":     65000.0,
        "stop_loss": 63000.0,
        "take_profit": 69000.0,
        "rsi":       28.5,
        "mode":      "PAPER",
        "risk_usd":  20.0,
        "trading_capital": 1000.0,
        "time":      datetime.now(timezone.utc).isoformat(),
        "no_rsi":    False,
    })
    if success:
        print("  ✅ Test email sent successfully!")
        print("\n  📤 Sending test SuperTrend confirmation notification...")
        notifier.send_supertrend_strong(
            symbol="BTCUSD_PERP", direction="LONG", entry=65000.0,
            new_tp=0.0, st1=64200.0, st2=63800.0, timeframe="1h", mode="PAPER",
        )
        print("  ✅ SuperTrend test email sent!")
    else:
        print("  ❌ Failed to send email. Check your App Password and settings.")


# ================================================================
#  24. ENTRY POINT
# ================================================================

def main() -> None:
    print()
    print("  +======================================================+")
    print("  |   DELTA EXCHANGE INDIA  —  TRADING BOT  v12.2       |")
    print("  |   STOP LOSS on CANDLE CLOSE | TP on PRICE TOUCH     |")
    print("  |   DUAL SUPERTREND TP EXTENSION                       |")
    print("  |   ST(14,2) + ST(21,1) — confirm & exit              |")
    print("  |   Short + Long  |  RSI(14) filter  |  GMAIL         |")
    print("  |   RSI LONG BLOCK: < 24 (extreme oversold)           |")
    print("  |   NO RSI FILTER: Range Break, Small→Large Body      |")
    print("  |   Small Body Threshold: 25% of range                |")
    print("  |   CONFIGURABLE HARAMI TOLERANCE                     |")
    print("  |   Auto-precision prices: BTC→2dp, altcoin→up to 10dp|")
    print("  +======================================================+")

    _divider("SETUP")
    test_gmail_first = input("  Test Gmail notifications first? (y/N) : ").strip().lower()
    if test_gmail_first == 'y':
        test_gmail()
        print("\n  Continuing with bot setup...")

    timeframe                  = ask_timeframe()
    paper, api_key, api_secret = ask_mode()
    enable_short, enable_long  = ask_trade_directions()
    notifier                   = ask_gmail_config()
    harami_tolerance           = ask_harami_tolerance()

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
    else:
        _divider("PAPER MODE CAPITAL")
        try:
            account_balance = float(
                input("  Notional capital USD (default = 1000) : ").strip() or 1000
            )
        except ValueError:
            account_balance = 1000.0

    trading_capital      = ask_trading_capital(account_balance)
    risk_pct, max_trades = ask_risk_params()
    daily_loss_limit_pct = ask_daily_loss_limit()

    _divider("CONFIRM")
    risk_usd        = trading_capital * risk_pct / 100
    daily_limit_usd = trading_capital * (daily_loss_limit_pct / 100)
    print(f"  Mode              : {'PAPER' if paper else 'LIVE TRADING'}")
    print(f"  Timeframe         : {timeframe}")
    print(f"  Short Trades      : {'ENABLED ✅' if enable_short else 'DISABLED ❌'}")
    print(f"  Long Trades       : {'ENABLED ✅' if enable_long  else 'DISABLED ❌'}")
    print(f"  Symbols           : {raw_symbols if raw_symbols else 'AUTO-SELECT'}")
    print(f"  Leverage          : {leverage}x")
    print(f"  Trading capital   : ${trading_capital:,.2f}")
    print(f"  Risk / trade      : {risk_pct}%  =  ~${risk_usd:,.2f}")
    print(f"  Take-Profit (def) : {TP_RR_RATIO:.1f}:1 R:R (triggers on PRICE TOUCH)")
    print(f"  Take-Profit (ST)  : Both SuperTrends ST(14,2)+ST(21,1) reverse")
    print(f"  Stop Loss         : Triggers on CANDLE CLOSE only")
    print(f"  Daily loss cap    : {daily_loss_limit_pct}%  =  ~${daily_limit_usd:,.2f}")
    print(f"  Max open trades   : {max_trades}")
    print(f"  GMAIL             : {'ENABLED' if notifier and notifier.enabled else 'DISABLED'}")
    print(f"  SuperTrend 1      : Length={ST1_LENGTH}, Factor={ST1_FACTOR}")
    print(f"  SuperTrend 2      : Length={ST2_LENGTH}, Factor={ST2_FACTOR}")
    print(f"  RSI LONG BLOCK    : RSI(14) < 24 (prevents trades in extreme oversold)")
    print(f"  NO RSI FILTER     : Range Break, Small→Large Body")
    print(f"  Doji Confirmation : close vs doji low/high")
    print(f"  Range Break Conf  : close vs break candle close")
    print(f"  Small Body Lookback: {SMALL_BODY_LOOKBACK} candles")
    print(f"  Small Body Threshold: {SMALL_BODY_THRESHOLD*100:.0f}% of range")
    print(f"  Large Body Factor : {LARGE_BODY_MULTIPLIER}x avg body")
    print(f"  Harami Tolerance  : {harami_tolerance*100:.2f}% body tolerance")
    print(f"  Price Precision   : auto dp — altcoin micro-prices supported up to 10 dp")
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
        "daily_loss_limit_pct":  daily_loss_limit_pct / 100.0,
        "enable_short":          enable_short,
        "enable_long":           enable_long,
        "harami_tolerance":      harami_tolerance,
    }

    bot = TradingBot(cfg, notifier=notifier)
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
            st_info   = ""
            no_rsi_info = ""
            for sym, t in bot.active_trades.items():
                if isinstance(t, dict):
                    if t.get("st_mode"):
                        st_info += f"[{sym}:ST-MODE] "
                    if t.get("no_rsi"):
                        no_rsi_info += f"[{sym}:NO-RSI] "
            print(
                f"  [STATUS] Open={open_n}/{max_trades}  "
                f"Signals={len(bot.signals)}  "
                f"TPs={len(bot.tp_events)}  SLs={len(bot.sl_events)}  "
                f"WS={ws_state}  {bot.daily_loss_tracker.status()}  "
                + (f"Trades={open_syms}" if open_syms else "NoOpenTrades")
                + (f"  {st_info}" if st_info else "")
                + (f"  {no_rsi_info}" if no_rsi_info else "")
            )
    except KeyboardInterrupt:
        bot.stop()
        print()
        print("  Bot stopped. Goodbye.")
        print()


if __name__ == "__main__":
    main()
