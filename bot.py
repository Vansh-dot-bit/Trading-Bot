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
import traceback
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
LOG_FORMAT = "%(asctime)s [%(levelname)-8s] %(message)s"
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
            "bot.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
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


def _log_exc(tag: str, msg: str) -> None:
    logger.error(f"[{tag}] {msg}\n{traceback.format_exc()}")


# ================================================================
#  3b.  PRICE FORMATTING UTILITY
# ================================================================

def smart_fmt(price: float) -> str:
    if price == 0:
        return "0"
    abs_p = abs(price)
    if abs_p >= 10_000:
        decimals = 2
    elif abs_p >= 1_000:
        decimals = 3
    elif abs_p >= 100:
        decimals = 4
    elif abs_p >= 10:
        decimals = 5
    elif abs_p >= 1:
        decimals = 6
    elif abs_p >= 0.1:
        decimals = 7
    elif abs_p >= 0.01:
        decimals = 8
    else:
        decimals = 10

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
#  3c.  TIMESTAMP NORMALIZATION
# ================================================================

def normalize_timestamp_to_seconds(raw_ts) -> Optional[int]:
    try:
        ts = float(raw_ts)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    ts = int(ts)
    if ts >= 1_000_000_000_000_000_000:
        ts //= 1_000_000_000
    elif ts >= 1_000_000_000_000_000:
        ts //= 1_000_000
    elif ts >= 1_000_000_000_000:
        ts //= 1_000
    if ts <= 0:
        return None
    return ts


# ================================================================
#  4.  GMAIL NOTIFIER
# ================================================================

class GmailNotifier:
    def __init__(
        self,
        sender_email: str,
        gmail_app_password: str,
        recipient_emails: List[str],
        enabled: bool = True,
    ):
        self.sender_email = sender_email
        self.gmail_app_password = gmail_app_password
        self.recipient_emails = recipient_emails
        self.enabled = enabled
        self._last_sr_alert: Dict[str, str] = {}
        self._last_rejection_alert: Dict[str, str] = {}
        self._consecutive_failures = 0
        self._disabled_until: float = 0.0
        self._max_consecutive_failures = 5
        self._backoff_seconds = 600
        self._failures_lock = threading.Lock()

    def _dispatch_async(self, subject: str, body: str) -> None:
        def _runner():
            try:
                self._send_email(subject, body)
            except Exception as e:
                _log_exc("GMAIL", f"Background email send raised unexpectedly: {e}")

        t = threading.Thread(target=_runner, daemon=True, name="GmailSendAsync")
        t.start()

    def _send_email(self, subject: str, body: str) -> bool:
        if not self.enabled:
            return False

        now = time.time()
        with self._failures_lock:
            disabled_until = self._disabled_until
        if disabled_until and now < disabled_until:
            _log("warning", "GMAIL",
                 f"Skipping send (in backoff after repeated failures) - "
                 f"will retry automatically in {int(disabled_until - now)}s")
            return False

        try:
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = ", ".join(self.recipient_emails)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain", "utf-8"))
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context, timeout=20) as server:
                server.login(self.sender_email, self.gmail_app_password)
                server.send_message(msg)
            _log("info", "GMAIL", f"Email sent: {subject}")
            with self._failures_lock:
                self._consecutive_failures = 0
                self._disabled_until = 0.0
            return True
        except Exception as e:
            with self._failures_lock:
                self._consecutive_failures += 1
                failures = self._consecutive_failures
                if failures >= self._max_consecutive_failures:
                    self._disabled_until = now + self._backoff_seconds
            _log_exc("GMAIL", f"Failed to send email ({failures} consecutive failures): {e}")
            if failures >= self._max_consecutive_failures:
                _log("error", "GMAIL",
                     f"Too many consecutive Gmail failures - backing off for "
                     f"{self._backoff_seconds}s before trying again automatically.")
            return False

    def send_signal(self, signal: dict) -> bool:
        if not self.enabled:
            return False
        direction = signal.get("direction", "UNKNOWN")
        symbol = signal.get("symbol", "UNKNOWN")
        strategy = signal.get("strategy", "UNKNOWN")
        timeframe = signal.get("timeframe", "UNKNOWN")
        entry = signal.get("entry", 0)
        stop_loss = signal.get("stop_loss", 0)
        take_profit = signal.get("take_profit", 0)
        rsi = signal.get("rsi", "N/A")
        mode = signal.get("mode", "PAPER")
        risk_usd = signal.get("risk_usd", 0)
        trading_capital = signal.get("trading_capital", 0)
        signal_time = signal.get("time", datetime.now(timezone.utc).isoformat())
        no_rsi = signal.get("no_rsi", False)

        direction_text = "SHORT" if direction == "SHORT" else "LONG"
        direction_emoji = "\U0001F534" if direction == "SHORT" else "\U0001F7E2"

        risk_dist = abs(entry - stop_loss)
        reward_dist = abs(take_profit - entry)
        rr = (reward_dist / risk_dist) if risk_dist > 0 else 0
        rsi_str = "N/A (no RSI)" if no_rsi else (f"{rsi:.2f}" if isinstance(rsi, float) else str(rsi))
        risk_pct = (risk_usd / trading_capital * 100) if trading_capital > 0 else 0

        subject = f"[{mode}] {direction_emoji} {symbol} {direction_text} - {strategy}"

        body = f"""TRADING SIGNAL ALERT
-----------------------------
Time       : {signal_time}
Mode       : {mode}
Symbol     : {symbol}
Direction  : {direction_text}
Strategy   : {strategy}
Timeframe  : {timeframe}
RSI(14)    : {rsi_str}

ENTRY & EXIT LEVELS
-----------------------------
Entry      : {smart_fmt(entry)}
Stop Loss  : {smart_fmt(stop_loss)}
Take Profit: {smart_fmt(take_profit)}
Risk Dist  : {smart_fmt(risk_dist)}
Reward Dist: {smart_fmt(reward_dist)}
Risk:Reward: {rr:.2f}:1

RISK MANAGEMENT
-----------------------------
Risk $     : ${risk_usd:.2f}
Capital    : ${trading_capital:,.2f}
Risk %     : {risk_pct:.2f}%

STRATEGY NOTES
-----------------------------
Stop Loss  : Triggers on CANDLE CLOSE
Take Profit: Triggers on PRICE TOUCH
SuperTrend : Monitored post-entry"""

        if no_rsi:
            body += "\nRSI Filter : DISABLED"
        if strategy in ("BEARISH_HARAMI", "BULLISH_HARAMI"):
            body += f"\nHarami Tol : {signal.get('harami_tolerance', 0.001) * 100:.2f}%"
        if "breakout_level" in signal:
            body += f"\nBreakout Lvl: {smart_fmt(signal['breakout_level'])}"
        if "level_strength" in signal:
            body += f"\nLevel Strength: {signal['level_strength']}x"
        if "reversal_candle_close" in signal:
            body += f"\nReversal Close: {smart_fmt(signal['reversal_candle_close'])}"
        if "confirmation_close" in signal:
            body += f"\nConfirm Close: {smart_fmt(signal['confirmation_close'])}"

        self._dispatch_async(subject, body)
        return True

    def send_supertrend_strong(self, symbol: str, direction: str, entry: float,
                                new_tp: float, st1: float, st2: float,
                                timeframe: str, mode: str) -> bool:
        if not self.enabled:
            return False
        if direction == "SHORT":
            trend_emoji = "\U0001F534"
            trend_desc = "BEARISH - Both SuperTrends turned RED"
        else:
            trend_emoji = "\U0001F7E2"
            trend_desc = "BULLISH - Both SuperTrends turned GREEN"

        subject = f"[{mode}] {trend_emoji} {symbol} SuperTrend Confirmed - {direction}"

        body = f"""SUPERTREND STRONG TREND CONFIRMATION
-----------------------------
Symbol     : {symbol}
Direction  : {direction}
Trend      : {trend_desc}
Timeframe  : {timeframe}
Mode       : {mode}

TRADE DETAILS
-----------------------------
Entry Price: {smart_fmt(entry)}
Exit Mode  : SuperTrend-based (no fixed TP)

SUPERTREND VALUES
-----------------------------
ST(14,2)   : {smart_fmt(st1)}
ST(21,1)   : {smart_fmt(st2)}

EXIT STRATEGY
-----------------------------
Will exit when BOTH SuperTrends reverse direction:
- {'Flip to GREEN for exit' if direction == 'SHORT' else 'Flip to RED for exit'}"""

        self._dispatch_async(subject, body)
        return True

    def send_supertrend_exit(self, symbol: str, direction: str, entry: float,
                              exit_price: float, realized_pnl: float,
                              timeframe: str, mode: str) -> bool:
        if not self.enabled:
            return False
        is_profit = realized_pnl > 0
        emoji = "\u2705" if is_profit else "\u274C"
        pnl_text = f"+${realized_pnl:.2f}" if is_profit else f"-${abs(realized_pnl):.2f}"
        result_text = "PROFIT" if is_profit else "LOSS"

        if direction == "SHORT":
            flip_desc = "Both SuperTrends flipped GREEN - bearish trend ended"
        else:
            flip_desc = "Both SuperTrends flipped RED - bullish trend ended"

        subject = f"[ST-EXIT] {emoji} {symbol} {direction} - {pnl_text}"

        body = f"""SUPERTREND EXIT - TREND REVERSAL
-----------------------------
Symbol      : {symbol}
Direction   : {direction}
Timeframe   : {timeframe}
Mode        : {mode}

EXIT DETAILS
-----------------------------
Exit Reason : {flip_desc}
Entry       : {smart_fmt(entry)}
Exit Price  : {smart_fmt(exit_price)}

RESULT
-----------------------------
Realized PnL: {pnl_text}
Result      : {result_text}"""

        self._dispatch_async(subject, body)
        return True

    def send_trade_executed(self, trade: dict) -> bool:
        if not self.enabled:
            return False
        direction = trade.get("direction", "UNKNOWN")
        symbol = trade.get("symbol", "UNKNOWN")
        entry = trade.get("entry", 0)
        stop_loss = trade.get("stop_loss", 0)
        take_profit = trade.get("take_profit", 0)
        size = trade.get("size", 0)
        strategy = trade.get("strategy", "UNKNOWN")
        no_rsi = trade.get("no_rsi", False)

        direction_emoji = "\U0001F534" if direction == "SHORT" else "\U0001F7E2"
        subject = f"[EXECUTED] {direction_emoji} {symbol} {direction} - {size} contracts"

        body = f"""TRADE EXECUTED SUCCESSFULLY
-----------------------------
Symbol     : {symbol}
Direction  : {direction}
Strategy   : {strategy}
Size       : {size} contracts

ENTRY & EXIT LEVELS
-----------------------------
Entry      : {smart_fmt(entry)}
Stop Loss  : {smart_fmt(stop_loss)}
Take Profit: {smart_fmt(take_profit)}

EXECUTION NOTES
-----------------------------
Stop Loss  : Triggers on CANDLE CLOSE
Take Profit: Triggers on PRICE TOUCH
SuperTrend : Monitoring post-entry"""

        if no_rsi:
            body += "\nRSI Filter : DISABLED"

        self._dispatch_async(subject, body)
        return True

    def send_trade_closed(self, trade: dict, close_reason: str, pnl_usd: float) -> bool:
        if not self.enabled:
            return False
        direction = trade.get("direction", "UNKNOWN")
        symbol = trade.get("symbol", "UNKNOWN")
        entry = trade.get("entry", 0)
        is_profit = pnl_usd > 0
        emoji = "\u2705" if is_profit else "\u274C"
        pnl_text = f"+${pnl_usd:.2f}" if is_profit else f"-${abs(pnl_usd):.2f}"
        subject = f"[CLOSED] {emoji} {symbol} - {pnl_text} - {close_reason}"

        body = f"""TRADE CLOSED
-----------------------------
Symbol      : {symbol}
Direction   : {direction}
Close Reason: {close_reason}

TRADE DETAILS
-----------------------------
Entry       : {smart_fmt(entry)}
Exit Price  : {trade.get('exit_price', entry)}

RESULT
-----------------------------
Realized PnL: {pnl_text}
Result      : {'PROFIT' if is_profit else 'LOSS'}"""

        self._dispatch_async(subject, body)
        return True

    def send_daily_loss_warning(self, daily_loss_usd: float, daily_limit_usd: float) -> bool:
        if not self.enabled:
            return False
        percent = (daily_loss_usd / daily_limit_usd) * 100 if daily_limit_usd > 0 else 0
        subject = f"WARNING DAILY LOSS WARNING - {percent:.1f}% of limit"

        body = f"""DAILY LOSS LIMIT WARNING
-----------------------------
Current Loss : ${daily_loss_usd:.2f}
Daily Limit  : ${daily_limit_usd:.2f}
Percentage   : {percent:.1f}%
Status       : {'NEAR LIMIT - Caution!' if percent >= 80 else 'Monitoring'}"""

        self._dispatch_async(subject, body)
        return True

    def send_daily_limit_hit(self, daily_loss_usd: float, daily_limit_usd: float) -> bool:
        if not self.enabled:
            return False
        subject = "DAILY LOSS LIMIT HIT - TRADING STOPPED"

        body = f"""DAILY LOSS LIMIT REACHED - TRADING HALTED
-----------------------------
Current Loss : ${daily_loss_usd:.2f}
Daily Limit  : ${daily_limit_usd:.2f}
Status       : TRADING HALTED UNTIL TOMORROW (UTC)
Action       : No new trades will be placed"""

        self._dispatch_async(subject, body)
        return True

    def send_startup_report(self, config: dict, symbols: List[str], harami_tolerance: float) -> bool:
        if not self.enabled:
            return False
        mode = "LIVE TRADING" if not config.get("paper_mode") else "PAPER MODE"
        subject = f"TRADING BOT STARTED - {mode}"

        symbols_list = "\n".join([f"  - {sym}" for sym in symbols[:10]])
        if len(symbols) > 10:
            symbols_list += f"\n  - ... and {len(symbols) - 10} more"

        body = f"""TRADING BOT STARTED - {mode}
-----------------------------
Timeframe      : {config.get('timeframe', '1h')}
Leverage       : {config.get('leverage', 5)}x
Risk/Trade     : {config.get('risk_pct', 2)}%
Max Trades     : {config.get('max_concurrent_trades', 2)}
Daily Loss Cap : {config.get('daily_loss_limit_pct', 0.05) * 100:.0f}%
Capital        : ${config.get('trading_capital', 0):,.2f}

TRADE DIRECTIONS
-----------------------------
SHORT TRADES   : {'ENABLED' if config.get('enable_short', True) else 'DISABLED'}
LONG TRADES    : {'ENABLED' if config.get('enable_long', True) else 'DISABLED'}

MONITORED SYMBOLS ({len(symbols)})
-----------------------------
{symbols_list}"""

        self._dispatch_async(subject, body)
        return True

    def send_sr_level_event(self, symbol: str, event_type: str, level_data: dict,
                             all_supports: List[dict], all_resistances: List[dict]) -> bool:
        if not self.enabled:
            return False

        event_key = f"{symbol}_{event_type}_{level_data.get('price', 0):.10f}"
        if self._last_sr_alert.get(event_key) == event_type:
            return False

        self._last_sr_alert[event_key] = event_type

        if len(self._last_sr_alert) > 100:
            keys = list(self._last_sr_alert.keys())
            for k in keys[:-100]:
                del self._last_sr_alert[k]

        if event_type == "NEW":
            event_emoji = "NEW"
            event_desc = "NEW SUPPORT/RESISTANCE LEVEL DETECTED"
        elif event_type == "EXPIRED":
            event_emoji = "EXPIRED"
            event_desc = "SUPPORT/RESISTANCE LEVEL EXPIRED"
        elif event_type == "REPLACED":
            event_emoji = "REPLACED"
            event_desc = "SUPPORT/RESISTANCE LEVEL REPLACED"
        else:
            return False

        level_type = "SUPPORT" if "SUPPORT" in str(level_data.get("type", "")) else "RESISTANCE"
        price = level_data.get("price", 0)
        strength = level_data.get("strength", 1)
        touches = level_data.get("touches", 0)
        age = level_data.get("age", 0)
        stars = "*" * strength

        subject = f"[S/R-ALERT] {event_emoji} {symbol} - {level_type} {smart_fmt(price)} - {event_type}"

        def format_level_list(levels: List[dict]) -> str:
            if not levels:
                return "  (none)"
            sorted_levels = sorted(levels, key=lambda x: x.get("price", 0))
            lines = []
            for lv in sorted_levels:
                lv_price = lv.get("price", 0)
                lv_strength = lv.get("strength", 1)
                lv_touches = lv.get("touches", 0)
                lv_age = lv.get("age", 0)
                lv_stars = "*" * lv_strength
                lines.append(f"  - {smart_fmt(lv_price):>15}  {lv_stars:>5}  Touches: {lv_touches:>3}  Age: {lv_age:>3}")
            return "\n".join(lines)

        supports_display = all_supports[-50:] if len(all_supports) > 50 else all_supports
        resistances_display = all_resistances[-50:] if len(all_resistances) > 50 else all_resistances

        support_lines = format_level_list(supports_display)
        resistance_lines = format_level_list(resistances_display)

        total_supports = len(all_supports)
        total_resistances = len(all_resistances)
        truncated_s = " (showing last 50)" if len(all_supports) > 50 else ""
        truncated_r = " (showing last 50)" if len(all_resistances) > 50 else ""

        body = f"""S/R LEVEL EVENT
-----------------------------
Event       : {event_desc}
Symbol      : {symbol}
Time        : {datetime.now(timezone.utc).isoformat()}

AFFECTED LEVEL
-----------------------------
Type        : {level_type}
Price       : {smart_fmt(price)}
Strength    : {stars} ({strength})
Touches     : {touches}
Age         : {age} candles"""

        if event_type == "EXPIRED":
            body += "\nReason      : Level aged out (max age reached)"
        elif event_type == "REPLACED":
            body += "\nReason      : Level was broken but confirmation failed\n            -> Immediate replacement at new level"
            if "old_price" in level_data:
                body += (
                    f"\nOld Level   : {smart_fmt(level_data['old_price'])}"
                    f"\nNew Level   : {smart_fmt(level_data['new_price'])}"
                    f"\nBreak Candle: {level_data.get('candle_type', '')} at {smart_fmt(level_data.get('break_price', 0))}"
                )

        body += f"""

CURRENT SUPPORT LEVELS{truncated_s} ({total_supports} total)
-----------------------------
{support_lines}

CURRENT RESISTANCE LEVELS{truncated_r} ({total_resistances} total)
-----------------------------
{resistance_lines}

LEGEND
-----------------------------
* = Strength (more * = stronger level)
Touches = Number of times price has touched this level
Age = Candles since level was created
Max Age = {SR_MAX_LEVEL_AGE} candles base (extends +15 per strength level)"""

        self._dispatch_async(subject, body)
        return True

    def send_sr_rejection(self, symbol: str, direction: str, level_price: float,
                           breakout_close: float, confirm_close: float,
                           rejection_reason: str, strategy: str = "S/R_BREAKOUT") -> bool:
        if not self.enabled:
            return False

        event_key = f"{symbol}_{direction}_{level_price:.10f}_{strategy}"
        if self._last_rejection_alert.get(event_key) == rejection_reason:
            return False

        self._last_rejection_alert[event_key] = rejection_reason

        if len(self._last_rejection_alert) > 200:
            keys = list(self._last_rejection_alert.keys())
            for k in keys[:-200]:
                del self._last_rejection_alert[k]

        direction_emoji = "\U0001F534" if direction == "SHORT" else "\U0001F7E2"
        direction_text = "SHORT" if direction == "SHORT" else "LONG"

        subject = f"[S/R-REJECTED] {direction_emoji} {symbol} {direction_text} - {rejection_reason[:30]}..."

        body = f"""S/R TRADE REJECTED
-----------------------------
Symbol      : {symbol}
Direction   : {direction_text}
Strategy    : {strategy}
Time        : {datetime.now(timezone.utc).isoformat()}

LEVEL DETAILS
-----------------------------
S/R Level   : {smart_fmt(level_price)}
Breakout Candle Close: {smart_fmt(breakout_close)}
Confirmation Candle Close: {smart_fmt(confirm_close)}

REJECTION REASON
-----------------------------
{rejection_reason}

NOTES
-----------------------------
- Trade was NOT executed
- S/R level may be replaced if confirmation failed
- Check logs for more details"""

        self._dispatch_async(subject, body)
        return True

    def send_health_alert(self, issue_key: str, message: str, resolved: bool = False) -> bool:
        if not self.enabled:
            return False

        if resolved:
            subject = f"[BOT HEALTH] RESOLVED - {issue_key}"
            body = f"""BOT HEALTH - ISSUE RESOLVED
-----------------------------
Issue    : {issue_key}
Time     : {datetime.now(timezone.utc).isoformat()}
Details  : {message}

The bot has recovered from this condition and appears to be operating normally again."""
        else:
            subject = f"[BOT HEALTH] WARNING - {issue_key}"
            body = f"""BOT HEALTH WARNING - POSSIBLE SILENT FAILURE
-----------------------------
Issue    : {issue_key}
Time     : {datetime.now(timezone.utc).isoformat()}
Details  : {message}

This means the bot may be running without actually evaluating or executing
trades, even though no crash has occurred. Check the bot logs and dashboard
as soon as possible.

You will get a RESOLVED email automatically once this condition clears.
This alert will not repeat for the same ongoing issue for at least
{HEALTH_ALERT_COOLDOWN // 60} minutes."""

        self._dispatch_async(subject, body)
        return True


# ================================================================
#  5.  WEBSOCKET MANAGER
# ================================================================

class WSState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    STOPPED = "stopped"


@dataclass
class WSConfig:
    url: str = "wss://socket.india.delta.exchange"
    ping_interval: int = 20
    ping_timeout: int = 10
    max_reconnect_attempts: int = 10
    reconnect_base_delay: int = 5
    reconnect_max_delay: int = 30
    reconnect_backoff_multiplier: int = 2


UNHANDLED_WS_MSG_LOG_INTERVAL = 60


class DeltaWebSocket:
    def __init__(self, config: Optional[WSConfig] = None):
        self.config = config or WSConfig()
        self._state = WSState.DISCONNECTED
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._thread_lock = threading.Lock()
        self._subscriptions: Dict[str, List[str]] = {}
        self._subscription_lock = threading.Lock()
        self._reconnect_attempts = 0
        self._reconnect_delay = self.config.reconnect_base_delay
        self._should_stop = threading.Event()
        self._on_candle_callback: Optional[callable] = None
        self._on_connected_callback: Optional[callable] = None
        self._on_disconnected_callback: Optional[callable] = None
        self._on_error_callback: Optional[callable] = None
        self._on_reconnect_callback: Optional[callable] = None
        self._on_subscribed_callback: Optional[callable] = None

        self._unhandled_type_counts: Dict[str, int] = {}
        self._unhandled_type_last_log: Dict[str, float] = {}
        self._first_message_logged = False

    def set_candle_callback(self, cb: callable) -> None:
        self._on_candle_callback = cb

    def set_connected_callback(self, cb: callable) -> None:
        self._on_connected_callback = cb

    def set_disconnected_callback(self, cb: callable) -> None:
        self._on_disconnected_callback = cb

    def set_error_callback(self, cb: callable) -> None:
        self._on_error_callback = cb

    def set_reconnect_callback(self, cb: callable) -> None:
        self._on_reconnect_callback = cb

    def set_subscribed_callback(self, cb: callable) -> None:
        self._on_subscribed_callback = cb

    def subscribe(self, timeframe: str, symbols: List[str]) -> None:
        channel = self._get_channel_name(timeframe)
        with self._subscription_lock:
            self._subscriptions[channel] = list(set(symbols))
        _log("info", "WS", f"Subscription queued: channel={channel}, symbols={symbols}")
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
            self._state = WSState.CONNECTING
            self._thread = threading.Thread(
                target=self._run_forever, daemon=True, name="DeltaWebSocket"
            )
            self._thread.start()

    def stop(self) -> None:
        self._should_stop.set()
        self._state = WSState.STOPPED
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=5.0)

    def is_connected(self) -> bool:
        return self._state == WSState.CONNECTED

    def get_state(self) -> str:
        return self._state.value

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
                        try:
                            self._on_reconnect_callback()
                        except Exception as e:
                            _log_exc("WS", f"Reconnect callback error: {e}")
                    first_connect = False
                    self._reconnect()
            except Exception as e:
                _log_exc("WS", f"Unexpected error in WS loop: {e}")
                if not self._should_stop.is_set():
                    first_connect = False
                    self._reconnect()

    def _connect(self) -> None:
        self._state = WSState.CONNECTING
        self._first_message_logged = False
        _log("info", "WS", f"Connecting to {self.config.url} ...")
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
            _log("error", "WS",
                 f"Max reconnect attempts ({self.config.max_reconnect_attempts}) reached - "
                 f"giving up automatic reconnect. Live candle feed is DOWN until manually restarted "
                 f"or until TradingBot's watchdog force-reconnects with a fresh instance.")
            if self._on_disconnected_callback:
                try:
                    self._on_disconnected_callback()
                except Exception as e:
                    _log_exc("WS", f"Disconnected callback error: {e}")
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
        self._reconnect_delay = self.config.reconnect_base_delay
        _log("info", "WS", f"CONNECTED to {self.config.url}")
        with self._subscription_lock:
            for channel, symbols in self._subscriptions.items():
                if symbols:
                    self._send_subscription(channel, symbols)
        if self._on_connected_callback:
            try:
                self._on_connected_callback()
            except Exception as e:
                _log_exc("WS", f"Connected callback error: {e}")

    def _send_subscription(self, channel: str, symbols: List[str]) -> None:
        if not self._ws:
            _log("error", "WS", f"Cannot send subscription for channel={channel}, symbols={symbols} - no active connection")
            return
        msg = {"type": "subscribe", "payload": {"channels": [{"name": channel, "symbols": symbols}]}}
        try:
            self._ws.send(json.dumps(msg))
            _log("info", "WS", f"Subscription SENT: channel={channel}, symbols={symbols}, raw={json.dumps(msg)}")
        except Exception as e:
            _log_exc("WS", f"Failed to send subscription for channel={channel}, symbols={symbols}: {e}")

    def _on_message(self, ws, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception as e:
            _log_exc("WS", f"Failed to parse WS message as JSON (ignored, feed continues). "
                            f"Raw (truncated to 200 chars): {str(message)[:200]!r}: {e}")
            return

        if not self._first_message_logged:
            self._first_message_logged = True
            _log("info", "WS", f"First message received after connect "
                                f"(type={data.get('type', '?')!r}) - feed is live")

        try:
            msg_type = str(data.get("type", ""))

            if msg_type in ("subscriptions", "success"):
                _log("info", "WS", f"Subscription CONFIRMED by server: {data}")
                if self._on_subscribed_callback:
                    try:
                        self._on_subscribed_callback()
                    except Exception as e:
                        _log_exc("WS", f"Subscribed callback error: {e}")
                return

            if msg_type == "error":
                _log("error", "WS", f"Server returned an ERROR message in response to our request: {data}")
                return

            if msg_type == "subscribe":
                _log("debug", "WS", f"Echo of subscribe request received: {data}")
                return

            candle_data = self._parse_candle_message(data)
            if candle_data is not None:
                symbol = candle_data.get("symbol")
                candle = candle_data.get("candle")
                if symbol and candle:
                    try:
                        self._on_candle_callback(symbol, candle) if self._on_candle_callback else None
                    except Exception as e:
                        _log_exc("WS", f"Candle callback error for {symbol}: {e}")
                return

            if "candlestick" in msg_type.lower():
                _log("error", "WS", f"Candlestick-type message received but could NOT be parsed - raw: {data}")
                return

            self._log_unhandled_type(msg_type, data)

        except Exception as e:
            _log_exc("WS", f"Unexpected error handling WS message (ignored, feed continues): {e}")

    def _log_unhandled_type(self, msg_type: str, data: dict) -> None:
        now = time.time()
        count = self._unhandled_type_counts.get(msg_type, 0) + 1
        self._unhandled_type_counts[msg_type] = count
        last_logged = self._unhandled_type_last_log.get(msg_type, 0.0)
        if count == 1 or (now - last_logged) >= UNHANDLED_WS_MSG_LOG_INTERVAL:
            _log("warning", "WS",
                 f"Unhandled WS message type {msg_type!r} (seen {count}x since connect) - "
                 f"raw sample: {data}")
            self._unhandled_type_last_log[msg_type] = now

    def _parse_candle_message(self, data: dict) -> Optional[dict]:
        msg_type = str(data.get("type", ""))
        if "candlestick" not in msg_type.lower():
            return None

        payload = data
        if isinstance(data.get("data"), dict):
            payload = data["data"]
        elif isinstance(data.get("payload"), dict):
            payload = data["payload"]

        ws_symbol = (
            payload.get("symbol") or data.get("symbol")
            or payload.get("s") or data.get("s") or ""
        )
        if not ws_symbol:
            _log("warning", "WS", f"Candlestick message missing a symbol field - raw: {data}")
            return None

        trading_symbol = ws_symbol if ws_symbol.endswith("_PERP") else ws_symbol + "_PERP"
        candle = self._normalize_candle_flat(payload)
        if candle is None:
            _log("warning", "WS", f"Candlestick message for {ws_symbol} failed OHLCV normalization - raw: {data}")
            return None
        return {"symbol": trading_symbol, "candle": candle}

    def _normalize_candle_flat(self, data: dict) -> Optional[dict]:
        try:
            ts_raw = data.get("candle_start_time")
            if ts_raw is None:
                for key in ("time", "start", "t", "timestamp"):
                    v = data.get(key)
                    if v is not None:
                        ts_raw = v
                        break
            ts = normalize_timestamp_to_seconds(ts_raw)
            if ts is None:
                return None

            o = data.get("open", data.get("o"))
            h = data.get("high", data.get("h"))
            l = data.get("low", data.get("l"))
            c = data.get("close", data.get("c"))
            v = data.get("volume", data.get("v", 0))

            if o is None or h is None or l is None or c is None:
                return None

            return {
                "time": ts,
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v or 0),
            }
        except (TypeError, ValueError):
            return None

    def _on_error(self, ws, error) -> None:
        error_msg = str(error)
        if "10054" not in error_msg and "Connection reset" not in error_msg:
            _log("error", "WS", f"Error: {error_msg}")
        else:
            _log("warning", "WS", f"Connection reset (benign, will reconnect): {error_msg}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._state = WSState.DISCONNECTED
        _log("warning", "WS",
             f"CLOSED (code={close_status_code}, msg={close_msg!r}) - "
             f"will attempt automatic reconnect")


# ================================================================
#  6.  SYMBOL HANDLING
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
#  7.  CONSTANTS
# ================================================================
REST_BASE_INDIA = "https://api.india.delta.exchange"
REST_BASE_GLOBAL = "https://api.delta.exchange"

CANDLE_LIMIT = 200
MAX_RETRIES = 3
RETRY_DELAYS = [5, 10, 15]
TIMEOUT = 30
CANDLE_SAFETY_SHIFT = 1

RSI_PERIOD = 14
RSI_OVERBOUGHT = 55.0
RSI_OVERSOLD = 40.0
RSI_MIN_CANDLES = RSI_PERIOD + 1

FILL_POLL_INTERVAL = 0.5
FILL_POLL_TIMEOUT = 15

DOJI_BODY_RATIO_MAX = 0.30
TP_RR_RATIO = 2.0
TP_MAX_PCT = 0.05
DAILY_LOSS_LIMIT_PCT = 0.05

MIN_ENGULF_BODY_PCT = 0.30
HARAMI_BODY_TOLERANCE = 0.001
RANGE_BREAK_LOOKBACK = 7
VOL_EXP_LOOKBACK = 21

SR_LOOKBACK = 100
SR_SWING_SENSITIVITY = 5
SR_MERGE_THRESHOLD = 0.005
SR_MIN_LEVEL_AGE = 0
SR_MAX_LEVEL_AGE = 100
SR_MIN_STRENGTH = 1
SR_PRICE_TOUCH_THRESHOLD = 0.002

# FINAL MINIMUM DISTANCE FILTER - 1.5% minimum separation between final levels
MIN_SR_DISTANCE_PERCENT = 1.5

ST1_LENGTH = 14
ST1_FACTOR = 2.0
ST2_LENGTH = 21
ST2_FACTOR = 1.0

TIMEFRAME_MAP: Dict[str, Dict] = {
    "1m": {"resolution": "1m", "api_resolution": "1m", "ws_channel": "candlestick_1m", "secs": 60},
    "5m": {"resolution": "5m", "api_resolution": "5m", "ws_channel": "candlestick_5m", "secs": 300},
    "15m": {"resolution": "15m", "api_resolution": "15m", "ws_channel": "candlestick_15m", "secs": 900},
    "1h": {"resolution": "1h", "api_resolution": "1h", "ws_channel": "candlestick_1h", "secs": 3600},
}

WATCHDOG_CHECK_INTERVAL = 300
STALE_CANDLE_MULTIPLIER = 3
EVAL_STALL_MULTIPLIER = 2
HEALTH_ALERT_COOLDOWN = 3600
WS_UPDATE_STALE_SECONDS = 180
CANDLE_CLOSE_GRACE_SECONDS = 90
SYMBOL_ERROR_RESET_THRESHOLD = 3
STALE_RECOVERY_COOLDOWN = 300
RECOVERY_SETTLE_SECONDS = 3
STARTUP_WS_CONNECT_TIMEOUT = 30
STARTUP_SUBSCRIBE_CONFIRM_TIMEOUT = 20
STARTUP_LIVE_DATA_TIMEOUT = 60


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
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "python-DeltaBot/14.9",
            "Accept": "application/json",
            "Connection": "keep-alive",
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
        timestamp = str(int(time.time()))
        query_string = ""
        if params:
            sorted_params = sorted(params.items())
            query_string = "?" + urlencode(sorted_params)
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
        url = base_url + endpoint
        headers = {}
        try:
            if endpoint_type == "private":
                headers = self._sign_request(method, endpoint, params, body)
        except ValueError as e:
            _log("error", "API-REQ", f"Cannot sign request: {e}")
            return None
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
            if retry_count < MAX_RETRIES - 1:
                _log("warning", "API-REQ",
                     f"{method} {endpoint} network error ({exc}); retrying "
                     f"({retry_count + 1}/{MAX_RETRIES - 1})...")
                time.sleep(RETRY_DELAYS[retry_count])
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            _log("error", "API-REQ", f"{method} {endpoint} failed after {MAX_RETRIES} attempts: {exc}")
            return None
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response else 0
            if status == 429 and retry_count < MAX_RETRIES - 1:
                _log("warning", "API-REQ", f"{method} {endpoint} rate-limited (429); retrying...")
                time.sleep(RETRY_DELAYS[retry_count])
                return self.request(method, endpoint, endpoint_type, params, body, retry_count + 1)
            _log("error", "API-REQ", f"{method} {endpoint} HTTP error {status}: {exc}")
            return None
        except Exception as exc:
            _log_exc("API-REQ", f"Unexpected error on {method} {endpoint}: {exc}")
            return None


# ================================================================
#  10. TIMEFRAME SAFETY
# ================================================================

class TimeframeSafe:
    def __init__(self, key: str):
        k = key.strip().lower()
        if k not in TIMEFRAME_MAP:
            raise ValueError(f"[TIMEFRAME] '{k}' not valid. Choose from: {list(TIMEFRAME_MAP)}")
        entry = TIMEFRAME_MAP[k]
        self._key = k
        self._resolution = entry["resolution"]
        self._api_resolution = entry["api_resolution"]
        self._ws_channel = entry["ws_channel"]
        self._secs = entry["secs"]

    @property
    def key(self) -> str:
        return self._key

    @property
    def resolution(self) -> str:
        return self._resolution

    @property
    def api_resolution(self) -> str:
        return self._api_resolution

    @property
    def ws_channel(self) -> str:
        return self._ws_channel

    @property
    def secs(self) -> int:
        return self._secs


# ================================================================
#  11. CANDLE VALIDATOR
# ================================================================

_CANDLE_FIELDS = ("time", "open", "high", "low", "close", "volume")


def validate_candle(candle: dict, symbol: str = "") -> bool:
    for f in _CANDLE_FIELDS:
        if f not in candle or candle[f] is None:
            return False
    for f in ("open", "high", "low", "close", "volume"):
        try:
            v = float(candle[f])
        except (TypeError, ValueError):
            return False
        if math.isnan(v) or math.isinf(v):
            return False
    if candle["time"] <= 0:
        return False
    if candle["high"] < candle["low"]:
        return False
    for f in ("open", "high", "low", "close"):
        if candle[f] <= 0:
            return False
    return True


# ================================================================
#  12. RSI CALCULATION
# ================================================================

def compute_rsi(closed_candles: List[dict], period: int = RSI_PERIOD) -> Optional[float]:
    if len(closed_candles) < period + 1:
        return None
    closes = [float(c["close"]) for c in closed_candles]
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    seed = changes[:period]
    avg_gain = sum(max(ch, 0.0) for ch in seed) / period
    avg_loss = sum(abs(min(ch, 0.0)) for ch in seed) / period
    for ch in changes[period:]:
        avg_gain = (avg_gain * (period - 1) + max(ch, 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + abs(min(ch, 0.0))) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return round(rsi, 2)


def check_rsi_filter(closed_candles: List[dict], symbol: str = "",
                      period: int = RSI_PERIOD,
                      threshold: float = RSI_OVERBOUGHT) -> Tuple[bool, Optional[float]]:
    rsi = compute_rsi(closed_candles, period)
    if rsi is None:
        _log("warning", f"RSI [{symbol}]",
             f"Insufficient candles for RSI({period}): have {len(closed_candles)}, "
             f"need {period + 1} - BLOCKING")
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
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)

    atrs = [0.0] * n
    if n < period + 1:
        return atrs

    seed = sum(trs[1: period + 1]) / period
    atrs[period] = seed
    alpha = 1.0 / period
    for i in range(period + 1, n):
        atrs[i] = atrs[i - 1] * (1 - alpha) + trs[i] * alpha

    return atrs


def compute_supertrend(candles: List[dict],
                        length: int, factor: float) -> List[Optional[bool]]:
    n = len(candles)
    atrs = compute_atr(candles, length)

    directions: List[Optional[bool]] = [None] * n
    upper_bands = [0.0] * n
    lower_bands = [0.0] * n

    start = length

    for i in range(start, n):
        hl2 = (candles[i]["high"] + candles[i]["low"]) / 2.0
        atr = atrs[i]

        raw_upper = hl2 + factor * atr
        raw_lower = hl2 - factor * atr

        if i == start:
            upper_bands[i] = raw_upper
            lower_bands[i] = raw_lower
            directions[i] = candles[i]["close"] >= hl2
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
            close = candles[i]["close"]

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
        n = len(cndls)
        atrs = compute_atr(cndls, length)
        if n < length + 1:
            return None
        upper_bands = [0.0] * n
        lower_bands = [0.0] * n
        dirs: List[Optional[bool]] = [None] * n
        start = length
        for i in range(start, n):
            hl2 = (cndls[i]["high"] + cndls[i]["low"]) / 2.0
            atr = atrs[i]
            raw_upper = hl2 + factor * atr
            raw_lower = hl2 - factor * atr
            if i == start:
                upper_bands[i] = raw_upper
                lower_bands[i] = raw_lower
                dirs[i] = cndls[i]["close"] >= hl2
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
                prev_dir = dirs[i - 1]
                close = cndls[i]["close"]
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

def candle_body(c: dict) -> float:
    return abs(c["close"] - c["open"])


def upper_wick(c: dict) -> float:
    return c["high"] - max(c["open"], c["close"])


def lower_wick(c: dict) -> float:
    return min(c["open"], c["close"]) - c["low"]


def candle_range(c: dict) -> float:
    return c["high"] - c["low"]


def is_bullish(c: dict) -> bool:
    return c["close"] > c["open"]


def is_bearish(c: dict) -> bool:
    return c["close"] < c["open"]


def is_doji(c: dict, body_ratio_max: float = DOJI_BODY_RATIO_MAX) -> bool:
    r = candle_range(c)
    if r <= 0:
        return False
    return (candle_body(c) / r) <= body_ratio_max


# ================================================================
#  13a. SHORT STRATEGIES
# ================================================================

def check_short_signal_strategy_1(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    signal = candles[-2]
    bearish_c = candles[-3]
    bullish_c = candles[-4]

    if not is_bullish(bullish_c):
        return False, None, ""
    if not is_bearish(bearish_c):
        return False, None, ""
    if not is_bearish(signal):
        return False, None, ""
    if signal["close"] >= bullish_c["low"]:
        return False, None, ""

    sc = signal.copy()
    sc["pattern_high"] = max(bullish_c["high"], bearish_c["high"], signal["high"])

    _log("info", "STRATEGY_1_SHORT",
         f"Signal close {signal['close']} < bullish low {bullish_c['low']}")
    return True, sc, "STRATEGY_1_SHORT"


def check_short_signal_strategy_5(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 5:
        return False, None, ""

    signal_c = candles[-2]
    doji_c = candles[-3]
    bullish_2 = candles[-4]
    bullish_1 = candles[-5]

    if not is_bullish(bullish_1):
        return False, None, ""
    if not is_bullish(bullish_2):
        return False, None, ""
    if bullish_2["close"] <= bullish_1["close"]:
        return False, None, ""
    if not is_doji(doji_c):
        return False, None, ""
    if not is_bearish(signal_c):
        return False, None, ""
    if signal_c["close"] >= doji_c["low"]:
        return False, None, ""

    result = signal_c.copy()
    result["doji_low"] = doji_c["low"]
    result["pattern_high"] = doji_c["high"]

    _log("info", "BEARISH_DOJI",
         f"Bullish breakout: C2 close {smart_fmt(bullish_2['close'])} > C1 close {smart_fmt(bullish_1['close'])} | "
         f"Doji (body ratio: {candle_body(doji_c) / candle_range(doji_c) * 100:.1f}%) -> Bearish confirmation: "
         f"Signal close {smart_fmt(signal_c['close'])} < doji low {smart_fmt(doji_c['low'])}")
    return True, result, "BEARISH_DOJI"


def check_short_signal_range_break(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    lookback = RANGE_BREAK_LOOKBACK
    if len(candles) < lookback + 3:
        return False, None, ""

    confirm_candle = candles[-2]
    break_candle = candles[-3]
    range_candles = candles[-(lookback + 3):-3]
    if len(range_candles) != lookback:
        return False, None, ""

    range_high = max(c["high"] for c in range_candles)
    range_low = min(c["low"] for c in range_candles)
    range_size = range_high - range_low

    if range_size <= 0:
        return False, None, ""

    if break_candle["close"] >= range_low:
        return False, None, ""
    if not is_bearish(confirm_candle):
        return False, None, ""
    if confirm_candle["close"] >= break_candle["close"]:
        return False, None, ""

    confirm_candle_copy = confirm_candle.copy()
    confirm_candle_copy["pattern_high"] = break_candle["high"]

    _log("info", "RANGE_BREAK_SHORT",
         f"Range {smart_fmt(range_low)} - {smart_fmt(range_high)} | "
         f"Break close {smart_fmt(break_candle['close'])} < range low | "
         f"Confirm close {confirm_candle['close']} < break close")
    return True, confirm_candle_copy, "RANGE_BREAK_SHORT"


def check_short_signal_vol_expansion(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    lookback = VOL_EXP_LOOKBACK
    if len(candles) < lookback + 2:
        return False, None, ""

    current = candles[-2]
    prev_candles = candles[-(lookback + 2):-2]
    if len(prev_candles) < lookback:
        return False, None, ""

    lowest_low = min(c["low"] for c in prev_candles)

    if current["close"] >= lowest_low:
        return False, None, ""
    if not is_bearish(current):
        return False, None, ""

    current_copy = current.copy()
    current_copy["pattern_high"] = current["high"]
    current_copy["breakout_level"] = lowest_low

    _log("info", "VOL_EXPANSION_SHORT",
         f"Break below 21-candle low {smart_fmt(lowest_low)} | "
         f"Close {smart_fmt(current['close'])} < low | NO WICK CONDITION")
    return True, current_copy, "VOL_EXPANSION_SHORT"


def check_short_signal_support_resistance_manager(candles: List[dict], sr_manager: 'SRLevelManager') -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    confirm_candle = candles[-2]
    break_candle = candles[-3]

    with sr_manager._lock:
        valid_supports = [l for l in sr_manager.support_levels
                           if l["strength"] >= sr_manager.min_strength]

    if not valid_supports:
        return False, None, ""

    valid_supports_sorted = sorted(valid_supports, key=lambda x: x["price"], reverse=True)

    current_price = candles[-1]["close"]

    for level_dict in valid_supports_sorted:
        support = level_dict["price"]

        if abs(support - current_price) > current_price * 0.50:
            continue

        if break_candle["close"] >= support:
            continue

        if not is_bearish(confirm_candle):
            continue

        if confirm_candle["close"] >= break_candle["close"]:
            continue

        signal_candle = confirm_candle.copy()
        signal_candle["pattern_high"] = confirm_candle["high"]
        signal_candle["breakout_level"] = support
        signal_candle["level_strength"] = level_dict["strength"]
        signal_candle["level_touches"] = level_dict["touches"]
        signal_candle["break_candle_close"] = break_candle["close"]
        signal_candle["confirmation_close"] = confirm_candle["close"]

        stars = "*" * level_dict["strength"]

        _log("info", "SUPPORT_BREAKDOWN_SHORT",
             f"SHORT: Support {smart_fmt(support)} broken (break close {smart_fmt(break_candle['close'])}) "
             f"CONFIRMED by bearish candle close {smart_fmt(confirm_candle['close'])} < break close "
             f"(Strength: {stars}, {level_dict['touches']} touches) -> TRADE IMMEDIATE")

        return True, signal_candle, "SUPPORT_BREAKDOWN_SHORT"

    return False, None, ""


def check_short_signal_resistance_false_breakout(candles: List[dict], sr_manager: 'SRLevelManager') -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    false_breakout_candle = candles[-3]
    confirm_candle = candles[-2]

    if not is_bearish(false_breakout_candle):
        return False, None, ""

    resistances, _ = sr_manager.get_levels_near_price(false_breakout_candle["high"], tolerance=0.02)

    if not resistances:
        return False, None, ""

    for level_dict in resistances:
        resistance = level_dict["price"]

        if false_breakout_candle["high"] <= resistance:
            continue

        if false_breakout_candle["close"] >= resistance:
            continue

        if not is_bearish(confirm_candle):
            continue

        if confirm_candle["close"] >= false_breakout_candle["close"]:
            continue

        signal_candle = confirm_candle.copy()
        signal_candle["pattern_high"] = false_breakout_candle["high"]
        signal_candle["breakout_level"] = resistance
        signal_candle["level_strength"] = level_dict["strength"]
        signal_candle["level_touches"] = level_dict["touches"]
        signal_candle["reversal_candle_close"] = false_breakout_candle["close"]
        signal_candle["confirmation_close"] = confirm_candle["close"]
        signal_candle["false_breakout_high"] = false_breakout_candle["high"]

        stars = "*" * level_dict["strength"]

        _log("info", "RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT",
             f"SHORT: Resistance false breakout reversal at {smart_fmt(resistance)} "
             f"(High {smart_fmt(false_breakout_candle['high'])} > Resistance > Close {smart_fmt(false_breakout_candle['close'])}) "
             f"CONFIRMED by bearish candle close {smart_fmt(confirm_candle['close'])} < false breakout close "
             f"(Strength: {stars}, {level_dict['touches']} touches) -> TRADE IMMEDIATE")

        return True, signal_candle, "RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"

    return False, None, ""


# ================================================================
#  13b. LONG STRATEGIES
# ================================================================

def check_long_signal_strategy_1(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    signal = candles[-2]
    bullish_c = candles[-3]
    bearish_c = candles[-4]

    if not is_bearish(bearish_c):
        return False, None, ""
    if not is_bullish(bullish_c):
        return False, None, ""
    if not is_bullish(signal):
        return False, None, ""
    if signal["close"] <= bearish_c["high"]:
        return False, None, ""

    sc = signal.copy()
    sc["pattern_low"] = min(bearish_c["low"], bullish_c["low"], signal["low"])

    _log("info", "STRATEGY_1_LONG",
         f"Signal close {signal['close']} > bearish high {bearish_c['high']}")
    return True, sc, "STRATEGY_1_LONG"


def check_long_signal_strategy_5(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 5:
        return False, None, ""

    signal_c = candles[-2]
    doji_c = candles[-3]
    bearish_2 = candles[-4]
    bearish_1 = candles[-5]

    if not is_bearish(bearish_1):
        return False, None, ""
    if not is_bearish(bearish_2):
        return False, None, ""
    if bearish_2["close"] >= bearish_1["close"]:
        return False, None, ""
    if not is_doji(doji_c):
        return False, None, ""
    if not is_bullish(signal_c):
        return False, None, ""
    if signal_c["close"] <= doji_c["high"]:
        return False, None, ""

    result = signal_c.copy()
    result["doji_high"] = doji_c["high"]
    result["pattern_low"] = doji_c["low"]

    _log("info", "BULLISH_DOJI",
         f"Bearish breakout: C2 close {smart_fmt(bearish_2['close'])} < C1 close {smart_fmt(bearish_1['close'])} | "
         f"Doji (body ratio: {candle_body(doji_c) / candle_range(doji_c) * 100:.1f}%) -> Bullish confirmation: "
         f"Signal close {smart_fmt(signal_c['close'])} > doji high {smart_fmt(doji_c['high'])}")
    return True, result, "BULLISH_DOJI"


def check_long_signal_range_break(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    lookback = RANGE_BREAK_LOOKBACK
    if len(candles) < lookback + 3:
        return False, None, ""

    confirm_candle = candles[-2]
    break_candle = candles[-3]
    range_candles = candles[-(lookback + 3):-3]
    if len(range_candles) != lookback:
        return False, None, ""

    range_high = max(c["high"] for c in range_candles)
    range_low = min(c["low"] for c in range_candles)
    range_size = range_high - range_low

    if range_size <= 0:
        return False, None, ""

    if break_candle["close"] <= range_high:
        return False, None, ""
    if not is_bullish(confirm_candle):
        return False, None, ""
    if confirm_candle["close"] <= break_candle["close"]:
        return False, None, ""

    confirm_candle_copy = confirm_candle.copy()
    confirm_candle_copy["pattern_low"] = break_candle["low"]

    _log("info", "RANGE_BREAK_LONG",
         f"Range {smart_fmt(range_low)} - {smart_fmt(range_high)} | "
         f"Break close {break_candle['close']} > range high | "
         f"Confirm close {confirm_candle['close']} > break close")
    return True, confirm_candle_copy, "RANGE_BREAK_LONG"


def check_long_signal_vol_expansion(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    lookback = VOL_EXP_LOOKBACK
    if len(candles) < lookback + 2:
        return False, None, ""

    current = candles[-2]
    prev_candles = candles[-(lookback + 2):-2]
    if len(prev_candles) < lookback:
        return False, None, ""

    highest_high = max(c["high"] for c in prev_candles)

    if current["close"] <= highest_high:
        return False, None, ""
    if not is_bullish(current):
        return False, None, ""

    current_copy = current.copy()
    current_copy["pattern_low"] = current["low"]
    current_copy["breakout_level"] = highest_high

    _log("info", "VOL_EXPANSION_LONG",
         f"Break above 21-candle high {smart_fmt(highest_high)} | "
         f"Close {smart_fmt(current['close'])} > high | NO WICK CONDITION")
    return True, current_copy, "VOL_EXPANSION_LONG"


def check_long_signal_support_resistance_manager(candles: List[dict], sr_manager: 'SRLevelManager') -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    confirm_candle = candles[-2]
    break_candle = candles[-3]

    with sr_manager._lock:
        valid_resistances = [l for l in sr_manager.resistance_levels
                              if l["strength"] >= sr_manager.min_strength]

    if not valid_resistances:
        return False, None, ""

    valid_resistances_sorted = sorted(valid_resistances, key=lambda x: x["price"])

    current_price = candles[-1]["close"]

    for level_dict in valid_resistances_sorted:
        resistance = level_dict["price"]

        if abs(resistance - current_price) > current_price * 0.50:
            continue

        if break_candle["close"] <= resistance:
            continue

        if not is_bullish(confirm_candle):
            continue

        if confirm_candle["close"] <= break_candle["close"]:
            continue

        signal_candle = confirm_candle.copy()
        signal_candle["pattern_low"] = confirm_candle["low"]
        signal_candle["breakout_level"] = resistance
        signal_candle["level_strength"] = level_dict["strength"]
        signal_candle["level_touches"] = level_dict["touches"]
        signal_candle["break_candle_close"] = break_candle["close"]
        signal_candle["confirmation_close"] = confirm_candle["close"]

        stars = "*" * level_dict["strength"]

        _log("info", "RESISTANCE_BREAKOUT_LONG",
             f"LONG: Resistance {smart_fmt(resistance)} broken (break close {smart_fmt(break_candle['close'])}) "
             f"CONFIRMED by bullish candle close {smart_fmt(confirm_candle['close'])} > break close "
             f"(Strength: {stars}, {level_dict['touches']} touches) -> TRADE IMMEDIATE")

        return True, signal_candle, "RESISTANCE_BREAKOUT_LONG"

    return False, None, ""


def check_long_signal_support_false_breakout(candles: List[dict], sr_manager: 'SRLevelManager') -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 4:
        return False, None, ""

    false_breakout_candle = candles[-3]
    confirm_candle = candles[-2]

    if not is_bullish(false_breakout_candle):
        return False, None, ""

    _, supports = sr_manager.get_levels_near_price(false_breakout_candle["low"], tolerance=0.02)

    if not supports:
        return False, None, ""

    for level_dict in supports:
        support = level_dict["price"]

        if false_breakout_candle["low"] >= support:
            continue

        if false_breakout_candle["close"] <= support:
            continue

        if not is_bullish(confirm_candle):
            continue

        if confirm_candle["close"] <= false_breakout_candle["close"]:
            continue

        signal_candle = confirm_candle.copy()
        signal_candle["pattern_low"] = false_breakout_candle["low"]
        signal_candle["breakout_level"] = support
        signal_candle["level_strength"] = level_dict["strength"]
        signal_candle["level_touches"] = level_dict["touches"]
        signal_candle["reversal_candle_close"] = false_breakout_candle["close"]
        signal_candle["confirmation_close"] = confirm_candle["close"]
        signal_candle["false_breakout_low"] = false_breakout_candle["low"]

        stars = "*" * level_dict["strength"]

        _log("info", "SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG",
             f"LONG: Support false breakout reversal at {smart_fmt(support)} "
             f"(Low {smart_fmt(false_breakout_candle['low'])} < Support < Close {smart_fmt(false_breakout_candle['close'])}) "
             f"CONFIRMED by bullish candle close {smart_fmt(confirm_candle['close'])} > false breakout close "
             f"(Strength: {stars}, {level_dict['touches']} touches) -> TRADE IMMEDIATE")

        return True, signal_candle, "SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"

    return False, None, ""


# ================================================================
#  13c. ENGULFING STRATEGIES
# ================================================================

def check_short_signal_bearish_engulfing(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 3:
        return False, None, ""

    signal = candles[-2]
    prev = candles[-3]

    if not is_bullish(prev):
        return False, None, ""
    if not is_bearish(signal):
        return False, None, ""
    if signal["open"] <= prev["close"] or signal["close"] >= prev["open"]:
        return False, None, ""

    signal_body = candle_body(signal)
    signal_range = candle_range(signal)

    if signal_range <= 0 or (signal_body / signal_range) < MIN_ENGULF_BODY_PCT:
        return False, None, ""

    signal_candle = signal.copy()
    signal_candle["pattern_high"] = signal["high"]

    _log("info", "BEARISH_ENGULFING",
         f"Bearish Engulfing: Signal body {smart_fmt(signal_body)} engulfed prev body "
         f"(range ratio: {(signal_body / signal_range) * 100:.1f}%)")
    return True, signal_candle, "BEARISH_ENGULFING"


def check_long_signal_bullish_engulfing(candles: List[dict]) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 3:
        return False, None, ""

    signal = candles[-2]
    prev = candles[-3]

    if not is_bearish(prev):
        return False, None, ""
    if not is_bullish(signal):
        return False, None, ""
    if signal["open"] >= prev["close"] or signal["close"] <= prev["open"]:
        return False, None, ""

    signal_body = candle_body(signal)
    signal_range = candle_range(signal)

    if signal_range <= 0 or (signal_body / signal_range) < MIN_ENGULF_BODY_PCT:
        return False, None, ""

    signal_candle = signal.copy()
    signal_candle["pattern_low"] = signal["low"]

    _log("info", "BULLISH_ENGULFING",
         f"Bullish Engulfing: Signal body {smart_fmt(signal_body)} engulfed prev body "
         f"(range ratio: {(signal_body / signal_range) * 100:.1f}%)")
    return True, signal_candle, "BULLISH_ENGULFING"


# ================================================================
#  13d. HARAMI STRATEGIES
# ================================================================

def check_short_signal_bearish_harami(candles: List[dict], harami_tolerance: float = HARAMI_BODY_TOLERANCE) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 3:
        return False, None, ""

    signal = candles[-2]
    prev = candles[-3]

    if not is_bullish(prev):
        return False, None, ""
    if not is_bearish(signal):
        return False, None, ""

    prev_body_top = max(prev["open"], prev["close"])
    prev_body_bottom = min(prev["open"], prev["close"])
    signal_body_top = max(signal["open"], signal["close"])
    signal_body_bottom = min(signal["open"], signal["close"])

    tolerance_amount = prev_body_top * harami_tolerance

    if signal_body_top > prev_body_top + tolerance_amount:
        return False, None, ""
    if signal_body_bottom < prev_body_bottom - tolerance_amount:
        return False, None, ""
    if signal_body_top >= prev_body_top and signal_body_bottom <= prev_body_bottom:
        return False, None, ""

    signal_candle = signal.copy()
    signal_candle["pattern_high"] = prev["high"]
    signal_candle["harami_tolerance"] = harami_tolerance

    _log("info", "BEARISH_HARAMI",
         f"Bearish Harami: Signal body inside prev body (tolerance: {harami_tolerance * 100:.2f}%)")
    return True, signal_candle, "BEARISH_HARAMI"


def check_long_signal_bullish_harami(candles: List[dict], harami_tolerance: float = HARAMI_BODY_TOLERANCE) -> Tuple[bool, Optional[dict], str]:
    if len(candles) < 3:
        return False, None, ""

    signal = candles[-2]
    prev = candles[-3]

    if not is_bearish(prev):
        return False, None, ""
    if not is_bullish(signal):
        return False, None, ""

    prev_body_top = max(prev["open"], prev["close"])
    prev_body_bottom = min(prev["open"], prev["close"])
    signal_body_top = max(signal["open"], signal["close"])
    signal_body_bottom = min(signal["open"], signal["close"])

    tolerance_amount = prev_body_top * harami_tolerance

    if signal_body_top > prev_body_top + tolerance_amount:
        return False, None, ""
    if signal_body_bottom < prev_body_bottom - tolerance_amount:
        return False, None, ""
    if signal_body_top >= prev_body_top and signal_body_bottom <= prev_body_bottom:
        return False, None, ""

    signal_candle = signal.copy()
    signal_candle["pattern_low"] = prev["low"]
    signal_candle["harami_tolerance"] = harami_tolerance

    _log("info", "BULLISH_HARAMI",
         f"Bullish Harami: Signal body inside prev body (tolerance: {harami_tolerance * 100:.2f}%)")
    return True, signal_candle, "BULLISH_HARAMI"


# ================================================================
#  13e. SIGNAL CHECKERS
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

    triggered, signal_candle, strategy = check_short_signal_strategy_1(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} - CONFIRMED")
        return True, signal_candle, strategy, rsi_value

    triggered, signal_candle, strategy = check_short_signal_strategy_5(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} - CONFIRMED")
        return True, signal_candle, strategy, rsi_value

    triggered, signal_candle, strategy = check_short_signal_bearish_engulfing(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} - CONFIRMED")
        return True, signal_candle, strategy, rsi_value

    triggered, signal_candle, strategy = check_short_signal_bearish_harami(candles, harami_tolerance)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | RSI={rsi_value:.2f} > {RSI_OVERBOUGHT} - CONFIRMED")
        return True, signal_candle, strategy, rsi_value

    return False, None, "", rsi_value


def check_short_signal_no_rsi(
    candles: List[dict], sr_manager: 'SRLevelManager', symbol: str = "",
    harami_tolerance: float = HARAMI_BODY_TOLERANCE,
    notifier: Optional[GmailNotifier] = None
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    triggered, signal_candle, strategy = check_short_signal_range_break(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | NO RSI FILTER - CONFIRMED")
        return True, signal_candle, strategy, None

    triggered, signal_candle, strategy = check_short_signal_vol_expansion(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | NO RSI FILTER - CONFIRMED")
        return True, signal_candle, strategy, None

    if len(candles) >= 4:
        confirm_candle = candles[-2]
        break_candle = candles[-3]

        with sr_manager._lock:
            valid_supports = [l for l in sr_manager.support_levels
                               if l["strength"] >= sr_manager.min_strength]

        if valid_supports:
            support_found = False
            for level_dict in valid_supports:
                support = level_dict["price"]
                current_price = candles[-1]["close"]

                if abs(support - current_price) > current_price * 0.50:
                    continue

                support_found = True

                if break_candle["close"] >= support:
                    rejection_reason = f"Break candle close {smart_fmt(break_candle['close'])} did not close below Support {smart_fmt(support)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT S/R Breakdown: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=support,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_BREAKDOWN_SHORT"
                        )
                    continue

                if not is_bearish(confirm_candle):
                    rejection_reason = f"Confirmation candle is not Bearish (close={smart_fmt(confirm_candle['close'])}, open={smart_fmt(confirm_candle['open'])})"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT S/R Breakdown: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=support,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_BREAKDOWN_SHORT"
                        )
                    continue

                if confirm_candle["close"] >= break_candle["close"]:
                    rejection_reason = f"Confirmation close {smart_fmt(confirm_candle['close'])} not below break close {smart_fmt(break_candle['close'])}"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT S/R Breakdown: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=support,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_BREAKDOWN_SHORT"
                        )
                    continue

                triggered, signal_candle, strategy = check_short_signal_support_resistance_manager(candles, sr_manager)
                if triggered:
                    _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | NO RSI FILTER - CONFIRMED IMMEDIATE")
                    return True, signal_candle, strategy, None

                break

            if not support_found:
                rejection_reason = "No matching Support level found (strength >= 1)"
                _log("info", "S/R-REJECT", f"[{symbol}] SHORT S/R Breakdown: {rejection_reason}")
                if notifier:
                    notifier.send_sr_rejection(
                        symbol=symbol, direction="SHORT",
                        level_price=0,
                        breakout_close=break_candle["close"],
                        confirm_close=confirm_candle["close"],
                        rejection_reason=rejection_reason,
                        strategy="SUPPORT_BREAKDOWN_SHORT"
                    )
        else:
            rejection_reason = "No valid Support levels available (strength >= 1 required)"
            _log("info", "S/R-REJECT", f"[{symbol}] SHORT S/R Breakdown: {rejection_reason}")
            if notifier and len(candles) >= 4:
                notifier.send_sr_rejection(
                    symbol=symbol, direction="SHORT",
                    level_price=0,
                    breakout_close=candles[-3]["close"],
                    confirm_close=candles[-2]["close"],
                    rejection_reason=rejection_reason,
                    strategy="SUPPORT_BREAKDOWN_SHORT"
                )

    if len(candles) >= 4:
        false_breakout_candle = candles[-3]
        confirm_candle = candles[-2]

        resistances, _ = sr_manager.get_levels_near_price(false_breakout_candle["high"], tolerance=0.02)

        if resistances:
            resistance_found = False
            for level_dict in resistances:
                resistance = level_dict["price"]
                resistance_found = True

                if false_breakout_candle["high"] <= resistance:
                    rejection_reason = f"False breakout high {smart_fmt(false_breakout_candle['high'])} not above Resistance {smart_fmt(resistance)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=resistance,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                        )
                    continue

                if false_breakout_candle["close"] >= resistance:
                    rejection_reason = f"False breakout close {smart_fmt(false_breakout_candle['close'])} not below Resistance {smart_fmt(resistance)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=resistance,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                        )
                    continue

                if not is_bearish(confirm_candle):
                    rejection_reason = f"Confirmation candle is not Bearish (close={smart_fmt(confirm_candle['close'])}, open={smart_fmt(confirm_candle['open'])})"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=resistance,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                        )
                    continue

                if confirm_candle["close"] >= false_breakout_candle["close"]:
                    rejection_reason = f"Confirmation close {smart_fmt(confirm_candle['close'])} not below false breakout close {smart_fmt(false_breakout_candle['close'])}"
                    _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="SHORT",
                            level_price=resistance,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                        )
                    continue

                triggered, signal_candle, strategy = check_short_signal_resistance_false_breakout(candles, sr_manager)
                if triggered:
                    _log("info", "SIGNAL", f"[{symbol}] SHORT {strategy} | NO RSI FILTER - CONFIRMED IMMEDIATE")
                    return True, signal_candle, strategy, None

                break

            if not resistance_found:
                rejection_reason = "No matching Resistance level found near false breakout high"
                _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
                if notifier:
                    notifier.send_sr_rejection(
                        symbol=symbol, direction="SHORT",
                        level_price=0,
                        breakout_close=false_breakout_candle["close"],
                        confirm_close=confirm_candle["close"],
                        rejection_reason=rejection_reason,
                        strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                    )
        else:
            rejection_reason = "No Resistance levels found near false breakout high (tolerance 2%)"
            _log("info", "S/R-REJECT", f"[{symbol}] SHORT Resistance False Breakout: {rejection_reason}")
            if notifier:
                notifier.send_sr_rejection(
                    symbol=symbol, direction="SHORT",
                    level_price=0,
                    breakout_close=false_breakout_candle["close"],
                    confirm_close=confirm_candle["close"],
                    rejection_reason=rejection_reason,
                    strategy="RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"
                )

    return False, None, "", None


def check_long_signal(
    candles: List[dict], symbol: str = "", harami_tolerance: float = HARAMI_BODY_TOLERANCE
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    closed_candles = candles[:-1]
    rsi = compute_rsi(closed_candles, RSI_PERIOD)
    if rsi is None:
        return False, None, "", None
    if rsi < 24.0:
        _log("warning", f"RSI [{symbol}]", f"RSI={rsi:.2f} < 24 - BLOCKING LONG TRADE (extreme oversold)")
        return False, None, "", rsi
    if rsi >= RSI_OVERSOLD:
        return False, None, "", rsi
    _log("info", f"RSI [{symbol}]", f"RSI={rsi:.2f} < {RSI_OVERSOLD} - LONG filter passes")

    triggered, signal_candle, strategy = check_long_signal_strategy_1(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} - CONFIRMED")
        return True, signal_candle, strategy, rsi

    triggered, signal_candle, strategy = check_long_signal_strategy_5(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} - CONFIRMED")
        return True, signal_candle, strategy, rsi

    triggered, signal_candle, strategy = check_long_signal_bullish_engulfing(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} - CONFIRMED")
        return True, signal_candle, strategy, rsi

    triggered, signal_candle, strategy = check_long_signal_bullish_harami(candles, harami_tolerance)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | RSI={rsi:.2f} < {RSI_OVERSOLD} - CONFIRMED")
        return True, signal_candle, strategy, rsi

    return False, None, "", rsi


def check_long_signal_no_rsi(
    candles: List[dict], sr_manager: 'SRLevelManager', symbol: str = "",
    harami_tolerance: float = HARAMI_BODY_TOLERANCE,
    notifier: Optional[GmailNotifier] = None
) -> Tuple[bool, Optional[dict], str, Optional[float]]:
    triggered, signal_candle, strategy = check_long_signal_range_break(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | NO RSI FILTER - CONFIRMED")
        return True, signal_candle, strategy, None

    triggered, signal_candle, strategy = check_long_signal_vol_expansion(candles)
    if triggered:
        _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | NO RSI FILTER - CONFIRMED")
        return True, signal_candle, strategy, None

    if len(candles) >= 4:
        confirm_candle = candles[-2]
        break_candle = candles[-3]

        with sr_manager._lock:
            valid_resistances = [l for l in sr_manager.resistance_levels
                                  if l["strength"] >= sr_manager.min_strength]

        if valid_resistances:
            resistance_found = False
            for level_dict in valid_resistances:
                resistance = level_dict["price"]
                current_price = candles[-1]["close"]

                if abs(resistance - current_price) > current_price * 0.50:
                    continue

                resistance_found = True

                if break_candle["close"] <= resistance:
                    rejection_reason = f"Break candle close {smart_fmt(break_candle['close'])} did not close above Resistance {smart_fmt(resistance)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG S/R Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=resistance,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_BREAKOUT_LONG"
                        )
                    continue

                if not is_bullish(confirm_candle):
                    rejection_reason = f"Confirmation candle is not Bullish (close={smart_fmt(confirm_candle['close'])}, open={smart_fmt(confirm_candle['open'])})"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG S/R Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=resistance,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_BREAKOUT_LONG"
                        )
                    continue

                if confirm_candle["close"] <= break_candle["close"]:
                    rejection_reason = f"Confirmation close {smart_fmt(confirm_candle['close'])} not above break close {smart_fmt(break_candle['close'])}"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG S/R Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=resistance,
                            breakout_close=break_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="RESISTANCE_BREAKOUT_LONG"
                        )
                    continue

                triggered, signal_candle, strategy = check_long_signal_support_resistance_manager(candles, sr_manager)
                if triggered:
                    _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | NO RSI FILTER - CONFIRMED IMMEDIATE")
                    return True, signal_candle, strategy, None

                break

            if not resistance_found:
                rejection_reason = "No matching Resistance level found (strength >= 1)"
                _log("info", "S/R-REJECT", f"[{symbol}] LONG S/R Breakout: {rejection_reason}")
                if notifier:
                    notifier.send_sr_rejection(
                        symbol=symbol, direction="LONG",
                        level_price=0,
                        breakout_close=break_candle["close"],
                        confirm_close=confirm_candle["close"],
                        rejection_reason=rejection_reason,
                        strategy="RESISTANCE_BREAKOUT_LONG"
                    )
        else:
            rejection_reason = "No valid Resistance levels available (strength >= 1 required)"
            _log("info", "S/R-REJECT", f"[{symbol}] LONG S/R Breakout: {rejection_reason}")
            if notifier and len(candles) >= 4:
                notifier.send_sr_rejection(
                    symbol=symbol, direction="LONG",
                    level_price=0,
                    breakout_close=candles[-3]["close"],
                    confirm_close=candles[-2]["close"],
                    rejection_reason=rejection_reason,
                    strategy="RESISTANCE_BREAKOUT_LONG"
                )

    if len(candles) >= 4:
        false_breakout_candle = candles[-3]
        confirm_candle = candles[-2]

        _, supports = sr_manager.get_levels_near_price(false_breakout_candle["low"], tolerance=0.02)

        if supports:
            support_found = False
            for level_dict in supports:
                support = level_dict["price"]
                support_found = True

                if false_breakout_candle["low"] >= support:
                    rejection_reason = f"False breakout low {smart_fmt(false_breakout_candle['low'])} not below Support {smart_fmt(support)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=support,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                        )
                    continue

                if false_breakout_candle["close"] <= support:
                    rejection_reason = f"False breakout close {smart_fmt(false_breakout_candle['close'])} not above Support {smart_fmt(support)}"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=support,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                        )
                    continue

                if not is_bullish(confirm_candle):
                    rejection_reason = f"Confirmation candle is not Bullish (close={smart_fmt(confirm_candle['close'])}, open={smart_fmt(confirm_candle['open'])})"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=support,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                        )
                    continue

                if confirm_candle["close"] <= false_breakout_candle["close"]:
                    rejection_reason = f"Confirmation close {smart_fmt(confirm_candle['close'])} not above false breakout close {smart_fmt(false_breakout_candle['close'])}"
                    _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
                    if notifier:
                        notifier.send_sr_rejection(
                            symbol=symbol, direction="LONG",
                            level_price=support,
                            breakout_close=false_breakout_candle["close"],
                            confirm_close=confirm_candle["close"],
                            rejection_reason=rejection_reason,
                            strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                        )
                    continue

                triggered, signal_candle, strategy = check_long_signal_support_false_breakout(candles, sr_manager)
                if triggered:
                    _log("info", "SIGNAL", f"[{symbol}] LONG {strategy} | NO RSI FILTER - CONFIRMED IMMEDIATE")
                    return True, signal_candle, strategy, None

                break

            if not support_found:
                rejection_reason = "No matching Support level found near false breakout low"
                _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
                if notifier:
                    notifier.send_sr_rejection(
                        symbol=symbol, direction="LONG",
                        level_price=0,
                        breakout_close=false_breakout_candle["close"],
                        confirm_close=confirm_candle["close"],
                        rejection_reason=rejection_reason,
                        strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                    )
        else:
            rejection_reason = "No Support levels found near false breakout low (tolerance 2%)"
            _log("info", "S/R-REJECT", f"[{symbol}] LONG Support False Breakout: {rejection_reason}")
            if notifier:
                notifier.send_sr_rejection(
                    symbol=symbol, direction="LONG",
                    level_price=0,
                    breakout_close=false_breakout_candle["close"],
                    confirm_close=confirm_candle["close"],
                    rejection_reason=rejection_reason,
                    strategy="SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"
                )

    return False, None, "", None


# ================================================================
#  14. SUPPORT/RESISTANCE LEVEL MANAGER (FIXED - WITH RECLASSIFICATION)
# ================================================================

class SRLevelManager:
    def __init__(self, symbol: str, lookback: int = SR_LOOKBACK,
                 merge_threshold: float = SR_MERGE_THRESHOLD,
                 min_age: int = SR_MIN_LEVEL_AGE,
                 max_age: int = SR_MAX_LEVEL_AGE,
                 min_strength: int = SR_MIN_STRENGTH,
                 min_distance_percent: float = MIN_SR_DISTANCE_PERCENT,
                 notifier: Optional[GmailNotifier] = None):
        self.symbol = symbol
        self.lookback = lookback
        self.merge_threshold = merge_threshold
        self.min_age = 0
        self.max_age = max_age
        self.min_strength = min_strength
        self.min_distance_percent = min_distance_percent
        self.notifier = notifier

        self.resistance_levels: List[Dict] = []
        self.support_levels: List[Dict] = []
        self.pending_swing_highs: List[float] = []
        self.pending_swing_lows: List[float] = []

        # Track which candle indices have been fully processed
        self._processed_indices: set = set()
        self._last_processed_index = -1

        self._lock = threading.RLock()

    def _progress_log(self, msg: str) -> None:
        logger.debug(f"[S/R-DEBUG][{self.symbol}] {msg}")

    def _log_new_level(self, level_type: str, price: float) -> None:
        """Log a new level detection."""
        _log("info", "S/R", f"[{self.symbol}] NEW {level_type} | {smart_fmt(price)}")

    def _log_merged_levels(self, level_type: str, prices: List[float], merged_price: float) -> None:
        """Log when levels are merged."""
        price_strs = [smart_fmt(p) for p in prices]
        _log("info", "S/R", f"[{self.symbol}] MERGED | {' + '.join(price_strs)} → {smart_fmt(merged_price)}")

    def _log_filtered_levels(self, level_type: str, prices: List[float], merged_price: float) -> None:
        """Log when levels are filtered by proximity."""
        price_strs = [smart_fmt(p) for p in prices]
        _log("info", "S/R", f"[{self.symbol}] FILTERED | {' + '.join(price_strs)} → {smart_fmt(merged_price)} | <{self.min_distance_percent}%")

    def _log_reclassified_level(self, old_type: str, new_type: str, price: float, current_price: float) -> None:
        """Log when a level is reclassified."""
        _log("info", "S/R", f"[{self.symbol}] RECLASSIFIED | {old_type} → {new_type} | {smart_fmt(price)} | price={smart_fmt(current_price)}")

    def _log_final_levels(self) -> None:
        """Log final support and resistance levels."""
        support_strs = [smart_fmt(l["price"]) for l in sorted(self.support_levels, key=lambda x: x["price"])]
        resistance_strs = [smart_fmt(l["price"]) for l in sorted(self.resistance_levels, key=lambda x: x["price"])]

        s_str = ", ".join(support_strs) if support_strs else "none"
        r_str = ", ".join(resistance_strs) if resistance_strs else "none"

        _log("info", "S/R", f"[{self.symbol}] FINAL | S: {s_str} | R: {r_str}")

    def _reclassify_levels_by_price(self, current_price: float) -> None:
        """
        Reclassify all levels based on current price:
        - Level below current price → SUPPORT
        - Level above current price → RESISTANCE
        This ensures correct classification after price moves significantly.
        """
        with self._lock:
            reclassified = []

            # Process resistance levels
            new_resistances = []
            for level in self.resistance_levels:
                price = level["price"]
                if price < current_price:
                    # Should be support
                    level_copy = dict(level)
                    level_copy["type"] = "SUPPORT"
                    self.support_levels.append(level_copy)
                    self._log_reclassified_level("RESISTANCE", "SUPPORT", price, current_price)
                    reclassified.append(level_copy)
                else:
                    new_resistances.append(level)
            self.resistance_levels = new_resistances

            # Process support levels
            new_supports = []
            for level in self.support_levels:
                price = level["price"]
                if price > current_price:
                    # Should be resistance
                    level_copy = dict(level)
                    level_copy["type"] = "RESISTANCE"
                    self.resistance_levels.append(level_copy)
                    self._log_reclassified_level("SUPPORT", "RESISTANCE", price, current_price)
                    reclassified.append(level_copy)
                else:
                    new_supports.append(level)
            self.support_levels = new_supports

            # Remove duplicates (if a level was in both lists)
            self._deduplicate_levels()

            if reclassified:
                self._progress_log(f"Reclassified {len(reclassified)} levels based on price {smart_fmt(current_price)}")

    def _deduplicate_levels(self) -> None:
        """Remove duplicate levels within tolerance from both lists."""
        # Deduplicate supports
        unique_supports = {}
        for level in self.support_levels:
            price = level["price"]
            # Find if there's already a level within tolerance
            found = False
            for existing in list(unique_supports.values()):
                if abs(existing["price"] - price) / max(price, 1) < self.merge_threshold:
                    # Keep the one with higher strength
                    if level.get("strength", 1) > existing.get("strength", 1):
                        unique_supports[id(level)] = level
                    found = True
                    break
            if not found:
                unique_supports[id(level)] = level
        self.support_levels = list(unique_supports.values())

        # Deduplicate resistances
        unique_resistances = {}
        for level in self.resistance_levels:
            price = level["price"]
            found = False
            for existing in list(unique_resistances.values()):
                if abs(existing["price"] - price) / max(price, 1) < self.merge_threshold:
                    if level.get("strength", 1) > existing.get("strength", 1):
                        unique_resistances[id(level)] = level
                    found = True
                    break
            if not found:
                unique_resistances[id(level)] = level
        self.resistance_levels = list(unique_resistances.values())

    def _merge_levels(self, levels: List[Dict], level_type: str) -> List[Dict]:
        """Merge nearby levels using weighted average based on touches."""
        if not levels:
            return []

        # Sort by price
        sorted_levels = sorted(levels, key=lambda x: x["price"])
        merged = []
        i = 0

        while i < len(sorted_levels):
            current = sorted_levels[i]
            group = [current]
            j = i + 1

            # Find all levels within merge threshold
            while j < len(sorted_levels):
                # Check if next level is within threshold of current group
                group_avg = sum(l["price"] for l in group) / len(group)
                if abs(sorted_levels[j]["price"] - group_avg) / max(group_avg, 1) <= self.merge_threshold:
                    group.append(sorted_levels[j])
                    j += 1
                else:
                    break

            if len(group) == 1:
                merged.append(group[0])
            else:
                # Merge the group into one level using weighted average
                total_touches = sum(l.get("touches", 1) for l in group)
                total_strength = sum(l.get("strength", 1) for l in group)
                merged_price = 0.0
                total_weight = 0.0

                for l in group:
                    weight = l.get("touches", 1) * l.get("strength", 1)
                    merged_price += l["price"] * weight
                    total_weight += weight

                if total_weight > 0:
                    merged_price /= total_weight
                else:
                    merged_price = sum(l["price"] for l in group) / len(group)

                # Create merged level
                merged_level = {
                    "price": round(merged_price, 8),
                    "age": min(l.get("age", 0) for l in group),
                    "strength": min(5, total_strength),
                    "touches": total_touches,
                    "type": level_type,
                    "_merged_from": len(group),
                    "_original_prices": [l["price"] for l in group],
                }

                # Log the merge
                self._log_merged_levels(level_type, [l["price"] for l in group], merged_price)
                self._progress_log(f"Merged {level_type} levels: {[smart_fmt(l['price']) for l in group]} → {smart_fmt(merged_price)} (touches={total_touches}, strength={min(5, total_strength)})")

                merged.append(merged_level)

            i = j

        return merged

    def _apply_final_proximity_filter(self, levels: List[Dict], level_type: str) -> List[Dict]:
        """
        Apply iterative minimum-distance filter to ensure no two levels of the same type
        are closer than min_distance_percent. Runs iteratively until no pairs remain.
        """
        if not levels:
            return []

        # Make a mutable copy
        working = levels.copy()
        merged_count = 0
        iterations = 0

        while True:
            iterations += 1
            if len(working) <= 1:
                break

            # Sort by price
            working.sort(key=lambda x: x["price"])
            merged_this_round = []
            new_working = []
            i = 0

            while i < len(working):
                current = working[i]
                group = [current]
                j = i + 1

                # Find all levels within the minimum distance threshold
                while j < len(working):
                    # Calculate percentage distance between current group avg and next level
                    group_avg = sum(l["price"] for l in group) / len(group)
                    dist_pct = abs(working[j]["price"] - group_avg) / max(group_avg, 1) * 100

                    if dist_pct < self.min_distance_percent:
                        group.append(working[j])
                        j += 1
                    else:
                        break

                if len(group) == 1:
                    new_working.append(group[0])
                else:
                    # Merge the group into one stronger level
                    merged_this_round.extend(group)

                    # Weighted average for the final price
                    total_weight = 0.0
                    merged_price = 0.0
                    total_touches = 0
                    total_strength = 0
                    min_age = float('inf')

                    for l in group:
                        weight = l.get("touches", 1) * l.get("strength", 1)
                        merged_price += l["price"] * weight
                        total_weight += weight
                        total_touches += l.get("touches", 1)
                        total_strength += l.get("strength", 1)
                        if l.get("age", 0) < min_age:
                            min_age = l.get("age", 0)

                    if total_weight > 0:
                        merged_price /= total_weight
                    else:
                        merged_price = sum(l["price"] for l in group) / len(group)

                    # Create merged level
                    merged_level = {
                        "price": round(merged_price, 8),
                        "age": min_age if min_age != float('inf') else 0,
                        "strength": min(5, total_strength),
                        "touches": total_touches,
                        "type": level_type,
                        "_proximity_merged_from": len(group),
                        "_proximity_prices": [l["price"] for l in group],
                    }

                    # Log the proximity filter
                    self._log_filtered_levels(level_type, [l["price"] for l in group], merged_price)
                    self._progress_log(f"Proximity filtered {level_type}: {[smart_fmt(l['price']) for l in group]} → {smart_fmt(merged_price)} (touches={total_touches}, strength={min(5, total_strength)})")

                    new_working.append(merged_level)
                    merged_count += len(group) - 1

                i = j

            working = new_working

            # If no merges happened this round, we're done
            if not merged_this_round:
                break

            # If we've done too many iterations, break to avoid infinite loops
            if iterations > 100:
                self._progress_log(f"Proximity filter exceeded max iterations, stopping")
                break

        return working

    def update_levels(self, candles: List[dict], initializing: bool = False) -> None:
        """Main entry point for S/R level updates."""
        if len(candles) < self.lookback:
            self._progress_log(f"Not enough candles: {len(candles)} < {self.lookback}")
            return

        with self._lock:
            if initializing:
                # INITIAL MODE: Process the complete history from scratch
                self._progress_log(f"INITIAL SCAN STARTED | Candles={len(candles)}")
                self._reset_all_levels()
                self._processed_indices.clear()
                self._last_processed_index = -1

                self._run_initial_scan(candles)

                # Mark all historical candles as processed
                for i in range(len(candles)):
                    self._processed_indices.add(i)
                self._last_processed_index = len(candles) - 1

                # Merge levels after initial scan (local proximity)
                self.resistance_levels = self._merge_levels(self.resistance_levels, "RESISTANCE")
                self.support_levels = self._merge_levels(self.support_levels, "SUPPORT")

                # RECLASSIFY: Fix classification based on current price
                current_price = candles[-1]["close"] if candles else 0
                if current_price > 0:
                    self._reclassify_levels_by_price(current_price)

                # Apply final minimum-distance filter iteratively
                self._progress_log(f"Final proximity filter | Threshold={self.min_distance_percent}%")
                before_r = len(self.resistance_levels)
                before_s = len(self.support_levels)

                self.resistance_levels = self._apply_final_proximity_filter(self.resistance_levels, "RESISTANCE")
                self.support_levels = self._apply_final_proximity_filter(self.support_levels, "SUPPORT")

                after_r = len(self.resistance_levels)
                after_s = len(self.support_levels)

                if before_r != after_r:
                    self._progress_log(f"RESISTANCE filtered: {before_r} → {after_r}")
                if before_s != after_s:
                    self._progress_log(f"SUPPORT filtered: {before_s} → {after_s}")

                # Log final levels
                self._log_final_levels()
            else:
                # LIVE MODE: Process only newly closed candles
                current_idx = len(candles) - 1

                # Find unprocessed indices
                unprocessed = []
                for i in range(self._last_processed_index + 1, current_idx + 1):
                    if i not in self._processed_indices:
                        unprocessed.append(i)

                if not unprocessed:
                    self._progress_log("No new candles to process")
                    return

                self._progress_log(f"LIVE INCREMENTAL UPDATE | New Candles={len(unprocessed)} | Range={unprocessed[0]}→{unprocessed[-1]}")

                # Process each unprocessed candle
                for idx in unprocessed:
                    # Need enough context for swing detection
                    context_start = max(0, idx - SR_SWING_SENSITIVITY - 1)
                    context_end = min(len(candles), idx + SR_SWING_SENSITIVITY + 2)
                    context = candles[context_start:context_end]

                    self._process_candle_at_index(idx, context, candles)

                    # Mark as processed
                    self._processed_indices.add(idx)
                    self._last_processed_index = max(self._last_processed_index, idx)

                # Age all levels by 1 for each new candle
                self._age_levels()

                # Update strength for levels near current price
                if candles:
                    self._update_level_strength(candles)

                # Merge levels after live update (local proximity)
                self.resistance_levels = self._merge_levels(self.resistance_levels, "RESISTANCE")
                self.support_levels = self._merge_levels(self.support_levels, "SUPPORT")

                # RECLASSIFY: Fix classification based on current price
                current_price = candles[-1]["close"] if candles else 0
                if current_price > 0:
                    self._reclassify_levels_by_price(current_price)

                # Apply final minimum-distance filter iteratively on live updates
                before_r = len(self.resistance_levels)
                before_s = len(self.support_levels)

                self.resistance_levels = self._apply_final_proximity_filter(self.resistance_levels, "RESISTANCE")
                self.support_levels = self._apply_final_proximity_filter(self.support_levels, "SUPPORT")

                after_r = len(self.resistance_levels)
                after_s = len(self.support_levels)

                if before_r != after_r or before_s != after_s:
                    self._progress_log(f"Live update filtered | RESISTANCE: {before_r}→{after_r} | SUPPORT: {before_s}→{after_s}")

                # Log final levels
                self._log_final_levels()

    def _reset_all_levels(self) -> None:
        """Reset all level data for a fresh initial scan."""
        self.resistance_levels.clear()
        self.support_levels.clear()
        self.pending_swing_highs.clear()
        self.pending_swing_lows.clear()

    def _run_initial_scan(self, candles: List[dict]) -> None:
        """Perform a complete historical scan for S/R levels."""
        n = len(candles)
        sensitivity = SR_SWING_SENSITIVITY

        self._progress_log(f"Initial scan over {n} candles with sensitivity={sensitivity}")

        # Detect all swing points in the entire historical dataset
        for i in range(sensitivity, n - sensitivity):
            # Check for swing high
            is_high = True
            for j in range(1, sensitivity + 1):
                if candles[i]["high"] <= candles[i - j]["high"] or candles[i]["high"] <= candles[i + j]["high"]:
                    is_high = False
                    break
            if is_high:
                price = candles[i]["high"]
                # Check if this level already exists (prevent duplicates)
                if not self._level_exists(self.resistance_levels, price):
                    new_level = {
                        "price": price,
                        "age": 0,
                        "strength": 1,
                        "touches": 1,
                        "type": "RESISTANCE",
                        "index": i
                    }
                    self.resistance_levels.append(new_level)
                    self.pending_swing_highs.append(price)
                    self._log_new_level("RESISTANCE", price)
                    self._progress_log(f"New RESISTANCE at {smart_fmt(price)} (index {i})")

            # Check for swing low
            is_low = True
            for j in range(1, sensitivity + 1):
                if candles[i]["low"] >= candles[i - j]["low"] or candles[i]["low"] >= candles[i + j]["low"]:
                    is_low = False
                    break
            if is_low:
                price = candles[i]["low"]
                if not self._level_exists(self.support_levels, price):
                    new_level = {
                        "price": price,
                        "age": 0,
                        "strength": 1,
                        "touches": 1,
                        "type": "SUPPORT",
                        "index": i
                    }
                    self.support_levels.append(new_level)
                    self.pending_swing_lows.append(price)
                    self._log_new_level("SUPPORT", price)
                    self._progress_log(f"New SUPPORT at {smart_fmt(price)} (index {i})")

        # Update strength and touches based on price proximity
        self._update_level_strength(candles)

        self._progress_log(f"Initial scan complete: {len(self.support_levels)} supports, {len(self.resistance_levels)} resistances")

    def _process_candle_at_index(self, idx: int, context: List[dict], full_candles: List[dict]) -> None:
        """Process a single candle for S/R detection in live mode."""
        if len(context) < SR_SWING_SENSITIVITY * 2 + 1:
            return

        sensitivity = SR_SWING_SENSITIVITY
        # The candle we're checking is at position sensitivity in the context
        check_pos = sensitivity

        # Check for swing high
        is_high = True
        for j in range(1, sensitivity + 1):
            if (check_pos - j < 0 or check_pos + j >= len(context)):
                is_high = False
                break
            if context[check_pos]["high"] <= context[check_pos - j]["high"] or context[check_pos]["high"] <= context[check_pos + j]["high"]:
                is_high = False
                break
        if is_high:
            price = context[check_pos]["high"]
            if not self._level_exists(self.resistance_levels, price):
                new_level = {
                    "price": price,
                    "age": 0,
                    "strength": 1,
                    "touches": 1,
                    "type": "RESISTANCE"
                }
                self.resistance_levels.append(new_level)
                self.pending_swing_highs.append(price)
                self._log_new_level("RESISTANCE", price)
                self._progress_log(f"New RESISTANCE at {smart_fmt(price)}")

        # Check for swing low
        is_low = True
        for j in range(1, sensitivity + 1):
            if (check_pos - j < 0 or check_pos + j >= len(context)):
                is_low = False
                break
            if context[check_pos]["low"] >= context[check_pos - j]["low"] or context[check_pos]["low"] >= context[check_pos + j]["low"]:
                is_low = False
                break
        if is_low:
            price = context[check_pos]["low"]
            if not self._level_exists(self.support_levels, price):
                new_level = {
                    "price": price,
                    "age": 0,
                    "strength": 1,
                    "touches": 1,
                    "type": "SUPPORT"
                }
                self.support_levels.append(new_level)
                self.pending_swing_lows.append(price)
                self._log_new_level("SUPPORT", price)
                self._progress_log(f"New SUPPORT at {smart_fmt(price)}")

    def _level_exists(self, levels: List[Dict], price: float, tolerance: float = 0.002) -> bool:
        """Check if a level with this price already exists."""
        for level in levels:
            if abs(level["price"] - price) / max(price, 1) < tolerance:
                return True
        return False

    def _update_level_strength(self, candles: List[dict]) -> None:
        """Update strength and touches for all levels based on price proximity."""
        if not candles:
            return
        current_price = candles[-1]["close"]
        threshold = current_price * SR_PRICE_TOUCH_THRESHOLD

        # Update resistance levels
        for level in self.resistance_levels:
            if abs(level["price"] - current_price) <= threshold:
                level["strength"] = min(5, level["strength"] + 1)
                level["touches"] += 1
                level["age"] = 0
                self._progress_log(f"RESISTANCE touched: {smart_fmt(level['price'])} {'*' * level['strength']}")

        # Update support levels
        for level in self.support_levels:
            if abs(level["price"] - current_price) <= threshold:
                level["strength"] = min(5, level["strength"] + 1)
                level["touches"] += 1
                level["age"] = 0
                self._progress_log(f"SUPPORT touched: {smart_fmt(level['price'])} {'*' * level['strength']}")

    def _age_levels(self) -> List[Dict]:
        """Age all levels by 1 and remove expired ones."""
        expired_levels = []

        # Age resistance levels
        new_resistances = []
        for level in self.resistance_levels:
            effective_max_age = self.max_age + (level["strength"] * 15)
            level["age"] += 1
            if level["age"] <= effective_max_age:
                new_resistances.append(level)
            else:
                expired_levels.append(level)
                self._progress_log(f"RESISTANCE expired: {smart_fmt(level['price'])} (age {level['age']}/{effective_max_age})")
        self.resistance_levels = new_resistances

        # Age support levels
        new_supports = []
        for level in self.support_levels:
            effective_max_age = self.max_age + (level["strength"] * 15)
            level["age"] += 1
            if level["age"] <= effective_max_age:
                new_supports.append(level)
            else:
                expired_levels.append(level)
                self._progress_log(f"SUPPORT expired: {smart_fmt(level['price'])} (age {level['age']}/{effective_max_age})")
        self.support_levels = new_supports

        return expired_levels

    def _check_broken_levels(self, candles: List[dict], initializing: bool = False) -> None:
        """Check for broken levels and handle replacements."""
        if len(candles) < 3:
            return

        break_candle = candles[-3] if len(candles) >= 3 else None
        confirm_candle = candles[-2] if len(candles) >= 2 else None

        if not break_candle or not confirm_candle:
            return

        # Check resistance levels
        for i, level in enumerate(self.resistance_levels):
            resistance_price = level["price"]
            if break_candle["close"] > resistance_price:
                confirm_failed = (
                    not is_bullish(confirm_candle) or
                    confirm_candle["close"] <= break_candle["close"]
                )
                if confirm_failed:
                    old_resistance = self.resistance_levels.pop(i)
                    new_price = break_candle["high"]
                    new_level = {
                        "price": new_price,
                        "age": 0,
                        "strength": max(1, old_resistance.get("strength", 1)),
                        "touches": old_resistance.get("touches", 0) + 1,
                        "type": "RESISTANCE",
                        "old_price": old_resistance["price"],
                        "new_price": new_price,
                        "break_price": break_candle["close"],
                        "candle_type": "BREAKOUT"
                    }
                    self.resistance_levels.append(new_level)
                    _log("info", f"S/R [{self.symbol}]",
                         f"RESISTANCE REPLACED: {smart_fmt(old_resistance['price'])} -> {smart_fmt(new_price)}")
                    if self.notifier and not initializing:
                        self.notifier.send_sr_level_event(
                            symbol=self.symbol,
                            event_type="REPLACED",
                            level_data=new_level,
                            all_supports=self.support_levels,
                            all_resistances=self.resistance_levels
                        )
                    break

        # Check support levels
        for i, level in enumerate(self.support_levels):
            support_price = level["price"]
            if break_candle["close"] < support_price:
                confirm_failed = (
                    not is_bearish(confirm_candle) or
                    confirm_candle["close"] >= break_candle["close"]
                )
                if confirm_failed:
                    old_support = self.support_levels.pop(i)
                    new_price = break_candle["low"]
                    new_level = {
                        "price": new_price,
                        "age": 0,
                        "strength": max(1, old_support.get("strength", 1)),
                        "touches": old_support.get("touches", 0) + 1,
                        "type": "SUPPORT",
                        "old_price": old_support["price"],
                        "new_price": new_price,
                        "break_price": break_candle["close"],
                        "candle_type": "BREAKDOWN"
                    }
                    self.support_levels.append(new_level)
                    _log("info", f"S/R [{self.symbol}]",
                         f"SUPPORT REPLACED: {smart_fmt(old_support['price'])} -> {smart_fmt(new_price)}")
                    if self.notifier and not initializing:
                        self.notifier.send_sr_level_event(
                            symbol=self.symbol,
                            event_type="REPLACED",
                            level_data=new_level,
                            all_supports=self.support_levels,
                            all_resistances=self.resistance_levels
                        )
                    break

    def get_relevant_levels(self, current_price: float) -> Tuple[List[Dict], List[Dict]]:
        with self._lock:
            price_range = current_price * 0.10
            supports = [l for l in self.support_levels
                        if abs(l["price"] - current_price) <= price_range
                        and l["strength"] >= self.min_strength]
            resistances = [l for l in self.resistance_levels
                           if abs(l["price"] - current_price) <= price_range
                           and l["strength"] >= self.min_strength]
            return supports, resistances

    def get_levels_near_price(self, current_price: float, tolerance: float = 0.02) -> Tuple[List[Dict], List[Dict]]:
        with self._lock:
            threshold = current_price * tolerance
            supports = [l for l in self.support_levels
                        if abs(l["price"] - current_price) <= threshold
                        and l["strength"] >= self.min_strength]
            resistances = [l for l in self.resistance_levels
                           if abs(l["price"] - current_price) <= threshold
                           and l["strength"] >= self.min_strength]
            return supports, resistances

    def reset(self) -> None:
        with self._lock:
            self.resistance_levels.clear()
            self.support_levels.clear()
            self.pending_swing_highs.clear()
            self.pending_swing_lows.clear()
            self._processed_indices.clear()
            self._last_processed_index = -1
            _log("info", f"S/R [{self.symbol}]", "Levels reset")


# ================================================================
#  15. SYMBOL VALIDATOR
# ================================================================

class SymbolValidator:
    def __init__(self, product_map: Dict[str, int]):
        self.product_map = product_map
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
#  16. TIME-RANGE HELPER
# ================================================================

def get_time_range(num_candles: int, timeframe_minutes: int) -> Tuple[int, int]:
    secs_per_candle = timeframe_minutes * 60
    now = int(time.time())
    aligned_now = (now // secs_per_candle) * secs_per_candle
    safe_end = aligned_now - (CANDLE_SAFETY_SHIFT * secs_per_candle)
    start = safe_end - (num_candles * secs_per_candle)
    start = max(1, start)
    safe_end = max(secs_per_candle * (CANDLE_SAFETY_SHIFT + 1), safe_end)
    if start >= safe_end:
        raise ValueError(f"start={start} >= end={safe_end}")
    return start, safe_end


def get_time_range_with_retry_shift(
    num_candles: int, timeframe_minutes: int, shift_candles: int = 0,
) -> Tuple[int, int]:
    start, end = get_time_range(num_candles, timeframe_minutes)
    if shift_candles > 0:
        secs = shift_candles * timeframe_minutes * 60
        start = max(1, start - secs)
        end = max(timeframe_minutes * 60, end - secs)
    return start, end


# ================================================================
#  17. CANDLE PARSING HELPERS
# ================================================================

def _extract_timestamp(src: dict):
    for key in ("start", "time", "open_time", "t", "timestamp"):
        v = src.get(key)
        if v is not None:
            return v
    return None


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
    ts = normalize_timestamp_to_seconds(_extract_timestamp(row))
    if ts is None:
        return None
    o = _extract_price(row, "open", "o")
    h = _extract_price(row, "high", "h")
    l = _extract_price(row, "low", "l")
    c = _extract_price(row, "close", "c")
    v = _extract_price(row, "volume", "v") or 0.0
    if any(x is None for x in (o, h, l, c)):
        return None
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


# ================================================================
#  18. TICK SIZE ROUNDING
# ================================================================

def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 8)
    rounded = round(price / tick_size) * tick_size
    tick_str = f"{tick_size:.10f}".rstrip("0")
    dp = len(tick_str.split(".")[-1]) if "." in tick_str else 0
    return round(rounded, max(dp, 2))


# ================================================================
#  19. DELTA REST CLIENT
# ================================================================

class DeltaREST:
    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret
        self.request_handler = APIRequestHandler(api_key, api_secret)
        self._tick_sizes: Dict[str, float] = {}

    def verify_account(self) -> Optional[dict]:
        result = self.request_handler.request("GET", "/v2/profile", endpoint_type="private")
        if result and "result" in result:
            profile = result["result"]
            _log("info", "AUTH", f"Authenticated: {profile.get('email', '?')}")
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
                sym = item.get("symbol", "")
                pid = item.get("id")
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
            state = order.get("state", "")
            size = int(order.get("size", 0))
            unfilled_size = int(order.get("unfilled_size", 0))
            filled_size = size - unfilled_size
            if state == "closed" and unfilled_size == 0:
                return True, filled_size
            if state in ("cancelled", "rejected"):
                return False, 0
            time.sleep(FILL_POLL_INTERVAL)
        return False, 0

    def place_order(self, product_id: int, side: str, size: int,
                     order_type: str = "market_order",
                     limit_price: Optional[float] = None) -> dict:
        if side not in ("buy", "sell"):
            return {"error": "invalid_side"}
        if size < 1:
            return {"error": "invalid_size"}
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
        tick_size = self.get_tick_size(symbol) if symbol else 0.01
        rounded_tp = round_to_tick(tp_price, tick_size)
        body: Dict = {
            "product_id": product_id,
            "take_profit_order": {
                "order_type": "limit_order",
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
                _log("info", "BRACKET-CANCEL", f"Bracket TP cancelled for product_id={product_id}")
                return True
        except Exception as e:
            _log_exc("BRACKET-CANCEL", f"Failed to cancel bracket TP: {e}")
        return False

    def cancel_order(self, order_id: int, product_id: int) -> dict:
        body = {"id": order_id, "product_id": product_id}
        result = self.request_handler.request(
            "DELETE", "/v2/orders", endpoint_type="private", body=body
        )
        return result or {"error": "no_response"}

    def get_top_symbols(self, product_map: Dict[str, int], mode: str = "volatile",
                         limit: int = 5, perp_only: bool = True) -> List[str]:
        result = self.request_handler.request("GET", "/v2/tickers", endpoint_type="public")
        tickers = result.get("result", []) if result else []
        ranked: List[Tuple[float, str]] = []
        for t in tickers:
            sym = t.get("symbol", "")
            if perp_only and "_PERP" not in sym:
                continue
            if sym not in product_map:
                continue
            try:
                score = (abs(float(t.get("change", 0) or 0)) if mode == "volatile"
                         else float(t.get("volume", 0) or 0))
                ranked.append((score, sym))
            except (TypeError, ValueError):
                continue
        ranked.sort(reverse=True)
        return [s for _, s in ranked[:limit]]

    def get_candles_with_retry(self, symbol: str, resolution: str = "1h",
                                limit: int = CANDLE_LIMIT) -> List[dict]:
        candles = self.get_candles(symbol, resolution, limit, shift_candles=0)
        if not candles:
            for shift in range(1, MAX_RETRIES + 1):
                time.sleep(RETRY_DELAYS[shift - 1])
                candles = self.get_candles(symbol, resolution, limit, shift_candles=shift)
                if candles:
                    break
        return candles

    def get_candles(self, symbol: str, resolution: str = "1h",
                     limit: int = CANDLE_LIMIT, shift_candles: int = 0) -> List[dict]:
        candle_symbol = to_candle_symbol(symbol)
        tf_entry = next(
            (tf for tf in TIMEFRAME_MAP.values()
             if tf["api_resolution"] == resolution or tf["resolution"] == resolution),
            TIMEFRAME_MAP["1h"],
        )
        timeframe_minutes = tf_entry["secs"] // 60
        api_resolution = tf_entry["api_resolution"]
        safe_limit = min(limit, 500)
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
        if not result:
            return []
        try:
            raw_result = result.get("result", [])
            if isinstance(raw_result, dict):
                for key in ("candles", "data", "ohlcv"):
                    if key in raw_result:
                        raw_result = raw_result[key]
                        break
            if not isinstance(raw_result, list) or not raw_result:
                return []
            candles = []
            for row in raw_result:
                candle = _parse_rest_candle_row(row, candle_symbol)
                if candle and validate_candle(candle, candle_symbol):
                    candles.append(candle)
            dedup: Dict[int, dict] = {}
            for c in candles:
                dedup[c["time"]] = c
            candles = sorted(dedup.values(), key=lambda x: x["time"])
            _log("info", "CANDLES", f"Loaded {len(candles)} candles for {candle_symbol}")
            return candles
        except Exception as e:
            _log_exc("CANDLES", f"Failed to parse candle response for {candle_symbol}: {e}")
            return []

    def set_leverage(self, product_id: int, leverage: int) -> bool:
        body = {"leverage": str(leverage)}
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
                        _log("info", "PNL", f"Fetched realized PnL for product_id={product_id}: ${realized_pnl:.2f}")
                        return realized_pnl
                _log("warning", "PNL", f"No position found for product_id={product_id}")
        except Exception as e:
            _log_exc("PNL", f"Error fetching realized PnL: {e}")
        return 0.0

    def get_order_realized_pnl(self, order_id: int) -> float:
        try:
            result = self.request_handler.request(
                "GET", f"/v2/orders/{order_id}", endpoint_type="private"
            )
            if result and "result" in result:
                order = result["result"]
                realized_pnl = float(order.get("realized_pnl", 0))
                _log("info", "PNL", f"Fetched realized PnL for order_id={order_id}: ${realized_pnl:.2f}")
                return realized_pnl
        except Exception as e:
            _log_exc("PNL", f"Error fetching order realized PnL: {e}")
        return 0.0


# ================================================================
#  20. POSITION SIZER
# ================================================================

def compute_position_size(entry_price: float, stop_loss_price: float,
                           account_balance: float, risk_pct: float,
                           leverage: int) -> Tuple[int, dict]:
    risk_amount = account_balance * risk_pct
    stop_distance = abs(entry_price - stop_loss_price)
    if stop_distance <= 0:
        return 0, {}
    risk_size = risk_amount / stop_distance
    max_by_margin = (account_balance * leverage) / entry_price
    final_size_raw = min(risk_size, max_by_margin)
    final_size = max(1, int(final_size_raw))
    margin_used = (final_size * entry_price) / leverage
    max_loss_est = final_size * stop_distance
    diag = {
        "account_balance": round(account_balance, 2),
        "risk_pct": round(risk_pct * 100, 2),
        "risk_amount": round(risk_amount, 2),
        "entry_price": entry_price,
        "stop_loss_price": stop_loss_price,
        "stop_distance": stop_distance,
        "risk_size_raw": round(risk_size, 6),
        "max_by_margin": round(max_by_margin, 6),
        "final_size": final_size,
        "margin_used": round(margin_used, 2),
        "max_loss_est": round(max_loss_est, 2),
        "leverage": leverage,
    }
    return final_size, diag


def compute_take_profit(entry_price: float, stop_loss_price: float,
                         direction: str = "SHORT") -> float:
    stop_distance = abs(entry_price - stop_loss_price)
    raw_tp_distance = stop_distance * TP_RR_RATIO
    max_tp_distance = entry_price * TP_MAX_PCT
    tp_distance = min(raw_tp_distance, max_tp_distance)
    if direction == "SHORT":
        tp = entry_price - tp_distance
    else:
        tp = entry_price + tp_distance
    return tp


# ================================================================
#  21. DAILY LOSS TRACKER
# ================================================================

class DailyLossTracker:
    def __init__(self, trading_capital: float, limit_pct: float,
                 notifier: Optional[GmailNotifier] = None):
        self.trading_capital = trading_capital
        self.limit_pct = limit_pct
        self.daily_loss_usd = 0.0
        self._day = datetime.now(timezone.utc).date()
        self._lock = threading.Lock()
        self.notifier = notifier

    def _check_day_rollover(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self._day:
            _log("info", "DAILY-LOSS", f"New day {today} - resetting daily loss counter")
            self.daily_loss_usd = 0.0
            self._day = today

    def update_with_realized_pnl(self, realized_pnl: float) -> None:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            if realized_pnl < 0:
                loss_amount = abs(realized_pnl)
                self.daily_loss_usd += loss_amount
                _log("warning", "DAILY-LOSS",
                     f"Realized loss: ${loss_amount:.2f} | Daily loss total: ${self.daily_loss_usd:.2f} / ${limit:.2f}")
            else:
                profit_amount = realized_pnl
                self.daily_loss_usd = max(0.0, self.daily_loss_usd - profit_amount)
                _log("info", "DAILY-LOSS",
                     f"Realized profit: ${profit_amount:.2f} | Daily loss total reduced to: ${self.daily_loss_usd:.2f} / ${limit:.2f}")
            if limit > 0 and self.notifier and self.daily_loss_usd >= limit * 0.8:
                self.notifier.send_daily_loss_warning(self.daily_loss_usd, limit)
            if limit > 0 and self.notifier and self.daily_loss_usd >= limit:
                self.notifier.send_daily_limit_hit(self.daily_loss_usd, limit)

    def is_limit_reached(self) -> bool:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            if limit <= 0:
                return False
            if self.daily_loss_usd >= limit:
                _log("error", "DAILY-LOSS", f"DAILY LOSS LIMIT REACHED: ${self.daily_loss_usd:.2f} >= ${limit:.2f}")
                return True
            return False

    def status(self) -> str:
        with self._lock:
            self._check_day_rollover()
            limit = self.trading_capital * self.limit_pct
            pct = (self.daily_loss_usd / limit * 100) if limit > 0 else 0
            return f"Daily loss: ${self.daily_loss_usd:.2f} / ${limit:.2f} ({pct:.1f}%)"


# ================================================================
#  22. TRADING BOT
# ================================================================

class TradingBot:
    def __init__(self, config: dict, notifier: Optional[GmailNotifier] = None):
        self.config = config
        self.notifier = notifier
        self.paper = config["paper_mode"]
        self.leverage = config["leverage"]
        self.max_trades = config.get("max_concurrent_trades", 2)
        self.api_key = config.get("api_key", "")
        self.api_secret = config.get("api_secret", "")
        self.trading_capital = float(config.get("trading_capital", 0.0))
        self.risk_pct = config["risk_pct"] / 100.0
        self.daily_loss_limit_pct = config.get("daily_loss_limit_pct", DAILY_LOSS_LIMIT_PCT)
        self.enable_short = config.get("enable_short", True)
        self.enable_long = config.get("enable_long", True)
        self.harami_tolerance = config.get("harami_tolerance", HARAMI_BODY_TOLERANCE)

        if self.trading_capital <= 0:
            _log("error", "STARTUP",
                 "trading_capital is 0 or missing in config - the bot will not be "
                 "able to size any trade and will be halted at start(). Fix your config.")

        self._tf = TimeframeSafe(config.get("timeframe", "1h"))
        self.timeframe = self._tf.key
        self.resolution = self._tf.resolution
        self.api_resolution = self._tf.api_resolution
        self.ws_channel = self._tf.ws_channel

        self.rest = DeltaREST(self.api_key, self.api_secret)

        self.symbols: List[str] = []
        self.product_map: Dict[str, int] = {}
        self.candle_store: Dict[str, deque] = {}

        self._forming_candle: Dict[str, Optional[dict]] = {}
        self._last_closed_time: Dict[str, int] = {}
        self._candle_locks: Dict[str, threading.Lock] = {}

        self._trade_lock = threading.Lock()
        self.active_trades: Dict[str, dict] = {}

        self.signals: List[dict] = []
        self.sl_events: List[dict] = []
        self.tp_events: List[dict] = []

        self.daily_loss_tracker = DailyLossTracker(
            trading_capital=self.trading_capital,
            limit_pct=self.daily_loss_limit_pct,
            notifier=notifier,
        )

        self.running = False
        self.ws_manager: Optional[DeltaWebSocket] = None
        self.sr_managers: Dict[str, SRLevelManager] = {}

        self._start_time: float = time.time()
        self.last_candle_time: Dict[str, float] = {}
        self.last_eval_time: Dict[str, float] = {}
        self.last_ws_update_time: Dict[str, float] = {}
        self._active_watchdog_issues: Dict[str, float] = {}
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()

        self._symbol_error_counts: Dict[str, int] = {}

        self._recovery_state_lock = threading.Lock()
        self._recovery_in_progress = False
        self._last_recovery_attempt: float = 0.0
        self._recovery_issue_baseline: Dict[str, Tuple[int, Optional[int]]] = {}

        self._ws_connected_event = threading.Event()
        self._ws_subscribed_event = threading.Event()
        self._first_live_data_event = threading.Event()
        self._pipeline_ready = threading.Event()
        self._symbols_with_live_data: set = set()
        self._readiness_lock = threading.Lock()

        self.on_signal_callback = None
        self.on_trade_callback = None
        self.on_log_callback = None

    def _log(self, level: str, tag: str, msg: str) -> None:
        _log(level, tag, msg)
        if self.on_log_callback:
            try:
                self.on_log_callback(f"[{tag}] {msg}", level)
            except Exception as e:
                _log_exc("LOG-CALLBACK", f"on_log_callback raised: {e}")

    def _get_realized_pnl_for_trade(self, trade: dict,
                                     max_retries: int = 5,
                                     retry_delay: float = 1.0) -> float:
        if self.paper:
            exit_price = trade.get("exit_price")
            entry = trade.get("entry")
            size = trade.get("size", 0)
            direction = trade.get("direction", "SHORT")
            if exit_price and entry and size > 0:
                return (entry - exit_price) * size if direction == "SHORT" \
                    else (exit_price - entry) * size
            return 0.0

        order_id = trade.get("order_id")
        product_id = trade.get("product_id")

        if order_id:
            for attempt in range(1, max_retries + 1):
                pnl = self.rest.get_order_realized_pnl(order_id)
                if pnl != 0:
                    _log("info", "PNL", f"Retrieved realized PnL from order_id {order_id} (attempt {attempt}): ${pnl:.2f}")
                    return pnl
                if attempt < max_retries:
                    time.sleep(retry_delay)
            _log("warning", "PNL", f"Could not fetch realized PnL from order_id {order_id} after {max_retries} attempts")

        if product_id:
            for attempt in range(1, max_retries + 1):
                pnl = self.rest.get_position_realized_pnl(product_id)
                if pnl != 0:
                    _log("info", "PNL", f"Retrieved realized PnL from product_id {product_id} (attempt {attempt}): ${pnl:.2f}")
                    return pnl
                if attempt < max_retries:
                    time.sleep(retry_delay)
            _log("warning", "PNL", f"Could not fetch realized PnL from product_id {product_id} after {max_retries} attempts")

        return 0.0

    def _close_trade(self, symbol: str, trade: dict, reason: str,
                      exit_price: Optional[float] = None) -> None:
        try:
            entry = trade["entry"]
            direction = trade.get("direction", "SHORT")

            _log("info", "TRADE-CLOSE", f"Closing trade: {symbol} {direction} | Reason: {reason}")

            if not self.paper:
                pid = trade.get("product_id")
                size = trade.get("size", 1)
                if pid and size > 0:
                    close_side = "buy" if direction == "SHORT" else "sell"
                    self.rest.close_position(pid, size, side=close_side)
                    _log("info", "TRADE-CLOSE", f"Closed position on Delta: product_id={pid}, size={size}, side={close_side}")
                    time.sleep(1)

            realized_pnl = self._get_realized_pnl_for_trade(trade)

            trade["close_reason"] = reason
            trade["close_time"] = datetime.now(timezone.utc).isoformat()
            trade["realized_pnl"] = realized_pnl

            if reason in ("TAKE_PROFIT", "ST_EXIT"):
                tp = exit_price or trade.get("take_profit")
                trade["exit_price"] = tp
                self.tp_events.append({
                    "time": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol, "entry": entry,
                    "take_profit": tp, "direction": direction,
                    "realized_pnl": realized_pnl, "reason": reason,
                })
            else:
                sl = trade["stop_loss"]
                trade["exit_price"] = sl
                self.sl_events.append({
                    "time": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol, "entry": entry,
                    "stop_loss": sl, "direction": direction,
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
                try:
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
                except Exception as e:
                    _log_exc("TRADE-CLOSE", f"Notifier error while closing {symbol} (trade still closed correctly): {e}")
        except Exception as e:
            _log_exc("TRADE-CLOSE", f"Unexpected error while closing {symbol} - attempting cleanup anyway: {e}")
        finally:
            self._cleanup_trade(symbol)

    def _cleanup_trade(self, symbol: str) -> None:
        with self._trade_lock:
            self.active_trades.pop(symbol, None)
        self._log("info", "CLEANUP", f"Trade record removed for {symbol}")

    def _check_supertrend_conditions(self, symbol: str, closed_candles: List[dict]) -> None:
        trade = self.active_trades.get(symbol)
        if not trade or "_reserved" in trade:
            return

        direction = trade.get("direction", "SHORT")
        entry = trade.get("entry", 0)
        st_mode = trade.get("st_mode", False)

        if len(closed_candles) < ST2_LENGTH + 2:
            return

        bearish_now = both_supertrends_bearish(closed_candles)
        bullish_now = both_supertrends_bullish(closed_candles)

        if not st_mode:
            confirmed = (
                (direction == "SHORT" and bearish_now) or
                (direction == "LONG" and bullish_now)
            )
            if confirmed:
                st1_val, st2_val = get_supertrend_values(closed_candles)
                trade["st_mode"] = True
                trade["st_confirmed_at"] = datetime.now(timezone.utc).isoformat()

                if not self.paper:
                    pid = trade.get("product_id")
                    if pid:
                        self.rest.cancel_bracket_tp(pid)

                trend_label = "STRONG DOWN TREND" if direction == "SHORT" else "STRONG UP TREND"
                print()
                print(f"  [SUPERTREND CONFIRMED] {symbol} {trend_label}")
                print(f"    Both ST(14,2) + ST(21,1) {'RED' if direction == 'SHORT' else 'GREEN'}")
                print(f"    Entry        : {smart_fmt(entry)}")
                print(f"    ST(14,2) val : {smart_fmt(st1_val) if st1_val else 'N/A'}")
                print(f"    ST(21,1) val : {smart_fmt(st2_val) if st2_val else 'N/A'}")
                print(f"    TP CHANGED -> Will exit when BOTH SuperTrends reverse direction")
                print()

                _log("info", "ST-CONFIRM", f"[{symbol}] {direction} trade - Both SuperTrends confirmed.")

                if self.notifier:
                    self.notifier.send_supertrend_strong(
                        symbol=symbol, direction=direction, entry=entry,
                        new_tp=0.0, st1=st1_val or 0.0, st2=st2_val or 0.0,
                        timeframe=self.timeframe, mode="PAPER" if self.paper else "LIVE",
                    )

        else:
            reversed_trend = (
                (direction == "SHORT" and bullish_now) or
                (direction == "LONG" and bearish_now)
            )
            if reversed_trend:
                _log("info", "ST-EXIT", f"[{symbol}] {direction} - Both SuperTrends REVERSED.")
                print()
                print(f"  [ST REVERSAL EXIT] {symbol} {direction}")
                flip_label = "both GREEN" if direction == "SHORT" else "both RED"
                print(f"    SuperTrends flipped {flip_label} - closing position")
                print()
                self._close_trade(symbol, trade, "ST_EXIT")

    # ================================================================
    #  STARTUP SEQUENCE
    # ================================================================

    def start(self) -> None:
        self.running = True
        self._print_banner()

        self._log("info", "STARTUP", "STEP  1/15: Validating configuration...")
        if self.trading_capital <= 0:
            self._log("error", "STARTUP",
                       "Aborting: trading_capital must be > 0. The bot cannot size "
                       "trades or evaluate the daily loss limit with $0 capital.")
            self.running = False
            return

        self._log("info", "STARTUP", "STEP  2/15: Logging and error handling ready.")
        self._log("info", "STARTUP", "STEP  3/15: Initializing REST API client...")
        warm_up_connection()
        if not self.paper:
            profile = self.rest.verify_account()
            if profile is None:
                self._log("error", "STARTUP", "Authentication failed. Aborting.")
                self.running = False
                return

        self._log("info", "STARTUP", "STEP  4/15: Loading product catalogue and resolving symbols...")
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

        validator = SymbolValidator(self.product_map)
        self.symbols = validator.validate_list(raw_syms)
        if not self.symbols:
            self._log("error", "STARTUP", "No valid symbols. Aborting.")
            self.running = False
            return

        for sym in self.symbols:
            self.candle_store[sym] = deque(maxlen=500)
            self.sr_managers[sym] = SRLevelManager(symbol=sym, notifier=self.notifier)
            self._forming_candle[sym] = None
            self._last_closed_time[sym] = 0
            self._candle_locks[sym] = threading.Lock()

        if not self.paper:
            for sym in self.symbols:
                pid = self.product_map.get(sym)
                if pid:
                    self.rest.set_leverage(pid, self.leverage)

        self._print_startup_summary()

        if self.notifier:
            self.notifier.send_startup_report(self.config, self.symbols, self.harami_tolerance)

        self._log("info", "STARTUP", "STEP  5/15: Fetching historical OHLCV data for all symbols...")
        self._log("info", "STARTUP", "STEP  6/15: Storing historical closed candles...")
        self._fetch_historical_candles()

        self._log("info", "STARTUP", "STEP  7/15: Running initial Support/Resistance detection on historical data...")
        self._run_initial_sr_detection()

        self._log("info", "STARTUP", "STEP  8/15: Validating indicator/strategy data readiness...")
        self._validate_indicator_readiness()

        self._start_time = time.time()

        self._log("info", "STARTUP", "STEP  9/15: Initializing WebSocket manager...")
        self.ws_manager = DeltaWebSocket()
        self._wire_ws_callbacks(self.ws_manager)
        ws_symbols = [to_ws_symbol(sym) for sym in self.symbols]
        self.ws_manager.subscribe(self.timeframe, ws_symbols)

        self._log("info", "STARTUP", "STEP 10/15: Connecting WebSocket...")
        self.ws_manager.start()
        connected = self._ws_connected_event.wait(timeout=STARTUP_WS_CONNECT_TIMEOUT)
        if connected:
            self._log("info", "STARTUP", "WebSocket connection CONFIRMED.")
        else:
            self._log("warning", "STARTUP",
                       f"WebSocket did not confirm connection within {STARTUP_WS_CONNECT_TIMEOUT}s - "
                       f"continuing startup; the WS reconnect loop and the watchdog "
                       f"(started at the end of this sequence) will keep trying and will "
                       f"alert if the feed stays down.")

        self._log("info", "STARTUP", "STEP 11/15: Waiting for subscription confirmation from server...")
        subscribed = self._ws_subscribed_event.wait(timeout=STARTUP_SUBSCRIBE_CONFIRM_TIMEOUT)
        if subscribed:
            self._log("info", "STARTUP", "Subscription CONFIRMED by server.")
        else:
            self._log("warning", "STARTUP",
                       f"No explicit subscription confirmation received within "
                       f"{STARTUP_SUBSCRIBE_CONFIRM_TIMEOUT}s - some feeds push data "
                       f"without a separate ack message; continuing and relying on "
                       f"step 12's live-data check instead.")

        self._log("info", "STARTUP", "STEP 12/15: Waiting for first live candle data from all symbols...")
        got_live_data = self._first_live_data_event.wait(timeout=STARTUP_LIVE_DATA_TIMEOUT)
        if got_live_data:
            self._log("info", "STARTUP", "Live candle data CONFIRMED for all monitored symbols.")
        else:
            missing = [s for s in self.symbols if s not in self._symbols_with_live_data]
            self._log("warning", "STARTUP",
                       f"Live data not yet confirmed for all symbols within "
                       f"{STARTUP_LIVE_DATA_TIMEOUT}s "
                       f"({len(self._symbols_with_live_data)}/{len(self.symbols)} reporting, "
                       f"missing: {missing}) - proceeding anyway; the watchdog will "
                       f"monitor these symbols and trigger automatic recovery if the "
                       f"feed genuinely stays silent.")

        self._log("info", "STARTUP", "STEP 13/15: Forming-candle state is being populated by the live feed.")

        self._pipeline_ready.set()
        self._log("info", "STARTUP", "STEP 14/15: Data pipeline healthy - STRATEGY EVALUATION ENABLED.")

        self._log("info", "STARTUP", "STEP 15/15: Starting watchdog and background health monitoring...")
        self._start_watchdog()

        self._log("info", "STARTUP", "Startup sequence complete - bot is fully operational.")

    def stop(self) -> None:
        self.running = False
        self._watchdog_stop.set()
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5.0)
        if self.ws_manager:
            self.ws_manager.stop()
        self._log("info", "BOT", "Bot stopped.")

    def _fetch_historical_candles(self) -> None:
        self._log("info", "CANDLES", f"Loading {CANDLE_LIMIT} candles x {self.timeframe} for {len(self.symbols)} symbol(s)")
        tf_secs = self._tf.secs

        for sym in self.symbols:
            try:
                raw_candles = self.rest.get_candles_with_retry(sym, self.api_resolution, CANDLE_LIMIT)
                if not raw_candles:
                    self._log("warning", "CANDLES", f"  [WARN] {sym}: 0 candles after retries.")
                    continue

                normalized: List[dict] = []
                for row in raw_candles:
                    ts = normalize_timestamp_to_seconds(row.get("time"))
                    if ts is None:
                        continue
                    c = dict(row)
                    c["time"] = ts
                    if validate_candle(c, sym):
                        normalized.append(c)

                dedup: Dict[int, dict] = {}
                for c in normalized:
                    dedup[c["time"]] = c
                sorted_candles = sorted(dedup.values(), key=lambda x: x["time"])

                if not sorted_candles:
                    self._log("warning", "CANDLES", f"  [WARN] {sym}: no valid candles after normalization.")
                    continue

                now_ts = time.time()
                confirmed_closed = [
                    c for c in sorted_candles
                    if (c["time"] + tf_secs) <= now_ts
                ]

                if not confirmed_closed:
                    self._log("warning", "CANDLES", f"  [WARN] {sym}: no confirmed closed candles (all excluded as forming).")
                    continue

                for c in confirmed_closed:
                    self.candle_store[sym].append(c)

                self._last_closed_time[sym] = confirmed_closed[-1]["time"]
                self._log("info", "CANDLES",
                          f"  [OK] {sym}: {len(confirmed_closed)} confirmed closed candles loaded | "
                          f"last_close={smart_fmt(confirmed_closed[-1]['close'])} | t={confirmed_closed[-1]['time']}")

            except Exception as e:
                _log_exc("CANDLES", f"  [ERROR] {sym}: historical load failed, continuing with other symbols: {e}")

    def _run_initial_sr_detection(self) -> None:
        self._log("info", "S/R-INIT", f"Starting historical S/R detection for {len(self.symbols)} symbol(s)")

        for idx, sym in enumerate(self.symbols, start=1):
            store = self.candle_store.get(sym)
            sr_manager = self.sr_managers.get(sym)

            self._log("info", "S/R-INIT",
                      f"Processing progress: symbol {idx}/{len(self.symbols)} -> {sym}")

            if not store:
                self._log("info", "S/R-INIT", f"[{sym}] No historical candles stored - skipping initial S/R detection.")
                continue
            if not sr_manager:
                self._log("warning", "S/R-INIT", f"[{sym}] No S/R manager found for symbol - skipping historical S/R detection.")
                continue

            confirmed_closed = list(store)
            self._log("info", "S/R-INIT",
                      f"[{sym}] Started | Candles={len(confirmed_closed)}")

            try:
                sr_manager.update_levels(confirmed_closed, initializing=True)
            except Exception as e:
                _log_exc("S/R-INIT", f"[{sym}] historical S/R detection failed: {e}")

            with sr_manager._lock:
                support_snapshot = sorted(sr_manager.support_levels, key=lambda x: x["price"])
                resistance_snapshot = sorted(sr_manager.resistance_levels, key=lambda x: x["price"])

            if support_snapshot:
                support_str = ", ".join(
                    f"{smart_fmt(l['price'])} ({'*' * l['strength']}, touches={l['touches']})"
                    for l in support_snapshot[:5]
                )
                if len(support_snapshot) > 5:
                    support_str += f", ... and {len(support_snapshot) - 5} more"
                self._log("debug", "S/R-INIT", f"[{sym}] SUPPORT levels: {support_str}")

            if resistance_snapshot:
                resistance_str = ", ".join(
                    f"{smart_fmt(l['price'])} ({'*' * l['strength']}, touches={l['touches']})"
                    for l in resistance_snapshot[:5]
                )
                if len(resistance_snapshot) > 5:
                    resistance_str += f", ... and {len(resistance_snapshot) - 5} more"
                self._log("debug", "S/R-INIT", f"[{sym}] RESISTANCE levels: {resistance_str}")

        self._log("info", "S/R-INIT",
                  f"Completed successfully - historical S/R detection finished for all {len(self.symbols)} symbol(s)")

    def _validate_indicator_readiness(self) -> None:
        min_rsi = RSI_MIN_CANDLES
        min_st = ST2_LENGTH + 2
        for sym in self.symbols:
            have = len(self.candle_store.get(sym, []))
            rsi_ok = have >= min_rsi
            st_ok = have >= min_st
            if rsi_ok and st_ok:
                self._log("info", "INDICATOR-INIT",
                          f"[{sym}] {have} candles available - sufficient for RSI({RSI_PERIOD}) "
                          f"and both SuperTrends on first evaluation.")
            else:
                missing = []
                if not rsi_ok:
                    missing.append(f"RSI needs >= {min_rsi} (have {have})")
                if not st_ok:
                    missing.append(f"SuperTrend needs >= {min_st} (have {have})")
                self._log("warning", "INDICATOR-INIT",
                          f"[{sym}] insufficient historical candles for some indicators: "
                          f"{'; '.join(missing)}. The bot will keep running and these "
                          f"indicators will simply return 'not enough data' (safely "
                          f"blocking related signals) until enough live candles close.")

    def _wire_ws_callbacks(self, ws: DeltaWebSocket) -> None:
        ws.set_candle_callback(self._on_ws_candle)
        ws.set_connected_callback(self._on_ws_connected)
        ws.set_disconnected_callback(self._on_ws_disconnected)
        ws.set_error_callback(self._on_ws_error)
        ws.set_reconnect_callback(self._on_ws_reconnect)
        ws.set_subscribed_callback(self._on_ws_subscribed)

    def _on_ws_candle(self, symbol: str, candle: dict) -> None:
        resolved: Optional[str] = None
        if symbol in self.candle_store:
            resolved = symbol
        else:
            for known in self.candle_store:
                if to_ws_symbol(known) == to_ws_symbol(symbol):
                    resolved = known
                    break

        if resolved is None:
            self._log("warning", "PIPELINE",
                       f"WS candle for unrecognized symbol '{symbol}' ignored - no matching candle store. "
                       f"Known symbols: {list(self.candle_store.keys())}")
            return

        if resolved not in self._symbols_with_live_data:
            with self._readiness_lock:
                self._symbols_with_live_data.add(resolved)
                if len(self._symbols_with_live_data) >= len(self.symbols):
                    self._first_live_data_event.set()

        self.process_candle(resolved, candle)

    def _on_ws_connected(self) -> None:
        self._log("info", "WS", "WebSocket CONNECTED - live candle feed should now be active")
        self._ws_connected_event.set()

    def _on_ws_subscribed(self) -> None:
        self._log("info", "WS", "Subscription CONFIRMED by server (startup readiness signal)")
        self._ws_subscribed_event.set()

    def _on_ws_disconnected(self) -> None:
        self._log("error", "WS", "WebSocket DISCONNECTED after max reconnect attempts - live candle feed is DOWN")

    def _on_ws_error(self, err) -> None:
        self._log("error", "WS", f"WebSocket error: {err}")

    def _on_ws_reconnect(self) -> None:
        self._log("info", "PIPELINE", "WebSocket reconnected internally - backfilling any missed candles for all symbols")
        for sym in self.symbols:
            try:
                self._backfill_symbol(sym)
            except Exception as e:
                _log_exc("BACKFILL", f"[{sym}] backfill after reconnect failed, continuing with other symbols: {e}")

    def _backfill_symbol(self, symbol: str) -> None:
        store = self.candle_store.get(symbol)
        if store is None:
            return

        lock = self._candle_locks.setdefault(symbol, threading.Lock())
        with lock:
            last_closed_time = self._last_closed_time.get(symbol, 0)
            current_forming = self._forming_candle.get(symbol)

        fresh = self.rest.get_candles_with_retry(symbol, self.api_resolution, CANDLE_LIMIT)
        if not fresh:
            self._log("warning", "BACKFILL", f"[{symbol}] backfill fetch returned no candles")
            return

        normalized: List[dict] = []
        for row in fresh:
            nc = self._validate_and_normalize(row, symbol)
            if nc is not None:
                normalized.append(nc)

        dedup: Dict[int, dict] = {}
        for c in normalized:
            dedup[c["time"]] = c
        deduped = sorted(dedup.values(), key=lambda x: x["time"])

        tf_secs = self._tf.secs
        now = time.time()
        forming_time = current_forming["time"] if current_forming else None

        confirmed_closed: List[dict] = []
        for c in deduped:
            if c["time"] <= last_closed_time:
                continue
            if forming_time is not None and c["time"] >= forming_time:
                continue
            if (c["time"] + tf_secs) > now:
                continue
            confirmed_closed.append(c)

        if not confirmed_closed:
            self._log("info", "BACKFILL", f"[{symbol}] no missing closed candles - already up to date (last closed t={last_closed_time})")
            return

        self._log("warning", "BACKFILL",
                   f"[{symbol}] backfilling {len(confirmed_closed)} missed closed candle(s), "
                   f"t={confirmed_closed[0]['time']}..{confirmed_closed[-1]['time']} "
                   f"(excluding forming candle t={forming_time})")

        for i, closed in enumerate(confirmed_closed):
            with lock:
                current_last = self._last_closed_time.get(symbol, 0)
            if closed["time"] <= current_last:
                continue

            if i + 1 < len(confirmed_closed):
                next_candle = confirmed_closed[i + 1]
            else:
                with lock:
                    latest_forming = self._forming_candle.get(symbol)
                next_candle = latest_forming if latest_forming is not None else closed

            self._process_closed_candle(symbol, closed, next_candle, source="BACKFILL")

    def _validate_and_normalize(self, raw_candle: dict, symbol: str) -> Optional[dict]:
        try:
            ts = normalize_timestamp_to_seconds(raw_candle.get("time"))
            if ts is None:
                self._log("warning", "PIPELINE", f"[{symbol}] rejected candle: invalid/missing timestamp ({raw_candle.get('time')!r})")
                return None
            c = dict(raw_candle)
            c["time"] = ts
            if not validate_candle(c, symbol):
                self._log("warning", "PIPELINE", f"[{symbol}] rejected candle: failed OHLCV validation t={ts}")
                return None
            return c
        except Exception as e:
            _log_exc("PIPELINE", f"[{symbol}] error normalizing incoming candle: {e}")
            return None

    def process_candle(self, symbol: str, raw_candle: dict) -> None:
        store = self.candle_store.get(symbol)
        if store is None:
            return

        normalized = self._validate_and_normalize(raw_candle, symbol)
        if normalized is None:
            return

        self.last_ws_update_time[symbol] = time.time()

        self._log("debug", "PIPELINE", f"[{symbol}] Live message received | t={normalized['time']} close={smart_fmt(normalized['close'])}")

        lock = self._candle_locks.setdefault(symbol, threading.Lock())
        same_time_update = False
        closed_for_processing: Optional[dict] = None
        new_forming: Optional[dict] = None

        with lock:
            last_closed_time = self._last_closed_time.get(symbol, 0)

            if normalized["time"] < last_closed_time:
                self._log("warning", "PIPELINE", f"[{symbol}] ignoring out-of-order candle t={normalized['time']} (< last closed t={last_closed_time})")
                return
            if normalized["time"] == last_closed_time:
                self._log("debug", "PIPELINE", f"[{symbol}] ignoring late duplicate of already-closed candle t={normalized['time']}")
                return

            forming = self._forming_candle.get(symbol)

            if forming is None:
                self._forming_candle[symbol] = normalized
                self._log("info", "PIPELINE", f"[{symbol}] Forming candle initialized | t={normalized['time']}")
                return

            if normalized["time"] < forming["time"]:
                self._log("warning", "PIPELINE", f"[{symbol}] ignoring stale candle t={normalized['time']} (< forming t={forming['time']})")
                return

            if normalized["time"] == forming["time"]:
                self._forming_candle[symbol] = normalized
                same_time_update = True
                self._log("debug", "PIPELINE", f"[{symbol}] Forming candle updated | t={normalized['time']} close={smart_fmt(normalized['close'])}")
            else:
                closed_for_processing = forming
                new_forming = normalized
                self._forming_candle[symbol] = normalized
                self._log("info", "PIPELINE",
                          f"[{symbol}] New candle timestamp received (t={normalized['time']}) -> "
                          f"previous candle (t={closed_for_processing['time']}) is now CLOSED")

        if same_time_update:
            self._check_take_profit(symbol, normalized)
            return

        if closed_for_processing is not None:
            self._process_closed_candle(symbol, closed_for_processing, new_forming, source="LIVE")

    def _process_closed_candle(self, symbol: str, closed_candle: dict,
                                new_forming_candle: dict, source: str = "LIVE") -> None:
        store = self.candle_store.get(symbol)
        if store is None:
            return
        lock = self._candle_locks.setdefault(symbol, threading.Lock())

        try:
            with lock:
                last_closed_time = self._last_closed_time.get(symbol, 0)
                if closed_candle["time"] <= last_closed_time:
                    self._log("debug", "PIPELINE", f"[{symbol}] closed candle t={closed_candle['time']} already processed - skipping duplicate ({source})")
                    return
                store.append(closed_candle)
                self._last_closed_time[symbol] = closed_candle["time"]
                self.last_candle_time[symbol] = time.time()
                store_snapshot = list(store)

            self._recovery_issue_baseline.pop(symbol, None)

            self._log("info", "CANDLE-CLOSED",
                      f"[CANDLE CLOSED] {symbol} | source={source} | timeframe={self.timeframe} | "
                      f"time={closed_candle['time']} | "
                      f"O={smart_fmt(closed_candle['open'])} "
                      f"H={smart_fmt(closed_candle['high'])} "
                      f"L={smart_fmt(closed_candle['low'])} "
                      f"C={smart_fmt(closed_candle['close'])} "
                      f"V={smart_fmt(closed_candle.get('volume', 0))}")
            self._log("info", "PIPELINE",
                      f"[{symbol}] Closed candle t={closed_candle['time']} processed exactly once "
                      f"(store size={len(store_snapshot)})")

            if symbol in self.sr_managers:
                self.sr_managers[symbol].update_levels(store_snapshot, initializing=False)

            if symbol in self.active_trades:
                trade = self.active_trades.get(symbol)
                if trade and not trade.get("_reserved"):
                    self._check_supertrend_conditions(symbol, store_snapshot)

                    trade = self.active_trades.get(symbol)
                    if trade and not trade.get("_reserved"):
                        st_mode = trade.get("st_mode", False)
                        if not st_mode:
                            self._check_stop_loss_on_close(symbol, closed_candle)

            if symbol not in self.active_trades:
                if not self._pipeline_ready.is_set():
                    self._log("info", "EVAL-SKIP",
                              f"[{symbol}] strategy evaluation skipped: startup pipeline not yet "
                              f"marked ready (waiting on historical data / initial S/R / live-feed "
                              f"confirmation).")
                elif self.daily_loss_tracker.is_limit_reached():
                    self._log("info", "EVAL-SKIP", f"[{symbol}] strategy evaluation skipped: daily loss limit reached ({self.daily_loss_tracker.status()})")
                else:
                    candle_list = store_snapshot + [new_forming_candle]
                    self._log("info", "PIPELINE", f"[{symbol}] Strategy evaluation RUNNING on {len(candle_list)} candles (closed + forming)")

                    signal_found = False

                    if self.enable_short:
                        triggered, signal_candle, strategy_name, rsi_value = check_short_signal_no_rsi(
                            candle_list, self.sr_managers[symbol], symbol=symbol,
                            harami_tolerance=self.harami_tolerance,
                            notifier=self.notifier
                        )
                        if triggered and signal_candle is not None:
                            self._on_signal(symbol, signal_candle, strategy_name,
                                             rsi_value, "SHORT", no_rsi=True)
                            signal_found = True
                        else:
                            triggered, signal_candle, strategy_name, rsi_value = check_short_signal(
                                candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                            )
                            if triggered and signal_candle is not None:
                                self._on_signal(symbol, signal_candle, strategy_name,
                                                 rsi_value, "SHORT", no_rsi=False)
                                signal_found = True

                    if self.enable_long and symbol not in self.active_trades:
                        triggered, signal_candle, strategy_name, rsi_value = check_long_signal_no_rsi(
                            candle_list, self.sr_managers[symbol], symbol=symbol,
                            harami_tolerance=self.harami_tolerance,
                            notifier=self.notifier
                        )
                        if triggered and signal_candle is not None:
                            self._on_signal(symbol, signal_candle, strategy_name,
                                             rsi_value, "LONG", no_rsi=True)
                            signal_found = True
                        else:
                            triggered, signal_candle, strategy_name, rsi_value = check_long_signal(
                                candle_list, symbol=symbol, harami_tolerance=self.harami_tolerance
                            )
                            if triggered and signal_candle is not None:
                                self._on_signal(symbol, signal_candle, strategy_name,
                                                 rsi_value, "LONG", no_rsi=False)
                                signal_found = True

                    self._log("info", "PIPELINE",
                              f"[{symbol}] Strategy evaluation COMPLETE - "
                              f"{'signal generated' if signal_found else 'no signal, all strategies rejected'}")

                    self.last_eval_time[symbol] = time.time()
            else:
                self._log("info", "EVAL-SKIP", f"[{symbol}] strategy evaluation skipped: an active trade is already open for this symbol")

            if self._symbol_error_counts.get(symbol):
                self._symbol_error_counts[symbol] = 0

        except Exception as e:
            self._handle_symbol_processing_error(symbol, e)

    def _check_take_profit(self, symbol: str, candle: dict) -> None:
        try:
            trade = self.active_trades.get(symbol)
            if not trade or "_reserved" in trade:
                return
            if trade.get("st_mode", False):
                return

            tp = trade.get("take_profit")
            direction = trade.get("direction", "SHORT")
            entry = trade["entry"]
            if tp is None:
                return

            if direction == "SHORT" and candle["low"] <= tp:
                _log("info", "TP-HIT", f"TAKE PROFIT HIT (intra-candle) | {symbol} | entry={smart_fmt(entry)} tp={smart_fmt(tp)}")
                self._close_trade(symbol, trade, "TAKE_PROFIT", exit_price=tp)

            elif direction == "LONG" and candle["high"] >= tp:
                _log("info", "TP-HIT", f"TAKE PROFIT HIT (intra-candle) | {symbol} | entry={smart_fmt(entry)} tp={smart_fmt(tp)}")
                self._close_trade(symbol, trade, "TAKE_PROFIT", exit_price=tp)
        except Exception as e:
            _log_exc("TP-CHECK", f"[{symbol}] take-profit check failed (position left open, will retry next tick): {e}")

    def _check_stop_loss_on_close(self, symbol: str, closed_candle: dict) -> None:
        try:
            trade = self.active_trades.get(symbol)
            if not trade or "_reserved" in trade:
                return
            if trade.get("st_mode", False):
                return

            sl = trade["stop_loss"]
            direction = trade.get("direction", "SHORT")
            close_price = closed_candle["close"]

            if direction == "SHORT" and close_price >= sl:
                _log("info", "SL-CLOSE", f"STOP LOSS HIT (candle close) | {symbol} SHORT | close={smart_fmt(close_price)} >= sl={smart_fmt(sl)}")
                self._close_trade(symbol, trade, "STOP_LOSS")

            elif direction == "LONG" and close_price <= sl:
                _log("info", "SL-CLOSE", f"STOP LOSS HIT (candle close) | {symbol} LONG | close={smart_fmt(close_price)} <= sl={smart_fmt(sl)}")
                self._close_trade(symbol, trade, "STOP_LOSS")
        except Exception as e:
            _log_exc("SL-CHECK", f"[{symbol}] stop-loss check failed (position left open, will retry next candle): {e}")

    def _on_signal(self, symbol: str, signal_candle: dict, strategy_name: str,
                   rsi_value: Optional[float], direction: str = "SHORT",
                   no_rsi: bool = False) -> None:
        try:
            entry = signal_candle["close"]

            if direction == "SHORT":
                if strategy_name in ("SUPPORT_BREAKDOWN_SHORT", "RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT"):
                    sl = signal_candle.get("pattern_high", signal_candle["high"])
                elif strategy_name == "BEARISH_DOJI":
                    sl = signal_candle.get("pattern_high", signal_candle["high"])
                elif strategy_name in ("STRATEGY_1_SHORT", "RANGE_BREAK_SHORT",
                                        "VOL_EXPANSION_SHORT", "BEARISH_ENGULFING") and "pattern_high" in signal_candle:
                    sl = signal_candle["pattern_high"]
                elif strategy_name == "BEARISH_HARAMI":
                    store = self.candle_store.get(symbol)
                    if store and len(store) >= 2:
                        sl = list(store)[-2]["high"]
                    else:
                        sl = signal_candle.get("pattern_high", signal_candle["high"])
                else:
                    store = self.candle_store.get(symbol)
                    sl = list(store)[-2]["high"] if store and len(store) >= 2 else signal_candle["high"]
            else:
                if strategy_name in ("RESISTANCE_BREAKOUT_LONG", "SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"):
                    sl = signal_candle.get("pattern_low", signal_candle["low"])
                elif strategy_name == "BULLISH_DOJI":
                    sl = signal_candle.get("pattern_low", signal_candle["low"])
                elif strategy_name in ("STRATEGY_1_LONG", "RANGE_BREAK_LONG",
                                        "VOL_EXPANSION_LONG", "BULLISH_ENGULFING") and "pattern_low" in signal_candle:
                    sl = signal_candle["pattern_low"]
                elif strategy_name == "BULLISH_HARAMI":
                    store = self.candle_store.get(symbol)
                    if store and len(store) >= 2:
                        sl = list(store)[-2]["low"]
                    else:
                        sl = signal_candle.get("pattern_low", signal_candle["low"])
                else:
                    store = self.candle_store.get(symbol)
                    sl = list(store)[-2]["low"] if store and len(store) >= 2 else signal_candle["low"]

            if abs(sl - entry) == 0:
                self._log("warning", "SIGNAL", f"risk_per_unit=0 for {symbol} - skip")
                return

            tp = compute_take_profit(entry, sl, direction)

            with self._trade_lock:
                if len(self.active_trades) >= self.max_trades:
                    self._log("info", "SIGNAL", f"[{symbol}] {direction} {strategy_name} REJECTED: Max trades reached")
                    return
                if symbol in self.active_trades:
                    self._log("info", "SIGNAL", f"[{symbol}] {direction} {strategy_name} REJECTED: Active trade exists")
                    return

                risk_usd = round(self.trading_capital * self.risk_pct, 2)
                signal = {
                    "time": datetime.now(timezone.utc).isoformat(),
                    "symbol": symbol, "direction": direction,
                    "entry": entry, "stop_loss": sl, "take_profit": tp,
                    "timeframe": self.timeframe,
                    "mode": "PAPER" if self.paper else "LIVE",
                    "executed": False,
                    "product_id": self.product_map.get(symbol),
                    "risk_usd": risk_usd,
                    "trading_capital": self.trading_capital,
                    "strategy": strategy_name,
                    "rsi": rsi_value,
                    "no_rsi": no_rsi,
                    "harami_tolerance": self.harami_tolerance if strategy_name in ("BEARISH_HARAMI", "BULLISH_HARAMI") else None,
                    "breakout_level": signal_candle.get("breakout_level", None),
                    "level_strength": signal_candle.get("level_strength", None),
                    "level_touches": signal_candle.get("level_touches", None),
                    "reversal_candle_close": signal_candle.get("reversal_candle_close", None),
                    "confirmation_close": signal_candle.get("confirmation_close", None),
                    "false_breakout_high": signal_candle.get("false_breakout_high", None),
                    "false_breakout_low": signal_candle.get("false_breakout_low", None),
                }
                self.signals.append(signal)

                if self.paper:
                    self.active_trades[symbol] = {
                        "entry": entry, "stop_loss": sl, "take_profit": tp,
                        "direction": direction, "size": 0,
                        "product_id": self.product_map.get(symbol),
                        "open_time": datetime.now(timezone.utc).isoformat(),
                        "strategy": strategy_name, "rsi": rsi_value,
                        "st_mode": False, "no_rsi": no_rsi,
                    }

            rsi_str = "N/A (no RSI)" if no_rsi else (f"{rsi_value:.2f}" if rsi_value is not None else "N/A")
            stop_dist = abs(entry - sl)
            rr_actual = abs(entry - tp) / stop_dist if stop_dist > 0 else 0
            direction_arrow = "DOWN SHORT" if direction == "SHORT" else "UP LONG"

            strategy_category = ""
            if strategy_name in ("RESISTANCE_FALSE_BREAKOUT_REVERSAL_SHORT", "SUPPORT_FALSE_BREAKOUT_REVERSAL_LONG"):
                strategy_category = " [S/R FALSE BREAKOUT REVERSAL]"
            elif strategy_name in ("RESISTANCE_BREAKOUT_LONG", "SUPPORT_BREAKDOWN_SHORT"):
                strategy_category = " [S/R BREAKOUT]"
            elif strategy_name in ("BEARISH_ENGULFING", "BULLISH_ENGULFING"):
                strategy_category = " [ENGULFING PATTERN]"
            elif strategy_name in ("BEARISH_HARAMI", "BULLISH_HARAMI"):
                strategy_category = f" [HARAMI PATTERN - tol: {self.harami_tolerance * 100:.2f}%]"
            elif strategy_name in ("BEARISH_DOJI", "BULLISH_DOJI"):
                strategy_category = f" [DOJI - body <= {DOJI_BODY_RATIO_MAX * 100:.0f}%]"

            print()
            print(f"  [SIGNAL] {symbol}  {direction_arrow}  [{self.timeframe}] - {strategy_name}{strategy_category}")
            print(f"           Entry       : {smart_fmt(entry)}")
            print(f"           Stop Loss   : {smart_fmt(sl)} (distance={smart_fmt(stop_dist)}) [CANDLE CLOSE]")
            print(f"           Take Profit : {smart_fmt(tp)} (R:R = 1:{rr_actual:.2f}) [PRICE TOUCH]")
            print(f"           Risk        : ${risk_usd:,.2f} ({self.config['risk_pct']}%)")
            print(f"           RSI(14)     : {rsi_str}")
            if signal.get("breakout_level"):
                stars = "*" * (signal.get("level_strength", 0) or 0)
                print(f"           S/R Level   : {smart_fmt(signal['breakout_level'])} {stars}")
                if signal.get("reversal_candle_close"):
                    print(f"           False Breakout Close: {smart_fmt(signal['reversal_candle_close'])}")
                if signal.get("confirmation_close"):
                    print(f"           Confirm Close: {smart_fmt(signal['confirmation_close'])}")
                if signal.get("break_candle_close"):
                    print(f"           Break Close  : {smart_fmt(signal['break_candle_close'])}")
            print(f"           Mode        : {signal['mode']}")
            print(f"           SuperTrend  : Monitoring ST(14,2) + ST(21,1) post-entry")
            print()

            if self.notifier:
                try:
                    self.notifier.send_signal(signal)
                except Exception as e:
                    _log_exc("SIGNAL", f"[{symbol}] failed to send signal email (signal still recorded): {e}")

            if not self.paper:
                self._execute_trade(symbol, signal)

            if self.on_signal_callback:
                try:
                    self.on_signal_callback(signal)
                except Exception as e:
                    _log_exc("SIGNAL-CALLBACK", f"on_signal_callback raised: {e}")

        except Exception as e:
            _log_exc("ON-SIGNAL", f"[{symbol}] unexpected error handling signal for {strategy_name}: {e}")
            with self._trade_lock:
                trade = self.active_trades.get(symbol)
                if trade and trade.get("_reserved"):
                    self.active_trades.pop(symbol, None)

    def _execute_trade(self, symbol: str, signal: dict) -> None:
        try:
            with self._trade_lock:
                if symbol in self.active_trades:
                    return
                self.active_trades[symbol] = {"_reserved": True}

            account_balance = self.rest.get_usd_balance() or 0.0
            capital = self.trading_capital
            if capital <= 0:
                self._cleanup_trade(symbol)
                return
            if account_balance < capital:
                capital = account_balance
            if capital <= 0:
                self._cleanup_trade(symbol)
                return

            entry = signal["entry"]
            sl = signal["stop_loss"]
            tp = signal["take_profit"]
            direction = signal["direction"]
            pid = self.product_map.get(symbol)
            if not pid:
                self._cleanup_trade(symbol)
                return

            position_size, _ = compute_position_size(
                entry_price=entry, stop_loss_price=sl,
                account_balance=capital, risk_pct=self.risk_pct, leverage=self.leverage,
            )
            if position_size < 1:
                self._cleanup_trade(symbol)
                return

            side = "sell" if direction == "SHORT" else "buy"
            entry_result = self.rest.place_order(
                product_id=pid, side=side, size=position_size, order_type="market_order",
            )

            if not entry_result or "error" in entry_result:
                self._log("error", "TRADE", f"[{symbol}] order placement failed: {entry_result}")
                self._cleanup_trade(symbol)
                return

            order_result = entry_result.get("result", {})
            order_id = order_result.get("id")
            order_state = order_result.get("state", "")

            if order_state == "rejected" or not order_id:
                self._log("error", "TRADE", f"[{symbol}] order rejected or missing id: {order_result}")
                self._cleanup_trade(symbol)
                return

            filled, actual_filled_size = self.rest.wait_for_fill(order_id, symbol)

            if not filled or actual_filled_size < 1:
                self._log("error", "TRADE", f"Fill not confirmed for {symbol} - attempting cancel...")
                self.rest.cancel_order(order_id, pid)
                self._log("error", "TRADE", f"IMPORTANT: Check {symbol} position manually. order_id={order_id}")
                self._cleanup_trade(symbol)
                return

            print(f"  [FILLED] {symbol} {direction} | order_id={order_id} | filled={actual_filled_size} contracts")

            bracket_result = self.rest.place_take_profit_only(product_id=pid, tp_price=tp, symbol=symbol)
            bracket_ok = bracket_result and "error" not in bracket_result

            if bracket_ok:
                print(f"  [TP BRACKET OK] Take profit bracket placed at {smart_fmt(tp)}")
                _log("info", "BRACKET", f"TP bracket placed for {symbol} at {smart_fmt(tp)}")
            else:
                self._log("warning", "BRACKET", f"TP bracket FAILED for {symbol}: {bracket_result} "
                                                  f"(local candle-close stop loss still protects the position)")

            self._log("info", "LOCAL-SL", f"Stop loss managed locally for {symbol} at {smart_fmt(sl)} (candle close)")

            signal["executed"] = True
            signal["size"] = actual_filled_size
            signal["order_id"] = order_id
            signal["bracket_tp_ok"] = bracket_ok

            trade_record = {
                "entry": entry, "stop_loss": sl, "take_profit": tp,
                "direction": direction, "size": actual_filled_size,
                "product_id": pid, "order_id": order_id,
                "open_time": datetime.now(timezone.utc).isoformat(),
                "strategy": signal.get("strategy", "UNKNOWN"),
                "rsi": signal.get("rsi"),
                "bracket_tp_ok": bracket_ok,
                "st_mode": False,
                "no_rsi": signal.get("no_rsi", False),
            }

            with self._trade_lock:
                self.active_trades[symbol] = trade_record

            if self.notifier:
                try:
                    trade_copy = trade_record.copy()
                    trade_copy["symbol"] = symbol
                    self.notifier.send_trade_executed(trade_copy)
                except Exception as e:
                    _log_exc("TRADE", f"[{symbol}] failed to send execution email (trade still live and tracked): {e}")

            if self.on_trade_callback:
                try:
                    self.on_trade_callback(signal)
                except Exception as e:
                    _log_exc("TRADE-CALLBACK", f"on_trade_callback raised: {e}")

        except Exception as exc:
            with self._trade_lock:
                current = self.active_trades.get(symbol)
                if current and current.get("_reserved"):
                    self.active_trades.pop(symbol, None)
                elif current:
                    self._log("error", "TRADE",
                               f"[{symbol}] error after a live position may already exist - "
                               f"KEEPING the local trade record so SL/TP monitoring stays active. "
                               f"Verify the exchange position manually.")
            _log_exc("TRADE", f"[{symbol}] execution error: {exc}")

    def _start_watchdog(self) -> None:
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="BotWatchdog"
        )
        self._watchdog_thread.start()
        self._log("info", "WATCHDOG", f"Health watchdog started (checks every {WATCHDOG_CHECK_INTERVAL // 60} min)")

    def _watchdog_loop(self) -> None:
        for _ in range(WATCHDOG_CHECK_INTERVAL):
            if self._watchdog_stop.is_set():
                return
            time.sleep(1)

        while not self._watchdog_stop.is_set():
            try:
                self._run_health_checks()
            except Exception as e:
                _log_exc("WATCHDOG", f"Watchdog check itself failed (watchdog keeps running): {e}")
            for _ in range(WATCHDOG_CHECK_INTERVAL):
                if self._watchdog_stop.is_set():
                    return
                time.sleep(1)

    def _run_health_checks(self) -> None:
        now = time.time()
        current_issues: Dict[str, str] = {}

        if self.trading_capital <= 0:
            current_issues["CAPITAL_ZERO"] = (
                "trading_capital is $0 or unset. The bot cannot size positions "
                "or evaluate the daily loss limit correctly in this state."
            )

        if not self._pipeline_ready.is_set():
            current_issues["PIPELINE_NOT_READY"] = (
                "The startup pipeline has not marked itself ready even though "
                "the watchdog has started - this should not normally happen, "
                "since the watchdog is only started after pipeline readiness "
                "is confirmed. Investigate the startup sequence."
            )

        ws_state = self.ws_manager.get_state() if self.ws_manager else "unknown"
        ws_down = ws_state in ("disconnected", "reconnecting")
        if ws_down:
            current_issues["WS_DOWN"] = (
                f"WebSocket state is '{ws_state}'. Live candle data may not be "
                f"updating, so no new signals can be detected until it reconnects."
            )

        expected_secs = self._tf.secs

        ws_stale_symbols: List[str] = []
        for sym in self.symbols:
            last_update = self.last_ws_update_time.get(sym)
            reference = last_update if last_update is not None else self._start_time
            silence = now - reference
            if silence > WS_UPDATE_STALE_SECONDS:
                mins = int(silence // 60)
                if last_update is None:
                    current_issues[f"WS_STALE_{sym}"] = (
                        f"[{sym}] no WebSocket update (forming or closed candle) "
                        f"received at all since startup ({mins} min ago, "
                        f"threshold {WS_UPDATE_STALE_SECONDS}s) - the feed may "
                        f"never have subscribed correctly."
                    )
                else:
                    current_issues[f"WS_STALE_{sym}"] = (
                        f"[{sym}] no WebSocket update (forming or closed candle) "
                        f"received in {mins} min (threshold "
                        f"{WS_UPDATE_STALE_SECONDS}s) - the live feed looks dead "
                        f"even though the connection state is '{ws_state}'."
                    )
                ws_stale_symbols.append(sym)

        closed_candle_stale_symbols: List[str] = []
        for sym in self.symbols:
            forming = self._forming_candle.get(sym)
            last_closed = self._last_closed_time.get(sym, 0)

            if forming is not None:
                forming_time = forming["time"]
                deadline = forming_time + expected_secs + CANDLE_CLOSE_GRACE_SECONDS
                is_overdue = now > deadline and last_closed < forming_time
            else:
                forming_time = None
                deadline = self._start_time + expected_secs + CANDLE_CLOSE_GRACE_SECONDS
                is_overdue = now > deadline and last_closed == 0

            if not is_overdue:
                continue

            mins_overdue = int((now - deadline) // 60)
            if forming_time is not None:
                current_issues[f"STALE_{sym}"] = (
                    f"[{sym}] the candle starting at t={forming_time} should "
                    f"have closed by t={int(deadline)} (timeframe={self.timeframe} "
                    f"+ {CANDLE_CLOSE_GRACE_SECONDS}s grace) but no new closed "
                    f"candle has been processed - {mins_overdue} min overdue."
                )
            else:
                current_issues[f"NO_CANDLES_{sym}"] = (
                    f"[{sym}] no closed candle has been processed since "
                    f"startup, and the expected first close time "
                    f"(+{CANDLE_CLOSE_GRACE_SECONDS}s grace) has passed by "
                    f"{mins_overdue} min. Check the WebSocket symbol "
                    f"subscription matches Delta's feed."
                )

            fingerprint = (last_closed, forming_time)
            baseline = self._recovery_issue_baseline.get(sym)
            if baseline == fingerprint:
                self._log("debug", "WATCHDOG",
                          f"[{sym}] closed-candle deadline still overdue but "
                          f"unchanged since the last recovery attempt - "
                          f"withholding another recovery trigger until real "
                          f"progress is observed")
            else:
                closed_candle_stale_symbols.append(sym)

        stale_trigger_symbols = list(dict.fromkeys(ws_stale_symbols + closed_candle_stale_symbols))
        if stale_trigger_symbols and not ws_down and self.ws_manager and self.ws_manager.get_state() == "connected":
            self._trigger_stale_recovery(stale_trigger_symbols)

        daily_limit_active = self.daily_loss_tracker.is_limit_reached()
        if daily_limit_active:
            current_issues["DAILY_LIMIT_ACTIVE"] = (
                f"Daily loss limit is currently ACTIVE - no new trades will open. "
                f"{self.daily_loss_tracker.status()}. This is expected if you "
                f"genuinely hit your loss cap today; if it persists across a day "
                f"rollover, that itself is worth investigating."
            )

        for sym in self.symbols:
            if sym in stale_trigger_symbols:
                continue
            if sym in self.active_trades:
                continue
            if daily_limit_active:
                continue
            last_candle = self.last_candle_time.get(sym)
            last_eval = self.last_eval_time.get(sym)
            if last_candle is not None:
                if last_eval is None or last_candle > last_eval:
                    stall_duration = now - last_candle
                    if stall_duration > expected_secs * EVAL_STALL_MULTIPLIER:
                        mins = int(stall_duration // 60)
                        current_issues[f"EVAL_BLOCKED_{sym}"] = (
                            f"[{sym}] closed candles are being processed but strategy "
                            f"evaluation has not run for {mins} min, with no valid skip "
                            f"reason (no stale feed, no open trade, no daily-loss gate). "
                            f"Something in the evaluation path may be silently failing. "
                            f"The bot auto-resets this symbol's S/R state after "
                            f"{SYMBOL_ERROR_RESET_THRESHOLD} consecutive processing errors - "
                            f"check bot.log for '[PROCESS-CANDLE]' / '[EVAL-SKIP]' entries "
                            f"for {sym}."
                        )

        self._reconcile_watchdog_issues(current_issues)

    def _reconcile_watchdog_issues(self, current_issues: Dict[str, str]) -> None:
        now = time.time()

        for key, message in current_issues.items():
            last_alert = self._active_watchdog_issues.get(key)
            if last_alert is None:
                self._log("error", "WATCHDOG", f"[{key}] {message}")
                if self.notifier:
                    self.notifier.send_health_alert(key, message, resolved=False)
                self._active_watchdog_issues[key] = now
            elif now - last_alert >= HEALTH_ALERT_COOLDOWN:
                self._log("error", "WATCHDOG", f"[{key}] (still active) {message}")
                if self.notifier:
                    self.notifier.send_health_alert(key, message, resolved=False)
                self._active_watchdog_issues[key] = now

        resolved_keys = [k for k in self._active_watchdog_issues if k not in current_issues]
        for key in resolved_keys:
            self._log("info", "WATCHDOG", f"[{key}] condition cleared")
            if self.notifier:
                self.notifier.send_health_alert(key, "This condition is no longer present.", resolved=True)
            del self._active_watchdog_issues[key]

    def _trigger_stale_recovery(self, stale_symbols: List[str]) -> None:
        now = time.time()
        with self._recovery_state_lock:
            if self._recovery_in_progress:
                self._log("info", "RECOVERY", f"Stale condition detected for {stale_symbols} but a recovery is already in progress - skipping")
                return
            elapsed = now - self._last_recovery_attempt
            if elapsed < STALE_RECOVERY_COOLDOWN:
                remaining = int(STALE_RECOVERY_COOLDOWN - elapsed)
                self._log("info", "RECOVERY", f"Stale condition detected for {stale_symbols} but still in cooldown ({remaining}s remaining) - skipping")
                return
            self._recovery_in_progress = True
            self._last_recovery_attempt = now
            for sym in stale_symbols:
                forming = self._forming_candle.get(sym)
                forming_time = forming["time"] if forming else None
                self._recovery_issue_baseline[sym] = (self._last_closed_time.get(sym, 0), forming_time)

        threading.Thread(
            target=self._run_stale_recovery, args=(stale_symbols,),
            daemon=True, name="StaleRecovery"
        ).start()

    def _run_stale_recovery(self, stale_symbols: List[str]) -> None:
        try:
            self._log("warning", "RECOVERY", f"Starting automatic stale-feed recovery (triggered by: {stale_symbols})")
            self._force_ws_reconnect()
            time.sleep(RECOVERY_SETTLE_SECONDS)
            for sym in self.symbols:
                try:
                    self._backfill_symbol(sym)
                except Exception as e:
                    _log_exc("RECOVERY", f"[{sym}] backfill during recovery failed, continuing with other symbols: {e}")
            self._log("info", "RECOVERY", "Recovery sequence complete - watchdog will confirm resolution on its next check")
        except Exception as e:
            _log_exc("RECOVERY", f"Stale-feed recovery attempt failed: {e}")
        finally:
            with self._recovery_state_lock:
                self._recovery_in_progress = False

    def _force_ws_reconnect(self) -> None:
        old_ws = self.ws_manager
        new_ws = DeltaWebSocket()
        self._wire_ws_callbacks(new_ws)
        ws_symbols = [to_ws_symbol(sym) for sym in self.symbols]
        new_ws.subscribe(self.timeframe, ws_symbols)
        new_ws.start()
        self.ws_manager = new_ws
        if old_ws:
            try:
                old_ws.stop()
            except Exception as e:
                _log_exc("RECOVERY", f"Error stopping old WebSocket during forced reconnect: {e}")
        self._log("info", "RECOVERY", "WebSocket force-reconnected with a fresh connection and re-subscribed to all symbols")

    def _handle_symbol_processing_error(self, symbol: str, exc: Exception) -> None:
        count = self._symbol_error_counts.get(symbol, 0) + 1
        self._symbol_error_counts[symbol] = count

        _log_exc("PROCESS-CANDLE",
                  f"[{symbol}] error #{count} while processing closed candle: {exc}")

        if count >= SYMBOL_ERROR_RESET_THRESHOLD:
            self._log("error", "SELF-HEAL",
                       f"[{symbol}] hit {count} consecutive processing errors - "
                       f"resetting S/R level state for this symbol and continuing.")
            sr_manager = self.sr_managers.get(symbol)
            if sr_manager:
                try:
                    sr_manager.reset()
                except Exception as e:
                    _log_exc("SELF-HEAL", f"[{symbol}] SR manager reset itself failed: {e}")
            self._symbol_error_counts[symbol] = 0
            if self.notifier:
                self.notifier.send_health_alert(
                    f"AUTO_RECOVERED_{symbol}",
                    f"[{symbol}] had {count} consecutive candle-processing errors. "
                    f"The bot automatically reset this symbol's S/R level state and "
                    f"will keep trading normally. See bot.log for the underlying "
                    f"exception if you want to investigate the root cause.",
                    resolved=False,
                )

    def _print_banner(self) -> None:
        print()
        print("+========================================================+")
        print("|   DELTA EXCHANGE INDIA - TRADING BOT  v14.9 (FIXED)     |")
        print("|   FIX: Clean concise S/R logs showing:                  |")
        print("|   NEW → MERGED → RECLASSIFIED → FILTERED → FINAL       |")
        print("|   All S/R levels clearly tracked through the pipeline.  |")
        print("+========================================================+")
        print()

    def _print_startup_summary(self) -> None:
        mode_str = "PAPER (signals only)" if self.paper else "LIVE TRADING"
        risk_usd = self.trading_capital * self.risk_pct
        daily_limit_usd = self.trading_capital * self.daily_loss_limit_pct
        print("+--------------------------------------------------------+")
        print(f"  Mode              : {mode_str}")
        print(f"  Timeframe         : {self.timeframe}")
        print(f"  Trading capital   : ${self.trading_capital:,.2f} USD")
        print(f"  Risk / trade      : {self.config['risk_pct']}%  =  ~${risk_usd:,.2f} USD")
        print(f"  Take-Profit (def) : {TP_RR_RATIO:.1f}:1 (triggers on PRICE TOUCH)")
        print(f"  Take-Profit (ST)  : Both SuperTrends reverse direction")
        print(f"  Stop Loss         : Triggers on CANDLE CLOSE only (disabled in ST mode)")
        print(f"  Daily loss cap    : {self.daily_loss_limit_pct * 100:.0f}%  =  ~${daily_limit_usd:,.2f} USD")
        print(f"  Leverage          : {self.leverage}x")
        print(f"  Max open trades   : {self.max_trades}")
        print(f"  SHORT TRADES      : {'ENABLED' if self.enable_short else 'DISABLED'}")
        print(f"  LONG TRADES       : {'ENABLED' if self.enable_long else 'DISABLED'}")
        print(f"  RSI SHORT filter  : RSI(14) > {RSI_OVERBOUGHT}")
        print(f"  RSI LONG  filter  : RSI(14) < {RSI_OVERSOLD}")
        print(f"  RSI LONG  BLOCK   : RSI(14) < 24 (extreme oversold - no trades)")
        print(f"  NO RSI FILTER     : Range Break, Vol Expansion, S/R Breakout, S/R Reversal")
        print(f"  S/R Trade Timing  : IMMEDIATE on confirmation candle close")
        print(f"  S/R Init          : Complete historical scan (all swing points) - once at startup")
        print(f"  S/R Live          : Incremental updates per new candle - maintains processed-state")
        print(f"  S/R Merging       : ENABLED (weighted avg by touches/strength, threshold={SR_MERGE_THRESHOLD*100:.2f}%)")
        print(f"  S/R Reclassify    : ENABLED (fixes classification based on current price)")
        print(f"  S/R Min Distance  : ENABLED ({MIN_SR_DISTANCE_PERCENT}% minimum separation - runs ITERATIVELY)")
        print(f"  S/R Logging       : NEW → MERGED → RECLASSIFIED → FILTERED → FINAL (clean concise)")
        print(f"  S/R Rejection Log : Detailed reasons + email alerts")
        print(f"  S/R Email Alerts  : {'ENABLED' if self.notifier and self.notifier.enabled else 'DISABLED'} (sent asynchronously)")
        print(f"  Price Precision   : auto dp via smart_fmt() - supports micro-price alts")
        print(f"  GMAIL             : {'ENABLED' if self.notifier and self.notifier.enabled else 'DISABLED'} (non-blocking async send)")
        print(f"  Health Watchdog   : ENABLED (checks every {WATCHDOG_CHECK_INTERVAL // 60} min, "
              f"timeframe-aware WS-liveness vs closed-candle checks, "
              f"emails on silent failure via GMAIL if enabled, started LAST)")
        print(f"  Auto Stale-Recover: ENABLED (reconnect + resubscribe + backfill, "
              f"cooldown {STALE_RECOVERY_COOLDOWN}s, WS-stale threshold "
              f"{WS_UPDATE_STALE_SECONDS}s, candle-close grace {CANDLE_CLOSE_GRACE_SECONDS}s)")
        print(f"  Self-Healing      : Per-symbol processing errors auto-reset after "
              f"{SYMBOL_ERROR_RESET_THRESHOLD} consecutive failures")
        print(f"  WS Diagnostics    : ENABLED (connect/subscribe/message/close all logged; "
              f"unhandled message types logged, throttled every {UNHANDLED_WS_MSG_LOG_INTERVAL}s per type)")
        print(f"  Startup Gating    : ENABLED (event-based readiness for WS connect / "
              f"subscribe-confirm / first-live-data, bounded timeouts as a safety net only)")
        print(f"  Symbols ({len(self.symbols)}):")
        for sym in self.symbols:
            pid = self.product_map.get(sym, "???")
            tick_sz = self.rest.get_tick_size(sym)
            print(f"    - {sym:<22} pid={pid}  tick={smart_fmt(tick_sz)}")
        print("+--------------------------------------------------------+")
        print()


# ================================================================
#  23. USER INPUT HELPERS
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
    print("  [1]  Paper Mode   - signals only, no real orders")
    print("  [2]  Live Trading - real orders on Delta Exchange India")
    raw = input("  Enter 1 or 2 (default = 1) : ").strip()
    if raw == "2":
        _divider("API CREDENTIALS")
        api_key = input("  API Key    : ").strip()
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
    enable_long = input("  Enable LONG trades? (Y/n) : ").strip().lower() != 'n'
    if not enable_short and not enable_long:
        print("\n  WARNING: Both short and long trades are disabled!")
        proceed = input("  Do you want to continue anyway? (y/N) : ").strip().lower()
        if proceed != 'y':
            print("  Restart and select at least one direction.")
            exit(0)
    print()
    print(f"  Short trades: {'ENABLED' if enable_short else 'DISABLED'}")
    print(f"  Long trades : {'ENABLED' if enable_long else 'DISABLED'}")
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
    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()] if recipients_raw else [sender]
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
    if not raw:
        return []
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
    if not raw:
        return account_balance
    try:
        capital = float(raw)
    except ValueError:
        capital = account_balance
    if capital <= 0:
        return account_balance
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
            print(f"  WARNING: {tolerance * 100:.1f}% is very permissive!")
            proceed = input("  Continue with this value? (y/N) : ").strip().lower()
            if proceed != 'y':
                return ask_harami_tolerance()
        tolerance = max(0.0001, tolerance)
    except ValueError:
        tolerance = 0.001
    print(f"  Harami tolerance : {tolerance * 100:.2f}%")
    return tolerance


def test_gmail():
    print("\n  TESTING GMAIL NOTIFICATIONS")
    print("  " + "=" * 50)
    sender = input("  Your Gmail address: ").strip()
    if not sender:
        print("  Invalid email. Test cancelled.")
        return
    app_password = input("  Gmail App Password: ").strip()
    if not app_password:
        print("  Invalid password. Test cancelled.")
        return
    notifier = GmailNotifier(
        sender_email=sender,
        gmail_app_password=app_password,
        recipient_emails=[sender],
        enabled=True,
    )
    print("\n  Sending test signal notification (async, non-blocking)...")
    success = notifier.send_signal({
        "direction": "LONG",
        "symbol": "BTCUSD_PERP",
        "strategy": "TEST_SIGNAL",
        "timeframe": "1h",
        "entry": 65000.0,
        "stop_loss": 63000.0,
        "take_profit": 69000.0,
        "rsi": 28.5,
        "mode": "PAPER",
        "risk_usd": 20.0,
        "trading_capital": 1000.0,
        "time": datetime.now(timezone.utc).isoformat(),
        "no_rsi": False,
    })
    if success:
        print("  Test email dispatched in the background - check bot.log for SUCCESS/FAILURE.")
        time.sleep(3)
    else:
        print("  Failed to dispatch email (notifier disabled).")


# ================================================================
#  24. ENTRY POINT
# ================================================================

def main() -> None:
    print()
    print("  +========================================================+")
    print("  |   DELTA EXCHANGE INDIA  -  TRADING BOT  v14.9 (FIXED)   |")
    print("  +========================================================+")

    _divider("SETUP")
    test_gmail_first = input("  Test Gmail notifications first? (y/N) : ").strip().lower()
    if test_gmail_first == 'y':
        test_gmail()
        print("\n  Continuing with bot setup...")

    timeframe = ask_timeframe()
    paper, api_key, api_secret = ask_mode()
    enable_short, enable_long = ask_trade_directions()
    notifier = ask_gmail_config()
    harami_tolerance = ask_harami_tolerance()

    _divider("CONNECTING TO DELTA EXCHANGE INDIA")
    rest_tmp = DeltaREST(api_key, api_secret)

    print("  Loading product catalogue...")
    product_map = rest_tmp.fetch_product_map()
    if not product_map:
        print("  [ERROR] Could not load product catalogue. Check network.")
        return
    print(f"  {len(product_map)} products loaded.")

    raw_symbols = ask_symbols(product_map)
    leverage = ask_leverage()

    account_balance = 0.0
    if not paper:
        print("\n  Fetching account balance...")
        account_balance = rest_tmp.get_usd_balance() or 0.0
    else:
        _divider("PAPER MODE CAPITAL")
        try:
            account_balance = float(input("  Notional capital USD (default = 1000) : ").strip() or 1000)
        except ValueError:
            account_balance = 1000.0

    trading_capital = ask_trading_capital(account_balance)
    risk_pct, max_trades = ask_risk_params()
    daily_loss_limit_pct = ask_daily_loss_limit()

    _divider("CONFIRM")
    risk_usd = trading_capital * risk_pct / 100
    daily_limit_usd = trading_capital * (daily_loss_limit_pct / 100)
    print(f"  Mode              : {'PAPER' if paper else 'LIVE TRADING'}")
    print(f"  Timeframe         : {timeframe}")
    print(f"  Short Trades      : {'ENABLED' if enable_short else 'DISABLED'}")
    print(f"  Long Trades       : {'ENABLED' if enable_long else 'DISABLED'}")
    print(f"  Symbols           : {raw_symbols if raw_symbols else 'AUTO-SELECT'}")
    print(f"  Leverage          : {leverage}x")
    print(f"  Trading capital   : ${trading_capital:,.2f}")
    print(f"  Risk / trade      : {risk_pct}%  =  ~${risk_usd:,.2f}")
    print(f"  Take-Profit (def) : {TP_RR_RATIO:.1f}:1 R:R (triggers on PRICE TOUCH)")
    print(f"  Take-Profit (ST)  : Both SuperTrends reverse")
    print(f"  Stop Loss         : Triggers on CANDLE CLOSE only")
    print(f"  Daily loss cap    : {daily_loss_limit_pct}%  =  ~${daily_limit_usd:,.2f}")
    print(f"  Max open trades   : {max_trades}")
    print(f"  GMAIL             : {'ENABLED' if notifier and notifier.enabled else 'DISABLED'}")
    print()

    if trading_capital <= 0:
        print("  [ERROR] Trading capital is $0 - the bot cannot start. Enter a positive amount.")
        return

    confirm = input("  Type YES to start the bot : ").strip().upper()
    if confirm != "YES":
        print("  Cancelled.")
        return

    cfg = {
        "paper_mode": paper,
        "symbols": raw_symbols,
        "risk_pct": risk_pct,
        "leverage": leverage,
        "trading_capital": trading_capital,
        "max_concurrent_trades": max_trades,
        "timeframe": timeframe,
        "api_key": api_key,
        "api_secret": api_secret,
        "daily_loss_limit_pct": daily_loss_limit_pct / 100.0,
        "enable_short": enable_short,
        "enable_long": enable_long,
        "harami_tolerance": harami_tolerance,
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
            try:
                open_n = len(bot.active_trades)
                open_syms = list(bot.active_trades.keys())
                ws_state = bot.ws_manager.get_state() if bot.ws_manager else "N/A"
                st_info = ""
                for sym, t in bot.active_trades.items():
                    if isinstance(t, dict) and t.get("st_mode"):
                        st_info += f"[{sym}:ST-MODE] "
                print(
                    f"  [STATUS] Open={open_n}/{max_trades}  "
                    f"Signals={len(bot.signals)}  "
                    f"TPs={len(bot.tp_events)}  SLs={len(bot.sl_events)}  "
                    f"WS={ws_state}  {bot.daily_loss_tracker.status()}  "
                    + (f"Trades={open_syms}" if open_syms else "NoOpenTrades")
                    + (f"  {st_info}" if st_info else "")
                )
            except Exception as e:
                _log_exc("STATUS", f"Status print failed (bot itself keeps running): {e}")
    except KeyboardInterrupt:
        bot.stop()
        print()
        print("  Bot stopped. Goodbye.")
        print()


if __name__ == "__main__":
    main()
