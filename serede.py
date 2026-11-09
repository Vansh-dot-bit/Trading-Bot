"""Security, order-integrity, reconciliation, watchdog, and recovery layer for the trading bot.

Keep this file beside the main bot file. Runtime globals are bound by the main module so
the security layer can use the bot's existing constants, REST client, logging, and helpers.
"""
from __future__ import annotations

import threading
import time
import os
import json
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

def bind_runtime_globals(namespace: dict) -> None:
    """Bind the main bot module namespace used by extracted legacy methods.

    This keeps this refactor behavior-preserving without introducing a circular import.
    """
    globals().update({k: v for k, v in namespace.items() if not k.startswith("__")})


class PositionReconciler:
    """Reconciles local active_trades vs exchange positions + open orders."""

    @dataclass
    class Result:
        matched: List[str] = field(default_factory=list)
        adopted: List[str] = field(default_factory=list)
        orphan: List[str] = field(default_factory=list)
        missing_on_exchange: List[str] = field(default_factory=list)
        size_mismatch: List[str] = field(default_factory=list)
        unmapped_positions: List[str] = field(default_factory=list)
        uncertain: bool = False
        error: Optional[str] = None

    def __init__(self, rest: DeltaREST, product_map: Dict[str, int]):
        self.rest = rest
        self.product_map = dict(product_map)

    def _pid_to_symbol(self) -> Dict[int, str]:
        return {pid: sym for sym, pid in self.product_map.items()}

    def _fetch_with_retry(self, fetch_fn, label: str) -> Optional[list]:
        for attempt in range(SAFETY_RECONCILE_RETRY_ATTEMPTS):
            try:
                data = fetch_fn()
                if data is not None:
                    return data
            except Exception as e:
                _log_exc("RECONCILE", f"{label} fetch raised on attempt {attempt + 1}: {e}")
            delay = _jit_backoff(attempt, base=SAFETY_RECONCILE_BACKOFF_BASE,
                                  cap=SAFETY_RECONCILE_BACKOFF_CAP)
            _log("warning", "RECONCILE",
                 f"{label} fetch attempt {attempt + 1}/{SAFETY_RECONCILE_RETRY_ATTEMPTS} "
                 f"returned nothing - retry in {delay:.1f}s")
            time.sleep(delay)
        return None

    def reconcile(self,
                  local_trades: Dict[str, dict],
                  journal_trades: Dict[str, dict],
                  ) -> "PositionReconciler.Result":
        res = self.Result()

        positions = self._fetch_with_retry(self.rest.get_open_positions, "positions")
        if positions is None:
            res.uncertain = True
            res.error = "Could not fetch exchange positions after retries"
            _log("error", "RECONCILE", res.error + " - local state left unchanged, new trades BLOCKED")
            return res

        orders = self._fetch_with_retry(self.rest.get_open_orders, "open orders")
        if orders is None:
            res.uncertain = True
            res.error = "Could not fetch exchange open orders after retries"
            _log("error", "RECONCILE", res.error + " - new trades BLOCKED until resolved")
            orders = []

        pid_to_sym = self._pid_to_symbol()

        exch_pos: Dict[str, dict] = {}
        for p in positions:
            try:
                size = float(p.get("size", 0) or 0)
            except (TypeError, ValueError):
                size = 0.0
            if abs(size) <= 0:
                continue
            pid = p.get("product_id")
            sym = pid_to_sym.get(pid)
            if not sym:
                label = f"product_id={pid}, size={size}"
                res.unmapped_positions.append(label)
                res.uncertain = True
                _log("error", "RECONCILE",
                     f"Exchange position cannot be mapped ({label}); account state is unverified")
                continue
            exch_pos[sym] = p

        local_syms = set(local_trades.keys())
        exch_syms = set(exch_pos.keys())
        journal_syms = set(journal_trades.keys())

        for sym in local_syms & exch_syms:
            try:
                local_size = int(local_trades[sym].get("size", 0) or 0)
            except (TypeError, ValueError):
                local_size = 0
            exch_size = abs(int(float(exch_pos[sym].get("size", 0) or 0)))
            if local_size == exch_size:
                res.matched.append(sym)
            else:
                _log("warning", "RECONCILE",
                     f"[{sym}] size mismatch local={local_size} exchange={exch_size}")
                res.size_mismatch.append(sym)

        for sym in exch_syms - local_syms:
            res.adopted.append(sym)

        for sym in local_syms - exch_syms:
            res.missing_on_exchange.append(sym)

        for sym in journal_syms - local_syms - exch_syms:
            _log("info", "RECONCILE",
                 f"[{sym}] journal has open record but neither memory nor exchange "
                 f"has a position - treating as already closed")

        _log("info", "RECONCILE",
             f"Reconcile summary | matched={res.matched} adopted={res.adopted} "
             f"size_mismatch={res.size_mismatch} missing_on_exchange={res.missing_on_exchange} "
             f"uncertain={res.uncertain}")
        return res

    def adopt_exchange_position(self, symbol: str, exch_pos: dict,
                                 fallback_trade: Optional[dict] = None) -> Optional[dict]:
        try:
            size = abs(int(float(exch_pos.get("size", 0) or 0)))
            if size < 1:
                return None
            pid = exch_pos.get("product_id")
            entry = float(exch_pos.get("entry_price", 0) or 0)

            if fallback_trade and fallback_trade.get("direction") in ("LONG", "SHORT"):
                direction = fallback_trade["direction"]
            else:
                raw_size = float(exch_pos.get("size", 0) or 0)
                direction = "LONG" if raw_size > 0 else "SHORT"

            if entry <= 0:
                _log("warning", "RECONCILE",
                     f"[{symbol}] exchange position has no entry_price - cannot adopt")
                return None

            sl = fallback_trade.get("stop_loss") if fallback_trade else None
            tp = fallback_trade.get("take_profit") if fallback_trade else None
            if sl is None or tp is None:
                _log("error", "RECONCILE",
                     f"[{symbol}] cannot safely adopt exchange position - "
                     f"no recorded stop_loss/take_profit available. "
                     f"MANUAL INTERVENTION REQUIRED for this symbol.")
                return None

            adopted = {
                "entry": entry,
                "stop_loss": float(sl),
                "take_profit": float(tp),
                "direction": direction,
                "size": size,
                "product_id": pid,
                "open_time": (fallback_trade.get("open_time")
                              if fallback_trade else datetime.now(timezone.utc).isoformat()),
                "strategy": (fallback_trade.get("strategy") if fallback_trade else "ADOPTED_AFTER_RECOVERY"),
                "rsi": (fallback_trade.get("rsi") if fallback_trade else None),
                "bracket_tp_ok": False,
                "st_mode": (fallback_trade.get("st_mode", False) if fallback_trade else False),
                "no_rsi": (fallback_trade.get("no_rsi", False) if fallback_trade else False),
                "balance_before_entry": (fallback_trade.get("balance_before_entry", 0.0)
                                          if fallback_trade else 0.0),
                "user_lot_size": (fallback_trade.get("user_lot_size") if fallback_trade else None),
                "sr_exit_target": (fallback_trade.get("sr_exit_target") if fallback_trade else None),
                "_adopted": True,
            }
            _log("warning", "RECONCILE",
                 f"[{symbol}] ADOPTED exchange position as local trade: "
                 f"{direction} size={size} entry={smart_fmt(entry)} "
                 f"sl={smart_fmt(adopted['stop_loss'])} tp={smart_fmt(adopted['take_profit'])} "
                 f"(journal fallback={'yes' if fallback_trade else 'no'})")
            return adopted
        except Exception as e:
            _log_exc("RECONCILE", f"[{symbol}] failed to adopt exchange position: {e}")
            return None

@dataclass
class UncertainOrder:
    client_order_id: str
    symbol: str
    side: str
    size: int
    product_id: int
    created_ts: float
    last_checked_ts: float = 0.0
    resolution_attempts: int = 0
    # resolved state is set only when exchange status is definitively known
    resolved: bool = False

class UncertainOrderTracker:
    """Tracks client order ids whose submission outcome is unknown.
    An order is removed ONLY when its exchange status is definitively
    established (filled, cancelled, rejected, or not-found-after-sufficient-checks).
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._orders: Dict[str, UncertainOrder] = {}

    def register(self, client_order_id: str, symbol: str, side: str,
                  size: int, product_id: int) -> None:
        with self._lock:
            if client_order_id in self._orders:
                return
            self._orders[client_order_id] = UncertainOrder(
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                size=size,
                product_id=product_id,
                created_ts=time.time(),
            )
            _log("warning", "UNCERTAIN-ORDER",
                 f"Registered uncertain order client_order_id={client_order_id} "
                 f"symbol={symbol} side={side} size={size}")

    def mark_resolved(self, client_order_id: str, reason: str) -> None:
        with self._lock:
            if client_order_id in self._orders:
                del self._orders[client_order_id]
                _log("info", "UNCERTAIN-ORDER",
                     f"Resolved client_order_id={client_order_id} - {reason}")

    def has_any(self) -> bool:
        with self._lock:
            return len(self._orders) > 0

    def count(self) -> int:
        with self._lock:
            return len(self._orders)

    def snapshot(self) -> List[UncertainOrder]:
        with self._lock:
            return list(self._orders.values())

    def record_check(self, client_order_id: str) -> None:
        with self._lock:
            if client_order_id in self._orders:
                o = self._orders[client_order_id]
                o.last_checked_ts = time.time()
                o.resolution_attempts += 1

    def symbols_with_uncertain(self) -> Set[str]:
        with self._lock:
            return {o.symbol for o in self._orders.values()}

class SecurityRecoveryMixin:
    def _block_trading(self, reason: str) -> None:
        with self._safety_state_lock:
            self._safety_block_reason = reason
        if self._trading_allowed.is_set():
            self._trading_allowed.clear()
            _log("warning", "SAFETY-GATE", f"Trading BLOCKED: {reason}")
        else:
            _log("info", "SAFETY-GATE", f"Trading remains BLOCKED: {reason}")

    def _allow_trading(self, reason: str) -> None:
        # This is the final gate: callers cannot override unresolved account or
        # order state by accidentally invoking _allow_trading().
        if getattr(self, "_unverified_account_state", True):
            self._block_trading(f"{reason}: account state is unverified")
            return
        if getattr(self, "_uncertain_orders", None) is not None and self._uncertain_orders.count() > 0:
            self._block_trading(f"{reason}: uncertain orders remain")
            return
        with self._safety_state_lock:
            prev = self._safety_block_reason
            self._safety_block_reason = None
        self._trading_allowed.set()
        _log("info", "SAFETY-GATE", f"Trading ALLOWED: {reason} (was: {prev})")

    def _is_trading_allowed(self) -> bool:
        return self._trading_allowed.is_set()

    def _manual_intervention_alert(self, key: str, message: str,
                                    cooldown: Optional[int] = None) -> None:
        if cooldown is None:
            cooldown = HEALTH_ALERT_COOLDOWN
        now = time.time()
        last = self._last_manual_intervention_alert.get(key, 0.0)
        if now - last < cooldown:
            return
        self._last_manual_intervention_alert[key] = now
        _log("error", "MANUAL-REQUIRED", f"[{key}] {message}")
        if self.notifier:
            try:
                self.notifier.send_health_alert(
                    f"MANUAL_REQUIRED_{key}",
                    f"{message}\n\nThis issue requires manual intervention.",
                    resolved=False,
                )
            except Exception as e:
                _log_exc("MANUAL-REQUIRED", f"failed to send manual-intervention alert: {e}")

    def _persist_journal(self) -> None:
        """Persist active trades to disk. Caller MUST NOT hold _trade_lock."""
        try:
            with self._trade_lock:
                snapshot = {s: dict(t) for s, t in self.active_trades.items()
                            if not t.get("_reserved")}
            self._journal.save(snapshot)
        except Exception as e:
            _log_exc("JOURNAL", f"journal persist failed (non-fatal): {e}")

    def _persist_journal_unlocked(self) -> None:
        """Persist from a caller that already holds _trade_lock.
        Reads active_trades directly; journal.save has its own lock."""
        try:
            snapshot = {s: dict(t) for s, t in self.active_trades.items()
                        if not t.get("_reserved")}
            self._journal.save(snapshot)
        except Exception as e:
            _log_exc("JOURNAL", f"journal persist failed (non-fatal): {e}")

    def _classify_open_orders(self, symbol: str,
                               open_orders: Optional[List[dict]] = None) -> Optional[Dict[str, List[dict]]]:
        """Classify exchange open orders for a symbol.

        Returns None if open_orders could not be fetched (unknown state).
        Otherwise returns:
          {
            "entry_orders": [...],   # opening orders (no stop_order_type, or reduce_only false)
            "tp_orders":    [...],   # take-profit bracket/stop orders
            "sl_orders":    [...],   # stop-loss bracket/stop orders (should not exist)
            "other_orders": [...],   # anything else open
          }

        Delta bracket orders carry `stop_order_type` = 'take_profit_order' or
        'stop_loss_order' [citation:21]. Plain entry orders have no
        `stop_order_type` (or it is null).
        """
        if self.paper:
            return {"entry_orders": [], "tp_orders": [], "sl_orders": [], "other_orders": []}
        if open_orders is None:
            open_orders = self.rest.get_open_orders()
        if open_orders is None:
            return None

        pid = self.product_map.get(symbol)
        if pid is None:
            return {"entry_orders": [], "tp_orders": [], "sl_orders": [], "other_orders": []}

        result = {"entry_orders": [], "tp_orders": [], "sl_orders": [], "other_orders": []}
        for o in open_orders:
            if o.get("product_id") != pid:
                continue
            stop_type = (o.get("stop_order_type") or "").lower()
            state = (o.get("state") or "").lower()
            if state not in ("open", "pending"):
                continue
            if stop_type == "take_profit_order":
                result["tp_orders"].append(o)
            elif stop_type == "stop_loss_order":
                result["sl_orders"].append(o)
            elif o.get("reduce_only", False):
                # reduce-only non-bracket order = likely an exit order
                result["other_orders"].append(o)
            else:
                result["entry_orders"].append(o)
        return result

    def _cancel_tp_orders_for_symbol(self, symbol: str) -> bool:
        """Cancel TP bracket orders for a symbol and verify cancellation.

        Returns True iff no TP orders remain open for the symbol afterward.
        """
        pid = self.product_map.get(symbol)
        if pid is None:
            return False

        classified = self._classify_open_orders(symbol)
        if classified is None:
            _log("error", "CANCEL-TP",
                 f"[{symbol}] cannot verify open orders before cancelling TP - aborting")
            return False

        tp_orders = classified["tp_orders"]
        if not tp_orders:
            return True

        _log("info", "CANCEL-TP",
             f"[{symbol}] cancelling {len(tp_orders)} TP order(s) before close")
        for o in tp_orders:
            try:
                res = self.rest.cancel_order(o.get("id"), pid)
                if res and "error" in res:
                    _log("warning", "CANCEL-TP",
                         f"[{symbol}] cancel returned error for order {o.get('id')}: {res}")
            except Exception as e:
                _log_exc("CANCEL-TP", f"[{symbol}] cancel raised: {e}")

        # Verify
        time.sleep(1.0)
        classified_after = self._classify_open_orders(symbol)
        if classified_after is None:
            _log("error", "CANCEL-TP",
                 f"[{symbol}] cannot verify TP cancellation - aborting close")
            return False
        if classified_after["tp_orders"]:
            _log("error", "CANCEL-TP",
                 f"[{symbol}] {len(classified_after['tp_orders'])} TP order(s) STILL OPEN after cancel - "
                 f"aborting close to avoid conflict")
            return False
        _log("info", "CANCEL-TP", f"[{symbol}] TP cancellation verified")
        return True

    def _check_open_orders_for_symbol(self, symbol: str) -> Optional[bool]:
        """Return True if ANY open order exists for the symbol (entry or
        bracket), False if definitively none, None if unverifiable."""
        classified = self._classify_open_orders(symbol)
        if classified is None:
            return None
        total = (len(classified["entry_orders"]) + len(classified["tp_orders"])
                 + len(classified["sl_orders"]) + len(classified["other_orders"]))
        return total > 0

    def _submit_order_idempotent(self, symbol: str, product_id: int, side: str,
                                  size: int, order_type: str = "market_order") -> dict:
        """Submit an entry order with full uncertainty handling.

        Returns dict with keys:
          - on definitive success: {"result": {...}, "client_order_id": "..."}
          - on definitive failure:  {"error": "...", ...}
          - on uncertainty:         {"error": "uncertain_submission", "client_order_id": "..."}
        """
        # Paper mode must never submit a real exchange order, even if this
        # helper is called accidentally by a future code path.
        if self.paper:
            _log("error", "DUP-GUARD",
                 f"[{symbol}] blocked live order submission because PAPER MODE is enabled")
            return {"error": "paper_mode_live_order_blocked"}

        # 1. If there are ANY uncertain orders outstanding for this symbol,
        #    refuse to submit a new one until they are reconciled.
        if self._uncertain_orders.has_any():
            _log("warning", "DUP-GUARD",
                 f"[{symbol}] refusing to submit new order while "
                 f"{self._uncertain_orders.count()} uncertain order(s) exist. "
                 f"Waiting for reconciliation.")
            return {"error": "uncertain_orders_outstanding"}

        # 2. Pre-flight: any open order for this symbol -> do not duplicate.
        has_open = self._check_open_orders_for_symbol(symbol)
        if has_open is True:
            _log("warning", "DUP-GUARD",
                 f"[{symbol}] exchange already shows an open order - refusing to submit another")
            return {"error": "duplicate_open_order_detected"}
        if has_open is None:
            _log("error", "DUP-GUARD",
                 f"[{symbol}] could not verify open orders before submit - refusing to submit "
                 f"to avoid duplicate execution")
            return {"error": "open_orders_unverifiable"}

        client_order_id = f"{ORDER_ID_PREFIX}-{uuid.uuid4().hex[:20]}"
        _log("info", "DUP-GUARD",
             f"[{symbol}] submitting order client_order_id={client_order_id}")
        try:
            result = self.rest.place_order(
                product_id=product_id, side=side, size=size,
                order_type=order_type, client_order_id=client_order_id,
            )
        except Exception as e:
            _log_exc("DUP-GUARD", f"[{symbol}] order submission raised: {e}")
            # Uncertain: register FIRST, then try lookup.
            self._uncertain_orders.register(client_order_id, symbol, side, size, product_id)
            found = self.rest.find_order_by_client_id(client_order_id)
            if found is not None:
                _log("info", "DUP-GUARD",
                     f"[{symbol}] lookup found order after raise - treating as submitted")
                self._uncertain_orders.mark_resolved(client_order_id, "found after raise")
                return {"result": found, "client_order_id": client_order_id}
            # None lookup = UNKNOWN, not proof of non-submission.
            self._manual_intervention_alert(
                f"UNCERTAIN_ORDER_{client_order_id}",
                f"[{symbol}] order submission raised AND lookup returned None (unknown state). "
                f"client_order_id={client_order_id}. Trading BLOCKED until reconciled.",
            )
            self._block_trading(f"uncertain order {client_order_id}")
            return {"error": "uncertain_submission", "client_order_id": client_order_id}

        if not result or "error" in result:
            _log("warning", "DUP-GUARD",
                 f"[{symbol}] submission returned error: {result}")
            # Uncertain: register and lookup.
            self._uncertain_orders.register(client_order_id, symbol, side, size, product_id)
            found = self.rest.find_order_by_client_id(client_order_id)
            if found is not None:
                _log("info", "DUP-GUARD",
                     f"[{symbol}] error response but lookup found the order - "
                     f"treating as submitted (client_order_id={client_order_id})")
                self._uncertain_orders.mark_resolved(client_order_id, "found after error response")
                return {"result": found, "client_order_id": client_order_id}
            # None = unknown. Do NOT assume not submitted.
            self._manual_intervention_alert(
                f"UNCERTAIN_ORDER_{client_order_id}",
                f"[{symbol}] submission returned '{result}' and lookup returned None "
                f"(unknown state). client_order_id={client_order_id}. "
                f"Trading BLOCKED until reconciled.",
            )
            self._block_trading(f"uncertain order {client_order_id}")
            return {"error": "uncertain_submission", "client_order_id": client_order_id}

        # Definitive success path
        return {"result": result.get("result", {}), "client_order_id": client_order_id}

    def _reconcile_uncertain_orders(self) -> bool:
        """Try to resolve every outstanding uncertain client order id.

        Returns True iff no uncertain orders remain.
        An order is removed ONLY when its exchange status is definitively
        established: present with a terminal state, present and open (adopt as
        tracked), or absent after multiple successful lookups (treated as
        not submitted).
        """
        pending = self._uncertain_orders.snapshot()
        if not pending:
            return True

        _log("info", "UNCERTAIN-ORDER",
             f"reconciling {len(pending)} uncertain order(s)")

        all_resolved = True
        for uo in pending:
            self._uncertain_orders.record_check(uo.client_order_id)
            found = self.rest.find_order_by_client_id(uo.client_order_id)

            if found is not None:
                state = (found.get("state") or "").lower()
                if state in ("closed", "cancelled", "rejected", "expired"):
                    self._uncertain_orders.mark_resolved(
                        uo.client_order_id, f"terminal state={state}")
                else:
                    # Order is live on exchange. Adopt it as a tracked entry so
                    # SL/TP monitoring can attach. If we cannot attach safely,
                    # block and flag.
                    _log("warning", "UNCERTAIN-ORDER",
                         f"client_order_id={uo.client_order_id} found LIVE on exchange "
                         f"(state={state}, side={uo.side}, size={uo.size}, "
                         f"symbol={uo.symbol})")
                    if self._attach_found_order(uo, found):
                        self._uncertain_orders.mark_resolved(
                            uo.client_order_id, f"adopted live order state={state}")
                    else:
                        all_resolved = False
            else:
                # The REST helper returns None both when an order is absent and
                # when lookup failed. That is not proof of non-submission. Keep
                # the order unresolved until an exchange order/position or a
                # definitive terminal response is available.
                age = time.time() - uo.created_ts
                _log("error", "UNCERTAIN-ORDER",
                     f"client_order_id={uo.client_order_id} remains UNKNOWN "
                     f"(age={age:.0f}s, attempts={uo.resolution_attempts}); "
                     "will keep trading blocked until positively reconciled")
                all_resolved = False

        if all_resolved:
            _log("info", "UNCERTAIN-ORDER", "all uncertain orders resolved")
        return all_resolved

    def _attach_found_order(self, uo: "UncertainOrder", found: dict) -> bool:
        """Try to attach a found live order to active_trades so SL/TP can be
        monitored. Returns True if attached, False if cannot attach safely."""
        # Only attach if we have a matching trade reservation or journal entry
        # with SL/TP. Otherwise we cannot safely monitor it.
        with self._trade_lock:
            existing = self.active_trades.get(uo.symbol)
        if existing and not existing.get("_reserved"):
            # We already have a record for this symbol - check size match.
            try:
                existing_size = int(existing.get("size", 0) or 0)
            except (TypeError, ValueError):
                existing_size = 0
            if existing_size == uo.size:
                _log("info", "UNCERTAIN-ORDER",
                     f"[{uo.symbol}] live order matches existing local record - attaching")
                return True
            _log("error", "UNCERTAIN-ORDER",
                 f"[{uo.symbol}] live order size {uo.size} != local size {existing_size} - "
                 f"cannot safely attach")
            return False

        # No local record. Cannot infer SL/TP. Flag for manual.
        self._manual_intervention_alert(
            f"ADOPT_ORDER_{uo.client_order_id}",
            f"[{uo.symbol}] found a live exchange order (client_order_id="
            f"{uo.client_order_id}, side={uo.side}, size={uo.size}) but no local "
            f"trade record with SL/TP. Cannot safely monitor. "
            f"Trading BLOCKED for this symbol until resolved.",
        )
        return False

    def _run_reconciliation(self, phase: str = "startup") -> bool:
        with self._reconcile_lock:
            if self._reconcile_in_progress:
                _log("info", "RECONCILE", f"already in progress - skipping ({phase})")
                return self._last_reconcile_ok
            self._reconcile_in_progress = True

        try:
            _log("info", "RECONCILE", f"Starting reconciliation ({phase})")

            # Paper mode has no exchange account to reconcile. Do not call
            # authenticated endpoints (positions, orders, or balance); validate
            # only the local paper-trading capital/state and market-data pipeline.
            if self.paper:
                with self._capital_lock:
                    paper_capital = float(self.trading_capital or 0.0)
                unresolved_orders = self._uncertain_orders.count()
                if paper_capital <= 0:
                    self._block_trading(
                        f"paper reconciliation({phase}): local paper capital must be positive")
                    self._unverified_account_state = True
                    self._last_reconcile_ok = False
                    self._last_reconcile_ts = time.time()
                    return False
                if unresolved_orders:
                    # Never silently discard unresolved state, even in paper mode.
                    self._block_trading(
                        f"paper reconciliation({phase}): {unresolved_orders} unresolved order(s) remain")
                    self._unverified_account_state = True
                    self._last_reconcile_ok = False
                    self._last_reconcile_ts = time.time()
                    return False

                self._persist_journal()
                self._unverified_account_state = False
                self._last_reconcile_ok = True
                self._last_reconcile_ts = time.time()
                pipeline_ok = bool(
                    self.ws_manager and self.ws_manager.is_connected()
                    and self._pipeline_ready.is_set()
                )
                if pipeline_ok:
                    self._allow_trading(
                        f"paper reconciliation({phase}) OK; local state verified, exchange API skipped")
                else:
                    self._block_trading(
                        f"paper reconciliation({phase}) local state OK but market-data pipeline not ready")
                _log("info", "RECONCILE",
                     f"Paper-mode local reconciliation complete | capital=${paper_capital:,.2f} | "
                     f"exchange account endpoints skipped")
                return True

            if self._reconciler is None:
                self._reconciler = PositionReconciler(self.rest, self.product_map)

            # First: resolve any uncertain orders (live mode only).
            uncertain_ok = self._reconcile_uncertain_orders()

            journal_trades = self._journal.load()
            with self._trade_lock:
                local_snapshot = {s: dict(t) for s, t in self.active_trades.items()
                                  if not t.get("_reserved")}

            result = self._reconciler.reconcile(local_snapshot, journal_trades)

            if result.uncertain:
                self._block_trading(f"reconcile({phase}) uncertain: {result.error}")
                self._unverified_account_state = True
                self._last_reconcile_ok = False
                self._last_reconcile_ts = time.time()
                return False

            # Adopt exchange-only positions. Any inability to safely adopt is
            # a reconciliation failure; never reopen entries in that state.
            adoption_failed = False
            for sym in result.adopted:
                positions = self.rest.get_open_positions()
                pid = self.product_map.get(sym)
                raw = (next((p for p in positions if p.get("product_id") == pid), None)
                       if positions is not None else None)
                if raw is None:
                    adoption_failed = True
                    self._manual_intervention_alert(
                        f"ADOPT_{sym}",
                        f"[{sym}] exchange position could not be re-fetched for safe adoption. "
                        "Trading remains blocked until reconciled.",
                    )
                    continue
                fallback = journal_trades.get(sym) or local_snapshot.get(sym)
                adopted = self._reconciler.adopt_exchange_position(sym, raw, fallback)
                if adopted is None:
                    adoption_failed = True
                    self._manual_intervention_alert(
                        f"ADOPT_{sym}",
                        f"[{sym}] Exchange shows an open position but the bot has no "
                        f"recorded stop_loss/take_profit to monitor it safely. "
                        f"Trading BLOCKED for this symbol.",
                    )
                    continue
                with self._trade_lock:
                    self.active_trades[sym] = adopted
                _log("warning", "RECONCILE",
                     f"[{sym}] position ADOPTED from exchange; SL/TP monitoring resumed "
                     f"(local SL - no exchange bracket)")
                if self.notifier:
                    try:
                        self.notifier.send_health_alert(
                            f"RECONCILED_{sym}",
                            f"[{sym}] position was adopted from exchange after recovery. "
                            f"SL/TP monitoring resumed locally. Please verify manually.",
                            resolved=False,
                        )
                    except Exception:
                        pass

            # Size mismatches: do NOT silently accept. Block.
            for sym in result.size_mismatch:
                self._manual_intervention_alert(
                    f"SIZE_MISMATCH_{sym}",
                    f"[{sym}] local size differs from exchange size. "
                    f"Trading BLOCKED until resolved.",
                )

            # Missing on exchange: flag but keep local record (exchange lag).
            for sym in result.missing_on_exchange:
                self._manual_intervention_alert(
                    f"MISSING_ON_EXCH_{sym}",
                    f"[{sym}] local bot thinks a position is open but exchange reports "
                    f"none. Please verify on the exchange dashboard.",
                )

            # Any unresolved position discrepancy is a hard stop. In particular,
            # never treat a missing exchange position as a harmless lag or mark
            # reconciliation successful when adoption/mapping failed.
            critical_position_issues = bool(
                result.size_mismatch or result.missing_on_exchange
                or result.unmapped_positions or adoption_failed
                or not uncertain_ok or self._uncertain_orders.count() > 0
            )
            if critical_position_issues:
                details = []
                if result.size_mismatch:
                    details.append(f"size_mismatch={result.size_mismatch}")
                if result.missing_on_exchange:
                    details.append(f"missing_on_exchange={result.missing_on_exchange}")
                if result.unmapped_positions:
                    details.append(f"unmapped_positions={result.unmapped_positions}")
                if adoption_failed:
                    details.append("position_adoption_failed=True")
                if not uncertain_ok or self._uncertain_orders.count() > 0:
                    details.append(f"uncertain_orders={self._uncertain_orders.count()}")
                reason = f"reconcile({phase}) unresolved position state: " + ", ".join(details)
                self._block_trading(reason)
                self._unverified_account_state = True
                self._last_reconcile_ok = False
                self._last_reconcile_ts = time.time()
                return False

            # Balance check
            bal = self.rest.get_usd_balance()
            if bal is None or bal <= 0:
                self._block_trading(f"reconcile({phase}): could not verify balance")
                self._unverified_account_state = True
                self._last_reconcile_ok = False
                self._last_reconcile_ts = time.time()
                return False
            with self._capital_lock:
                self.trading_capital = bal
                self.daily_loss_tracker.update_capital(bal)

            self._persist_journal()
            self._unverified_account_state = False
            self._last_reconcile_ok = True
            self._last_reconcile_ts = time.time()

            # NEW: Only allow trading if:
            #   - reconciliation succeeded
            #   - NO uncertain orders remain
            #   - NO size mismatches
            #   - WS connected + pipeline ready
            can_trade = (
                uncertain_ok
                and self._uncertain_orders.count() == 0
                and not result.size_mismatch
                and self.ws_manager
                and self.ws_manager.is_connected()
                and self._pipeline_ready.is_set()
            )
            if can_trade:
                self._allow_trading(f"reconcile({phase}) OK, no unresolved issues")
            else:
                reasons = []
                if not uncertain_ok or self._uncertain_orders.count() > 0:
                    reasons.append(f"uncertain_orders={self._uncertain_orders.count()}")
                if result.size_mismatch:
                    reasons.append(f"size_mismatch={result.size_mismatch}")
                if not (self.ws_manager and self.ws_manager.is_connected()):
                    reasons.append("ws_not_connected")
                if not self._pipeline_ready.is_set():
                    reasons.append("pipeline_not_ready")
                self._block_trading(
                    f"reconcile({phase}) incomplete: {', '.join(reasons)}")
            return True
        except Exception as e:
            _log_exc("RECONCILE", f"reconciliation({phase}) failed: {e}")
            self._block_trading(f"reconcile({phase}) exception: {e}")
            self._unverified_account_state = True
            self._last_reconcile_ok = False
            self._last_reconcile_ts = time.time()
            return False
        finally:
            with self._reconcile_lock:
                self._reconcile_in_progress = False

    def _check_and_recover_threads(self) -> None:
        if self.ws_manager:
            if not self.ws_manager.is_thread_alive():
                if self.ws_manager.get_state() not in ("stopped",):
                    _log("error", "RECOVERY",
                         "WebSocket thread is DEAD - forcing reconnect with fresh instance")
                    try:
                        self._force_ws_reconnect()
                    except Exception as e:
                        _log_exc("RECOVERY", f"WS thread recovery failed: {e}")

        if self._watchdog_thread is not None and not self._watchdog_thread.is_alive():
            if not self._watchdog_stop.is_set():
                _log("error", "RECOVERY",
                     "Watchdog thread is DEAD - spawning replacement")
                self._start_watchdog()

        stale = time.time() - self._watchdog_heartbeat
        if stale > WATCHDOG_THREAD_STALL_SECONDS:
            key = "WATCHDOG_STALLED"
            now = time.time()
            last = self._active_watchdog_issues.get(key, 0.0)
            if now - last >= HEALTH_ALERT_COOLDOWN:
                _log("error", "RECOVERY",
                     f"Watchdog heartbeat stale by {stale:.0f}s - a health check may be "
                     f"blocking. Next cycle will re-check; if this repeats, restart the bot.")
                self._active_watchdog_issues[key] = now

    def _automatic_recovery_cycle(self) -> None:
        # Claim the same recovery lock used by stale-feed recovery so watchdog
        # triggers cannot launch overlapping recovery/reconciliation cycles.
        with self._recovery_state_lock:
            if self._recovery_in_progress:
                _log("info", "RECOVERY", "Recovery already in progress - skipping duplicate cycle")
                return
            now = time.time()
            elapsed = now - self._last_recovery_attempt
            if elapsed < STALE_RECOVERY_COOLDOWN:
                _log("info", "RECOVERY",
                     f"Recovery cooldown active ({int(STALE_RECOVERY_COOLDOWN - elapsed)}s remaining)")
                return
            self._recovery_in_progress = True
            self._last_recovery_attempt = now

        _log("info", "RECOVERY", "Starting automatic recovery cycle")
        try:
            try:
                self._check_and_recover_threads()
            except Exception as e:
                _log_exc("RECOVERY", f"thread recovery step failed: {e}")

            ws_ok = False
            try:
                if self.ws_manager and not self.ws_manager.is_connected():
                    _log("warning", "RECOVERY",
                         f"WS not connected (state={self.ws_manager.get_state()}) - forcing reconnect")
                    self._force_ws_reconnect()
                    time.sleep(RECOVERY_SETTLE_SECONDS)
                ws_ok = bool(self.ws_manager and self.ws_manager.is_connected())
            except Exception as e:
                _log_exc("RECOVERY", f"WS recovery step failed: {e}")

            try:
                for sym in self.symbols:
                    try:
                        self._backfill_symbol(sym)
                    except Exception as e:
                        _log_exc("RECOVERY", f"[{sym}] backfill in recovery failed: {e}")
            except Exception as e:
                _log_exc("RECOVERY", f"backfill step failed: {e}")

            reconcile_ok = self._run_reconciliation(phase="auto-recovery")
            if (reconcile_ok and ws_ok and self._pipeline_ready.is_set()
                    and self._uncertain_orders.count() == 0
                    and not self._unverified_account_state):
                self._allow_trading("auto-recovery: all checks passed")
            else:
                self._block_trading(
                    f"auto-recovery incomplete: reconcile_ok={reconcile_ok} ws_ok={ws_ok} "
                    f"uncertain={self._uncertain_orders.count()} "
                    f"unverified={self._unverified_account_state}"
                )
            _log("info", "RECOVERY", "Automatic recovery cycle complete")
        except Exception as e:
            self._block_trading(f"automatic recovery exception: {e}")
            self._unverified_account_state = True
            self._last_reconcile_ok = False
            _log_exc("RECOVERY", f"automatic recovery cycle failed: {e}")
        finally:
            with self._recovery_state_lock:
                self._recovery_in_progress = False

    def _start_watchdog(self) -> None:
        self._watchdog_stop.clear()
        self._watchdog_heartbeat = time.time()
        with self._watchdog_generation_lock:
            self._watchdog_generation += 1
            generation = self._watchdog_generation
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, args=(generation,),
            daemon=True, name=f"BotWatchdog-{generation}"
        )
        self._watchdog_thread.start()
        self._log("info", "WATCHDOG", f"Health watchdog started (generation={generation}, checks every {WATCHDOG_CHECK_INTERVAL // 60} min)")

    def _start_watchdog_supervisor(self) -> None:
        """Independent supervisor that restarts a dead watchdog thread."""
        self._watchdog_supervisor_stop.clear()
        self._watchdog_supervisor_thread = threading.Thread(
            target=self._watchdog_supervisor_loop, daemon=True, name="WatchdogSupervisor"
        )
        self._watchdog_supervisor_thread.start()
        self._log("info", "SUPERVISOR",
                  f"Watchdog supervisor started (checks every {WATCHDOG_SUPERVISOR_INTERVAL}s)")

    def _watchdog_supervisor_loop(self) -> None:
        while not self._watchdog_supervisor_stop.is_set():
            for _ in range(WATCHDOG_SUPERVISOR_INTERVAL):
                if self._watchdog_supervisor_stop.is_set():
                    return
                time.sleep(1)
            try:
                thread_dead = self._watchdog_thread is None or not self._watchdog_thread.is_alive()
                heartbeat_age = time.time() - self._watchdog_heartbeat
                thread_stalled = (not thread_dead and heartbeat_age > WATCHDOG_THREAD_STALL_SECONDS)
                if (thread_dead or thread_stalled) and not self._watchdog_stop.is_set():
                    reason = "DEAD" if thread_dead else f"STALLED (heartbeat age {heartbeat_age:.0f}s)"
                    _log("error", "SUPERVISOR", f"Watchdog thread is {reason} - blocking entries and restarting generation")
                    self._block_trading(f"watchdog {reason.lower()}; awaiting fresh health checks and reconciliation")
                    self._start_watchdog()
            except Exception as e:
                _log_exc("SUPERVISOR", f"Supervisor check failed: {e}")

    def _watchdog_loop(self, generation: Optional[int] = None) -> None:
        def superseded() -> bool:
            with self._watchdog_generation_lock:
                return generation is not None and generation != self._watchdog_generation

        for _ in range(WATCHDOG_CHECK_INTERVAL):
            if self._watchdog_stop.is_set() or superseded():
                return
            self._watchdog_heartbeat = time.time()
            time.sleep(1)

        while not self._watchdog_stop.is_set() and not superseded():
            self._watchdog_heartbeat = time.time()
            try:
                self._run_health_checks()
            except Exception as e:
                _log_exc("WATCHDOG", f"Watchdog check failed (watchdog keeps running): {e}")
            for _ in range(WATCHDOG_CHECK_INTERVAL):
                if self._watchdog_stop.is_set() or superseded():
                    return
                self._watchdog_heartbeat = time.time()
                time.sleep(1)

    def _run_health_checks(self) -> None:
        now = time.time()
        current_issues: Dict[str, str] = {}

        self._check_and_recover_threads()

        with self._capital_lock:
            capital_snapshot = self.trading_capital

        if capital_snapshot <= 0:
            current_issues["CAPITAL_ZERO"] = (
                "trading_capital is $0 or unset."
            )

        if not self._pipeline_ready.is_set():
            current_issues["PIPELINE_NOT_READY"] = (
                "The startup pipeline has not marked itself ready."
            )

        ws_state = self.ws_manager.get_state() if self.ws_manager else "unknown"
        ws_down = ws_state in ("disconnected", "reconnecting")
        if ws_down:
            current_issues["WS_DOWN"] = (
                f"WebSocket state is '{ws_state}'."
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
                        f"[{sym}] no WebSocket update ever received ({mins} min)."
                    )
                else:
                    current_issues[f"WS_STALE_{sym}"] = (
                        f"[{sym}] no WebSocket update in {mins} min."
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
                    f"[{sym}] candle starting at t={forming_time} overdue by {mins_overdue} min."
                )
            else:
                current_issues[f"NO_CANDLES_{sym}"] = (
                    f"[{sym}] no closed candle since startup, overdue by {mins_overdue} min."
                )

            fingerprint = (last_closed, forming_time)
            baseline = self._recovery_issue_baseline.get(sym)
            if baseline == fingerprint:
                self._log("debug", "WATCHDOG",
                          f"[{sym}] still overdue but unchanged since last recovery - "
                          f"withholding another trigger")
            else:
                closed_candle_stale_symbols.append(sym)

        stale_trigger_symbols = list(dict.fromkeys(ws_stale_symbols + closed_candle_stale_symbols))
        if stale_trigger_symbols and not ws_down and self.ws_manager and self.ws_manager.get_state() == "connected":
            self._trigger_stale_recovery(stale_trigger_symbols)

        daily_limit_active = self.daily_loss_tracker.is_limit_reached()
        if daily_limit_active:
            current_issues["DAILY_LIMIT_ACTIVE"] = (
                f"Daily loss limit is ACTIVE. {self.daily_loss_tracker.status()}"
            )

        for sym in self.symbols:
            if sym in stale_trigger_symbols:
                continue
            if sym in self.active_trades:
                continue
            if self._post_exit_cooldown.get(sym, 0) > 0:
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
                            f"[{sym}] strategy evaluation stalled for {mins} min."
                        )

        # Uncertain orders need attention
        if self._uncertain_orders.count() > 0:
            current_issues["UNCERTAIN_ORDERS"] = (
                f"{self._uncertain_orders.count()} uncertain order(s) outstanding. "
                f"Trading BLOCKED until reconciled."
            )

        if (not self._last_reconcile_ok) and (now - self._last_reconcile_ts > RECONCILE_REQUIRED_GRACE):
            current_issues["RECONCILE_NEEDED"] = (
                "Exchange state is not reconciled. Trading remains blocked."
            )
            # Fire automatic recovery if not already running.
            threading.Thread(
                target=self._automatic_recovery_cycle,
                daemon=True, name="AutoRecoveryFromWatchdog",
            ).start()

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
                self._log("info", "RECOVERY", f"stale detected for {stale_symbols} - recovery already in progress")
                return
            elapsed = now - self._last_recovery_attempt
            if elapsed < STALE_RECOVERY_COOLDOWN:
                remaining = int(STALE_RECOVERY_COOLDOWN - elapsed)
                self._log("info", "RECOVERY", f"stale detected for {stale_symbols} - cooldown {remaining}s")
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
            self._log("warning", "RECOVERY", f"automatic stale-feed recovery (triggered by: {stale_symbols})")
            self._force_ws_reconnect()
            time.sleep(RECOVERY_SETTLE_SECONDS)
            for sym in self.symbols:
                try:
                    self._backfill_symbol(sym)
                except Exception as e:
                    _log_exc("RECOVERY", f"[{sym}] backfill during recovery failed: {e}")
            self._run_reconciliation(phase="stale-recovery")
            self._log("info", "RECOVERY", "Recovery sequence complete")
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
        self._log("info", "RECOVERY", "WebSocket force-reconnected and re-subscribed")


# ------------------------------------------------------------------
# Shared API resilience for the Fed and tech-index background services
# ------------------------------------------------------------------
import logging as _logging
import random as _random

class ApiResilience:
    """Bounded retries, exponential backoff, and deduplicated service alerts.

    This protects data collection only. It never retries order placement.
    """
    def __init__(self, notifier=None, attempts=4, base_delay=2.0, max_delay=45.0):
        self.notifier = notifier
        self.attempts = max(1, int(attempts))
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)
        self._lock = threading.Lock()
        self._active_failures = set()

    def get(self, session, url, *, timeout=30, params=None, service="API", **kwargs):
        last_error = None
        for attempt in range(self.attempts):
            try:
                response = session.get(url, params=params, timeout=timeout, **kwargs)
                response.raise_for_status()
                self._mark_recovered(service)
                return response
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.attempts:
                    delay = min(self.max_delay, self.base_delay * (2 ** attempt))
                    delay += _random.uniform(0, min(1.0, delay * 0.2))
                    _logging.getLogger("DeltaBot").warning(
                        "[%s] request failed (%s/%s): %s; retrying in %.1fs",
                        service, attempt + 1, self.attempts, exc, delay)
                    time.sleep(delay)
        self._mark_failed(service, str(last_error))
        raise RuntimeError(f"{service} unavailable after {self.attempts} attempts: {last_error}") from last_error

    def _mark_failed(self, service, detail):
        with self._lock:
            is_new = service not in self._active_failures
            self._active_failures.add(service)
        if is_new and self.notifier:
            try:
                self.notifier.send_custom_alert(
                    f"[{service}] Data service unavailable",
                    f"{service} data requests failed after retries.\n\nLast error: {detail}\n\nThe service will retry automatically.")
            except Exception:
                _logging.getLogger("DeltaBot").exception("Could not send API failure notification")

    def _mark_recovered(self, service):
        with self._lock:
            was_failed = service in self._active_failures
            self._active_failures.discard(service)
        if was_failed and self.notifier:
            try:
                self.notifier.send_custom_alert(
                    f"[{service}] Data service recovered",
                    f"{service} requests are working again. Automatic monitoring has resumed.")
            except Exception:
                _logging.getLogger("DeltaBot").exception("Could not send API recovery notification")

# ------------------------------------------------------------------
# Compatibility supervisors used by the integrated bot launcher
# ------------------------------------------------------------------
import inspect as _inspect

class MainBotSupervisor:
    """Lightweight lifecycle monitor for the main bot.

    TradingBot already owns its candle-feed watchdog and recovery logic. This
    class deliberately does not duplicate those checks or restart order logic;
    it only monitors the bot's running flag and reports unexpected termination.
    """
    def __init__(self, bot, notifier=None, check_interval=15):
        self.bot = bot
        self.notifier = notifier
        self.check_interval = max(1, int(check_interval))
        self._stop_event = threading.Event()
        self._thread = None
        self._reported = False

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._monitor, name="MainBotSupervisor", daemon=True
        )
        self._thread.start()
        _logging.getLogger("DeltaBot").info("Main bot lifecycle supervisor started")

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)

    def _monitor(self):
        while not self._stop_event.wait(self.check_interval):
            if not bool(getattr(self.bot, "running", False)):
                if not self._reported:
                    self._reported = True
                    msg = "The main trading bot has stopped. Check the bot logs for the reason."
                    _logging.getLogger("DeltaBot").error(msg)
                    if self.notifier:
                        try:
                            self.notifier.send_custom_alert("Main trading bot stopped", msg)
                        except Exception:
                            _logging.getLogger("DeltaBot").exception(
                                "Could not send main bot stopped notification"
                            )
                return


def run_supervised_service(label, target, config=None, notifier=None,
                           api_guard=None, stop_event=None,
                           restart_delay=30, max_restart_delay=300):
    """Run a detector service with bounded restart backoff.

    Only keyword parameters accepted by the detector's run_service function
    are passed. This is important because the Fed and Tech service entry points
    intentionally have different signatures.
    """
    stop_event = stop_event or threading.Event()
    logger = _logging.getLogger("DeltaBot")
    delay = max(1, float(restart_delay))

    while not stop_event.is_set():
        try:
            signature = _inspect.signature(target)
            available = {
                "config": config,
                "notifier": notifier,
                "api_guard": api_guard,
                "stop_event": stop_event,
            }
            # Pass only supported named parameters; do not guess positional args.
            accepts_kwargs = any(
                p.kind == _inspect.Parameter.VAR_KEYWORD
                for p in signature.parameters.values()
            )
            kwargs = {
                key: value for key, value in available.items()
                if key in signature.parameters or accepts_kwargs
            }
            target(**kwargs)
            if stop_event.is_set():
                return
            logger.warning("[%s] service returned unexpectedly; restarting in %.0fs",
                           label, delay)
        except Exception as exc:
            logger.exception("[%s] service crashed: %s", label, exc)
            if notifier:
                try:
                    notifier.send_custom_alert(
                        f"[{label}] service error",
                        f"{label} service stopped unexpectedly and will retry.\n\nError: {exc}"
                    )
                except Exception:
                    logger.exception("[%s] could not send service error notification", label)

        if stop_event.wait(delay):
            return
        delay = min(float(max_restart_delay), delay * 2)
