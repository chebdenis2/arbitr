import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

from ethereal import AsyncRESTClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ethereal_vwap_bot")

__version__ = "0.1.0"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _as_decimal(x: Any) -> Decimal:
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _quantize_down(x: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return x
    # quantize to step size, rounding down
    q = (x / step).to_integral_value(rounding=ROUND_DOWN) * step
    return q


@dataclass(frozen=True)
class StrategyConfig:
    ticker: str = "SOLUSD"
    direction: str = "LONG"  # LONG|SHORT
    poll_interval_sec: int = 3
    # Candle timeframe used for "new candle" refresh logic (like original bot).
    timeframe: str = "1h"
    # VWAP anchor period (like original bot).
    anchor_period: str = "Session"  # Session|Week|Month|Year

    # One entry level
    entry_distance_long_pct: Decimal = Decimal("1.0")  # % from VWAP for LONG
    entry_distance_short_pct: Decimal = Decimal("1.5")  # % from VWAP for SHORT
    entry_quantity: Decimal = Decimal("0.001")  # base asset qty
    post_only: bool = True

    # One exit "bracket" (OCO TP+SL)
    tp_pct: Decimal = Decimal("1.5")  # % from VWAP
    sl_pct: Decimal = Decimal("4.0")  # % from VWAP
    exits_as_stop_market: bool = True

    # Safety
    pause_on_sl: bool = False
    max_trade_pages: int = 6  # VWAP from public trades: pages*limit trades
    trades_page_limit: int = 200  # API constraint: max 200
    vwap_recalc_threshold: Decimal = Decimal("0.0005")  # 0.05% like original bot


class EtherealVWAPStrategy:
    def __init__(self, client: AsyncRESTClient, cfg: StrategyConfig, subaccount_index: int = 0):
        self.client = client
        self.cfg = cfg

        d = (cfg.direction or "LONG").strip().upper()
        if d in {"L", "LONG"}:
            d = "LONG"
        elif d in {"S", "SHORT"}:
            d = "SHORT"
        if d not in {"LONG", "SHORT"}:
            raise ValueError("direction must be LONG or SHORT")
        self.direction = d

        # Ethereal constraint: clientOrderId max length is 32 chars.
        # Keep a short unique prefix and generate compact IDs per order.
        self.client_prefix = f"V1{self.direction[0]}{uuid.uuid4().hex[:6].upper()}"  # e.g. V1L12ABCD
        self.state_file = f"strategy_state_{self.direction}_{cfg.ticker}.json"
        self.config_file = f"strategy_config_{self.direction}_{cfg.ticker}.json"

        self.subaccount_index = subaccount_index
        self.subaccount_id: Optional[UUID] = None
        self.subaccount_name: Optional[str] = None
        self.product_id: Optional[UUID] = None

        self.product_tick_size: Decimal = Decimal("0")
        self.product_lot_size: Decimal = Decimal("0")
        self.product_min_qty: Decimal = Decimal("0")
        self.product_max_qty: Decimal = Decimal("0")

        self.state: Dict[str, Any] = {}
        self._load_state()

    async def initialize(self) -> None:
        # If the user already provided both identifiers, we can skip discovery.
        # Note: Ethereal uses the subaccount *name* (bytes/hex string) for signing.
        if not (self.subaccount_id and self.subaccount_name):
            subs = await self.client.subaccounts()
            if not subs:
                raise RuntimeError(
                    "No subaccounts found for this key. On Ethereal, a subaccount is created only after you "
                    "deposit USDe. Deposit (testnet: https://deposit.etherealtest.net, mainnet: https://deposit.ethereal.trade) "
                    "then re-run the bot."
                )
            idx = int(self.subaccount_index)
            if idx < 0 or idx >= len(subs):
                raise RuntimeError(f"subaccount_index={idx} is out of range. Found {len(subs)} subaccounts.")

            self.subaccount_id = subs[idx].id
            self.subaccount_name = subs[idx].name

        products = await self.client.products_by_ticker()
        if self.cfg.ticker not in products:
            raise RuntimeError(f"Unknown ticker {self.cfg.ticker}. Available: {', '.join(sorted(products.keys()))}")
        p = products[self.cfg.ticker]
        self.product_id = p.id
        # SDK models expose snake_case attributes (tick_size/lot_size); aliases are tickSize/lotSize.
        self.product_tick_size = _as_decimal(getattr(p, "tick_size", None) or getattr(p, "tickSize", "0") or "0")
        self.product_lot_size = _as_decimal(getattr(p, "lot_size", None) or getattr(p, "lotSize", "0") or "0")
        self.product_min_qty = _as_decimal(getattr(p, "min_quantity", None) or getattr(p, "minQuantity", "0") or "0")
        self.product_max_qty = _as_decimal(getattr(p, "max_quantity", None) or getattr(p, "maxQuantity", "0") or "0")

        logger.info(
            "Initialized: ticker=%s product_id=%s subaccount=%s (%s)",
            self.cfg.ticker,
            str(self.product_id),
            self.subaccount_name,
            str(self.subaccount_id),
        )
        # Note: linked signers are optional. The subaccount owner can trade without linking a separate signer.

    # ---------------------------
    # Persistence
    # ---------------------------
    def _load_state(self) -> None:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    self.state = json.load(f)
            except Exception:
                self.state = {}
        else:
            self.state = {}

        self.state.setdefault("entry_order_id", None)
        self.state.setdefault("entry_client_order_id", None)
        self.state.setdefault("exit_order_ids", [])
        self.state.setdefault("exit_orders", {"tp": None, "sl": None})
        self.state.setdefault("exit_group_id", None)
        self.state.setdefault("trading_paused", False)
        self.state.setdefault("pause_reason", None)
        self.state.setdefault("paused_at", None)
        self.state.setdefault("last_anchor_start_ms", 0)
        self.state.setdefault("last_candle_start_ms", 0)
        # VWAP accumulator from anchor (using trades VWAP: sum(price*qty)/sum(qty))
        self.state.setdefault("cum_pq", "0")
        self.state.setdefault("cum_q", "0")
        self.state.setdefault("last_trade_ts", 0)  # ms timestamp of last processed trade

    def _save_state(self) -> None:
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, default=str)

    # ---------------------------
    # Anchor + VWAP
    # ---------------------------
    @staticmethod
    def _timeframe_ms(tf: str) -> int:
        s = (tf or "").strip().lower()
        if not s:
            raise ValueError("timeframe is empty")
        # formats like 1m, 3m, 1h, 4h, 1d
        num = ""
        unit = ""
        for ch in s:
            if ch.isdigit():
                num += ch
            else:
                unit += ch
        if not num or not unit:
            raise ValueError(f"Unsupported timeframe '{tf}' (expected like 1h, 3m)")
        n = int(num)
        if unit == "m":
            return n * 60_000
        if unit == "h":
            return n * 3_600_000
        if unit == "d":
            return n * 86_400_000
        raise ValueError(f"Unsupported timeframe unit '{unit}' in '{tf}'")

    def _anchor_start(self, t: datetime) -> datetime:
        t = t.astimezone(timezone.utc)
        p = self.cfg.anchor_period
        if p == "Session":
            return t.replace(hour=0, minute=0, second=0, microsecond=0)
        if p == "Week":
            start = t - timedelta(days=t.weekday())
            return start.replace(hour=0, minute=0, second=0, microsecond=0)
        if p == "Month":
            return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if p == "Year":
            return t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        return t

    async def _fetch_public_trades_page(
        self, cursor: Optional[str]
    ) -> Tuple[list[dict], Optional[str], bool]:
        assert self.product_id is not None
        limit = int(self.cfg.trades_page_limit)
        # Ethereal API constraint (observed): limit must be <= 200
        if limit <= 0:
            limit = 200
        limit = min(limit, 200)
        params: Dict[str, Any] = {
            "productId": str(self.product_id),
            "order": "desc",
            "orderBy": "createdAt",
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor
        res = await self.client.prepare_and_send_request(
            "GET",
            "/v1/order/trade",
            params=params,
        )
        # res: {data: [...], hasNext: bool, nextCursor?: str}
        data = res.get("data") or []
        next_cursor = res.get("nextCursor")
        has_next = bool(res.get("hasNext"))
        return data, next_cursor, has_next

    async def _sync_vwap_from_trades(self, anchor_start_ms: int) -> Decimal:
        """Incrementally update VWAP accumulators from public trades since last_trade_ts."""
        last_ts = int(self.state.get("last_trade_ts") or 0)
        if last_ts <= 0:
            last_ts = anchor_start_ms

        cursor: Optional[str] = None
        pages = 0
        batch: list[dict] = []

        while pages < int(self.cfg.max_trade_pages):
            trades, next_cursor, has_next = await self._fetch_public_trades_page(cursor)
            if not trades:
                break

            stop = False
            for t in trades:
                ts = int(t.get("createdAt") or 0)
                if ts and ts <= last_ts:
                    stop = True
                    break
                if ts and ts < anchor_start_ms:
                    stop = True
                    break
                batch.append(t)

            pages += 1
            if stop or not has_next or not next_cursor:
                break
            cursor = next_cursor

        if batch:
            # trades were fetched in desc; process in chronological order
            batch.sort(key=lambda x: int(x.get("createdAt") or 0))
            cum_pq = _as_decimal(self.state.get("cum_pq") or "0")
            cum_q = _as_decimal(self.state.get("cum_q") or "0")
            max_ts = last_ts

            for t in batch:
                ts = int(t.get("createdAt") or 0)
                price = _as_decimal(t.get("price") or "0")
                qty = _as_decimal(t.get("filled") or "0")
                if ts <= 0 or price <= 0 or qty <= 0:
                    continue
                cum_pq += price * qty
                cum_q += qty
                if ts > max_ts:
                    max_ts = ts

            self.state["cum_pq"] = str(cum_pq)
            self.state["cum_q"] = str(cum_q)
            self.state["last_trade_ts"] = max_ts
            self._save_state()

        cum_q_now = _as_decimal(self.state.get("cum_q") or "0")
        if cum_q_now <= 0:
            return Decimal("NaN")
        return _as_decimal(self.state.get("cum_pq") or "0") / cum_q_now

    # ---------------------------
    # Market + rounding helpers
    # ---------------------------
    async def get_oracle_price(self) -> Decimal:
        assert self.product_id is not None
        prices = await self.client.list_market_prices(product_ids=[str(self.product_id)])
        if not prices:
            return Decimal("NaN")
        p = prices[0]
        return _as_decimal(getattr(p, "oraclePrice", "0") or "0")

    def _round_price(self, px: Decimal) -> Decimal:
        return _quantize_down(px, self.product_tick_size)

    def _round_qty(self, qty: Decimal) -> Decimal:
        q = _quantize_down(qty, self.product_lot_size)
        # Enforce product min/max constraints.
        if self.product_min_qty and q < self.product_min_qty:
            q = _quantize_down(self.product_min_qty, self.product_lot_size)
        if self.product_max_qty and q > self.product_max_qty:
            q = _quantize_down(self.product_max_qty, self.product_lot_size)
        return q

    def _levels(self, vwap: Decimal) -> Tuple[Decimal, Decimal, Decimal]:
        """(entry, tp, sl) based on VWAP and config."""
        if not (vwap == vwap) or vwap <= 0:
            return Decimal("NaN"), Decimal("NaN"), Decimal("NaN")

        p_entry = self.cfg.entry_distance_long_pct if self.direction == "LONG" else self.cfg.entry_distance_short_pct
        p_tp = self.cfg.tp_pct
        p_sl = self.cfg.sl_pct

        if self.direction == "LONG":
            entry = vwap * (Decimal("1") - p_entry / Decimal("100"))
            tp = vwap * (Decimal("1") + p_tp / Decimal("100"))
            sl = vwap * (Decimal("1") - p_sl / Decimal("100"))
        else:
            entry = vwap * (Decimal("1") + p_entry / Decimal("100"))
            tp = vwap * (Decimal("1") - p_tp / Decimal("100"))
            sl = vwap * (Decimal("1") + p_sl / Decimal("100"))

        return self._round_price(entry), self._round_price(tp), self._round_price(sl)

    # ---------------------------
    # Position + orders
    # ---------------------------
    async def _get_open_position(self) -> Optional[dict]:
        assert self.subaccount_id is not None and self.product_id is not None
        positions = await self.client.list_positions(
            subaccount_id=str(self.subaccount_id),
            product_ids=[str(self.product_id)],
            open=True,
        )
        if not positions:
            return None
        # pick the latest updated one
        p = sorted(positions, key=lambda x: getattr(x, "updatedAt", 0) or 0)[-1]
        return p.model_dump()

    async def _cancel_order_ids(self, ids: list[str]) -> None:
        if not ids:
            return
        try:
            await self.client.cancel_orders(
                order_ids=ids,
                subaccount=self.subaccount_name,
            )
        except Exception as e:
            logger.warning("Cancel orders failed (%s): %s", ids, e)

    async def _cancel_entry(self) -> None:
        oid = self.state.get("entry_order_id")
        if oid:
            await self._cancel_order_ids([oid])
        self.state["entry_order_id"] = None
        self.state["entry_client_order_id"] = None

    async def _cancel_exits(self) -> None:
        ids = list(self.state.get("exit_order_ids") or [])
        if ids:
            await self._cancel_order_ids(ids)
        self.state["exit_order_ids"] = []
        self.state["exit_orders"] = {"tp": None, "sl": None}
        self.state["exit_group_id"] = None

    async def _debug_linked_signers(self) -> None:
        """Optional helper: prints linked signers (does not affect trading)."""
        if not self.subaccount_id:
            return
        try:
            signers = await self.client.list_signers(subaccount_id=str(self.subaccount_id), limit=50)
        except Exception:
            return
        if not signers:
            return
        logger.info("Linked signers for subaccount %s:", str(self.subaccount_id))
        for s in signers:
            logger.info("  signer=%s status=%s expiresAt=%s", getattr(s, "signer", None), getattr(s, "status", None), getattr(s, "expires_at", None))

    def _mk_client_order_id(self, kind: str) -> str:
        """
        Generate a <=32 char client order id.
        kind: 'E' | 'TP' | 'SL'
        """
        kind = (kind or "").upper()
        if kind not in {"E", "TP", "SL"}:
            kind = "X"
        # Use last 9 digits of ms timestamp + 3 random hex chars.
        ts = int(time.time() * 1000) % 1_000_000_000
        rnd = uuid.uuid4().hex[:3].upper()
        cid = f"{self.client_prefix}{kind}{ts:09d}{rnd}"
        return cid[:32]

    async def _ensure_entry_order(self, entry_px: Decimal) -> None:
        if self.state.get("entry_order_id"):
            return
        qty = self._round_qty(self.cfg.entry_quantity)
        if qty <= 0 or entry_px <= 0:
            return

        side = 0 if self.direction == "LONG" else 1
        cid = self._mk_client_order_id("E")

        try:
            sender = getattr(getattr(self.client, "chain", None), "address", None)
            o = await self.client.create_order(
                order_type="LIMIT",
                product_id=self.product_id,
                ticker=self.cfg.ticker,
                side=side,
                quantity=float(qty),
                price=float(entry_px),
                post_only=bool(self.cfg.post_only),
                time_in_force="GTD",
                client_order_id=cid,
                sender=sender,
                subaccount=self.subaccount_name,
            )
            oid = str(getattr(o, "id"))
            self.state["entry_order_id"] = oid
            self.state["entry_client_order_id"] = cid
            self._save_state()
            logger.info("ENTRY placed: %s qty=%s px=%s (order_id=%s)", self.direction, qty, entry_px, oid)
        except Exception as e:
            logger.error("Failed to place ENTRY: %r", e)
            if "401" in str(e) or "Unauthorized" in str(e):
                logger.error(
                    "Got 401 Unauthorized. Check ETHEREAL_TESTNET/ETHEREAL_BASE_URL match, and that this key "
                    "is allowed to trade on this subaccount. Linked signers are only needed if trading via a separate signer."
                )

    async def _ensure_oco_exits(self, pos: dict, tp_px: Decimal, sl_px: Decimal) -> None:
        if self.state.get("exit_order_ids"):
            return

        size = _as_decimal(pos.get("size") or "0").copy_abs()
        qty = self._round_qty(size)
        if qty <= 0:
            return

        # Close direction
        exit_side = 1 if self.direction == "LONG" else 0
        group_id = str(uuid.uuid4())

        # stop_type: 0=GAIN (TP), 1=LOSS (SL)
        try:
            sender = getattr(getattr(self.client, "chain", None), "address", None)
            order_type = "MARKET" if self.cfg.exits_as_stop_market else "LIMIT"
            tp = await self.client.create_order(
                order_type=order_type,
                product_id=self.product_id,
                ticker=self.cfg.ticker,
                side=exit_side,
                quantity=float(qty),
                reduce_only=True,
                stop_type=0,
                stop_price=float(tp_px),
                price=(float(tp_px) if order_type == "LIMIT" else None),
                time_in_force="GTD",
                client_order_id=self._mk_client_order_id("TP"),
                group_id=group_id,
                group_contingency_type=1,  # OCO
                sender=sender,
                subaccount=self.subaccount_name,
            )
            sl = await self.client.create_order(
                order_type=order_type,
                product_id=self.product_id,
                ticker=self.cfg.ticker,
                side=exit_side,
                quantity=float(qty),
                reduce_only=True,
                stop_type=1,
                stop_price=float(sl_px),
                price=(float(sl_px) if order_type == "LIMIT" else None),
                time_in_force="GTD",
                client_order_id=self._mk_client_order_id("SL"),
                group_id=group_id,
                group_contingency_type=1,  # OCO
                sender=sender,
                subaccount=self.subaccount_name,
            )
            tp_id = str(getattr(tp, "id"))
            sl_id = str(getattr(sl, "id"))
            self.state["exit_order_ids"] = [tp_id, sl_id]
            self.state["exit_orders"] = {"tp": tp_id, "sl": sl_id}
            self.state["exit_group_id"] = group_id
            self._save_state()
            logger.info("EXITS placed (OCO): TP=%s SL=%s qty=%s group=%s", tp_px, sl_px, qty, group_id)
        except Exception as e:
            logger.error("Failed to place exits: %s", e)

    async def _detect_close_reason(self, exit_ids: list[str]) -> Optional[str]:
        """Return 'TP'|'SL'|None based on which exit filled."""
        if not exit_ids:
            return None
        try:
            filled: dict[str, str] = {}
            for oid in exit_ids:
                o = await self.client.get_order(id=oid)
                status = (getattr(o, "status", "") or "").upper()
                filled[oid] = status
            for oid, st in filled.items():
                if st != "FILLED":
                    continue
                eo = self.state.get("exit_orders") or {}
                if oid == eo.get("sl"):
                    return "SL"
                if oid == eo.get("tp"):
                    return "TP"
                # fallback if state is missing mapping
                return "TP"
            return None
        except Exception:
            return None

    def _pause_released(self, price: Decimal, vwap: Decimal) -> bool:
        if not self.state.get("trading_paused"):
            return True
        if not (price == price) or price <= 0 or not (vwap == vwap) or vwap <= 0:
            return False
        p = self.cfg.entry_distance_long_pct if self.direction == "LONG" else self.cfg.entry_distance_short_pct
        if p <= 0:
            return False
        # Same logic as your Bybit version: after SL, wait for opposite L1 touch
        if self.direction == "LONG":
            opposite_l1 = vwap * (Decimal("1") + p / Decimal("100"))
            return price >= opposite_l1
        opposite_l1 = vwap * (Decimal("1") - p / Decimal("100"))
        return price <= opposite_l1

    async def step(self) -> None:
        assert self.subaccount_id is not None and self.product_id is not None

        now = _utc_now()
        now_ms = _dt_to_ms(now)

        # Anchor-based VWAP (like original).
        anchor_start = self._anchor_start(now)
        anchor_start_ms = _dt_to_ms(anchor_start)
        prev_anchor = int(self.state.get("last_anchor_start_ms") or 0)
        if prev_anchor != anchor_start_ms:
            # Reset VWAP accumulators for new anchor period
            self.state["last_anchor_start_ms"] = anchor_start_ms
            self.state["cum_pq"] = "0"
            self.state["cum_q"] = "0"
            self.state["last_trade_ts"] = anchor_start_ms
            self._save_state()
            await self._cancel_entry()
            await self._cancel_exits()
            logger.info("New anchor period: %s", anchor_start.isoformat())

        # Candle boundary (timeframe) for "full refresh" logic (like original).
        tf_ms = self._timeframe_ms(self.cfg.timeframe)
        candle_start_ms = now_ms - (now_ms % tf_ms)
        is_new_candle = int(self.state.get("last_candle_start_ms") or 0) != candle_start_ms
        if is_new_candle:
            self.state["last_candle_start_ms"] = candle_start_ms
            self._save_state()
            logger.info("NEW CANDLE %s — full refresh", _ms_to_dt(candle_start_ms).isoformat())

        price = await self.get_oracle_price()
        vwap = await self._sync_vwap_from_trades(anchor_start_ms)
        entry_px, tp_px, sl_px = self._levels(vwap)

        pos = await self._get_open_position()
        has_pos = bool(pos and _as_decimal(pos.get("size") or "0").copy_abs() > 0)

        # Handle pause-after-SL
        if self.state.get("trading_paused"):
            if self._pause_released(price, vwap):
                self.state["trading_paused"] = False
                self.state["pause_reason"] = None
                self.state["paused_at"] = None
                self._save_state()
                logger.warning("PAUSE cleared: resume condition met.")
            else:
                logger.info(
                    "PAUSED: price=%s vwap=%s (waiting for opposite L1 touch)",
                    str(price),
                    str(vwap),
                )
                return

        if not has_pos:
            # If position is closed, clean exits and possibly set pause on SL
            exit_ids = list(self.state.get("exit_order_ids") or [])
            if exit_ids:
                reason = await self._detect_close_reason(exit_ids)
                await self._cancel_exits()
                if reason == "SL" and self.cfg.pause_on_sl:
                    self.state["trading_paused"] = True
                    self.state["pause_reason"] = "SL"
                    self.state["paused_at"] = _utc_now().isoformat()
                    self._save_state()
                    logger.warning("PAUSED: stop-loss hit. Will resume after opposite L1 touch.")
                    return

            # On every new candle, re-place entry at fresh level (like original).
            if is_new_candle:
                await self._cancel_entry()
            await self._ensure_entry_order(entry_px)
            return

        # Position exists:
        # - Cancel stale entry if still recorded
        if self.state.get("entry_order_id"):
            await self._cancel_entry()
            self._save_state()

        # Ensure exits exist (OCO TP+SL). On each new candle we refresh exits to follow VWAP.
        if is_new_candle:
            await self._cancel_exits()
        await self._ensure_oco_exits(pos, tp_px, sl_px)

    async def run(self) -> None:
        logger.info("Starting VWAP strategy: %s %s", self.direction, self.cfg.ticker)
        while True:
            try:
                await self.step()
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error("Loop error: %s", e)
            await asyncio.sleep(int(self.cfg.poll_interval_sec))


def _load_or_create_config(path: str) -> tuple[StrategyConfig, bool]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        trades_page_limit = int(raw.get("trades_page_limit", 200))
        # clamp to API max
        trades_page_limit = min(max(trades_page_limit, 1), 200)
        return (
            StrategyConfig(
            ticker=str(raw.get("ticker", "SOLUSD")),
            direction=str(raw.get("direction", "LONG")).upper(),
            poll_interval_sec=int(raw.get("poll_interval_sec", 3)),
            timeframe=str(raw.get("timeframe", "1h")),
            anchor_period=str(raw.get("anchor_period", "Session")),
            entry_distance_long_pct=_as_decimal(raw.get("entry_distance_long_pct", "1.0")),
            entry_distance_short_pct=_as_decimal(raw.get("entry_distance_short_pct", "1.5")),
            entry_quantity=_as_decimal(raw.get("entry_quantity", "0.001")),
            post_only=bool(raw.get("post_only", True)),
            tp_pct=_as_decimal(raw.get("tp_pct", "1.5")),
            sl_pct=_as_decimal(raw.get("sl_pct", "4.0")),
            exits_as_stop_market=bool(raw.get("exits_as_stop_market", True)),
            pause_on_sl=bool(raw.get("pause_on_sl", False)),
            max_trade_pages=int(raw.get("max_trade_pages", 6)),
            trades_page_limit=trades_page_limit,
            vwap_recalc_threshold=_as_decimal(raw.get("vwap_recalc_threshold", "0.0005")),
            ),
            False,
        )

    cfg = StrategyConfig()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "ticker": cfg.ticker,
                "direction": cfg.direction,
                "poll_interval_sec": cfg.poll_interval_sec,
                "timeframe": cfg.timeframe,
                "anchor_period": cfg.anchor_period,
                "entry_distance_long_pct": str(cfg.entry_distance_long_pct),
                "entry_distance_short_pct": str(cfg.entry_distance_short_pct),
                "entry_quantity": str(cfg.entry_quantity),
                "post_only": cfg.post_only,
                "tp_pct": str(cfg.tp_pct),
                "sl_pct": str(cfg.sl_pct),
                "exits_as_stop_market": cfg.exits_as_stop_market,
                "pause_on_sl": cfg.pause_on_sl,
                "max_trade_pages": cfg.max_trade_pages,
                "trades_page_limit": cfg.trades_page_limit,
                "vwap_recalc_threshold": str(cfg.vwap_recalc_threshold),
            },
            f,
            indent=2,
        )
    logger.info("Config created: %s (edit it and re-run)", path)
    return cfg, True


async def amain() -> None:
    testnet = (os.getenv("ETHEREAL_TESTNET", "1").strip().lower() in {"1", "true", "y", "yes"})
    network = "testnet" if testnet else "mainnet"
    base_url = os.getenv("ETHEREAL_BASE_URL") or ("https://api.etherealtest.net" if testnet else "https://api.ethereal.trade")
    rpc_url = os.getenv("ETHEREAL_RPC_URL") or ("https://rpc.etherealtest.net" if testnet else "https://rpc.ethereal.trade")
    private_key = (os.getenv("ETHEREAL_PRIVATE_KEY") or "").strip()
    if not private_key:
        raise RuntimeError("Set ETHEREAL_PRIVATE_KEY (EVM private key) in env.")

    # Config file (auto-created if missing)
    ticker = (os.getenv("ETHEREAL_TICKER") or "SOLUSD").strip().upper()
    direction = (os.getenv("ETHEREAL_DIRECTION") or "LONG").strip().upper()
    config_path = f"strategy_config_{direction}_{ticker}.json"
    cfg, created = _load_or_create_config(config_path)
    if created:
        # Avoid continuing with defaults on first run (prevents confusing errors).
        return

    # allow env overrides (optional)
    if os.getenv("ETHEREAL_ENTRY_QTY"):
        cfg = StrategyConfig(**{**cfg.__dict__, "entry_quantity": _as_decimal(os.getenv("ETHEREAL_ENTRY_QTY"))})

    sub_idx = int(os.getenv("ETHEREAL_SUBACCOUNT_INDEX", "0"))

    client = await AsyncRESTClient.create(
        {
            "network": network,
            "base_url": base_url,
            "chain_config": {
                "rpc_url": rpc_url,
                "private_key": private_key,
            },
        }
    )
    try:
        strat = EtherealVWAPStrategy(client, cfg, subaccount_index=sub_idx)
        try:
            await strat.initialize()
        except RuntimeError as e:
            logger.error(str(e))
            return
        await strat.run()
    finally:
        await client.close()


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    main()

