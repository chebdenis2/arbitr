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

import httpx
from ethereal import AsyncRESTClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ethereal_vwap_bot")

__version__ = "0.2.0"

MULTI_CONFIG_FILE = "strategy_config_multi.json"


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


def _is_pos_finite_decimal(x: Decimal) -> bool:
    """True if x is a finite Decimal > 0 (guards NaN comparisons)."""
    try:
        if not (x == x):
            return False
        return x > 0
    except Exception:
        return False


def _is_order_not_found_error(e: Exception) -> bool:
    s = str(e) or ""
    return ("404" in s and "Not Found" in s) or ("Order not found" in s)


def _is_connect_error(e: Exception) -> bool:
    # SDK requests use httpx/httpcore; be robust to both.
    if isinstance(e, httpx.ConnectError):
        return True
    name = type(e).__name__
    return "ConnectError" in name or "NetworkError" in name or "ReadTimeout" in name or "Timeout" in name


def _norm_status(x: Any) -> str:
    """
    Normalize status values coming from SDK/enums to a plain uppercase token.
    Examples:
      - Status.canceled -> CANCELED
      - STATUS.NEW -> NEW
      - "filled" -> FILLED
    """
    try:
        v = getattr(x, "value", x)
        s = str(v or "").strip()
        if not s:
            return ""
        # Common SDK enum stringification: "Status.filled", "STATUS.NEW"
        if "." in s:
            s = s.split(".")[-1]
        return s.strip().upper()
    except Exception:
        return ""


@dataclass(frozen=True)
class StrategyConfig:
    # Strategy selection:
    # - "vwap" (default): existing logic (limit entry at VWAP ± distance + OCO exits)
    # - "anchor_open": new logic (market entry on new anchor + TP to last VWAP of prev anchor + SL from TP size + close at anchor end)
    strategy: str = "vwap"

    # Trailing stop (applies to both strategies when enabled):
    # For each new candle, look at the previous candle BODY:
    #   body = abs(close - open)
    # - If trailing_candle_filter == "opposite" (original rule):
    #   - LONG: act only if prev candle is bearish (close < open) => tighten SL by (open-close)
    #   - SHORT: act only if prev candle is bullish (close > open) => tighten SL by (close-open)
    # - If trailing_candle_filter == "all" (test mode):
    #   - LONG: always tighten SL by abs(close-open)
    #   - SHORT: always tighten SL by abs(close-open)
    trailing_stop_enabled: bool = False
    trailing_candle_filter: str = "all"  # "all" | "opposite"
    # Which price stream to build candles for trailing stop:
    # - "market": use Ethereal market/oracle/mark/index/last price (recommended)
    # - "ref": use ref_price (may fall back to VWAP when market price is unavailable)
    trailing_candle_price_source: str = "market"
    # Where to take previous candle open/close for trailing:
    # - "state": use candle tracking from ref_price sampling (default)
    # - "bybit_klines": fetch previous candle OHLC from Bybit (more reliable when vwap_source=bybit_klines)
    # - "auto": use state, fallback to bybit_klines when missing
    trailing_prev_candle_source: str = "auto"
    # Extra verbose logs for trailing stop and candle tracking.
    trailing_debug_logs: bool = False

    ticker: str = "SOLUSD"
    direction: str = "LONG"  # LONG|SHORT|BOTH
    # VWAP auto-direction (direction="BOTH"): choose LONG/SHORT per anchor if the
    # new anchor open differs from previous anchor VWAP by this threshold (%).
    vwap_both_min_delta_pct: Decimal = Decimal("0.1")
    # Optional max delta threshold (%). If > 0, signals above this are skipped.
    # Example: min=0.1, max=0.3 -> accept deltas in [0.1, 0.3].
    vwap_both_max_delta_pct: Decimal = Decimal("0")
    # VWAP auto-direction startup hint when there is no prev_anchor_vwap yet:
    # "LONG" | "SHORT" | "NONE" (default waits for first completed anchor).
    vwap_both_start_direction: str = "NONE"
    # VWAP auto-direction mode for BOTH:
    # - "trend": LONG when open > prev VWAP, SHORT when open < prev VWAP (default)
    # - "countertrend": SHORT when open > prev VWAP, LONG when open < prev VWAP
    vwap_both_mode: str = "trend"
    poll_interval_sec: int = 3
    # Candle timeframe used for "new candle" refresh logic (like original bot).
    timeframe: str = "1h"
    # VWAP anchor period (like original bot).
    anchor_period: str = "Session"  # Session|Week|Month|Year|Hour|2 Hours|4 Hours|8 Hours|12 Hours

    # One entry level
    entry_distance_long_pct: Decimal = Decimal("1.0")  # % from VWAP for LONG
    entry_distance_short_pct: Decimal = Decimal("1.5")  # % from VWAP for SHORT
    entry_quantity: Decimal = Decimal("0.001")  # base asset qty
    post_only: bool = True
    # If post_only order is rejected as ImmediateMatchPostOnly, reprice away from market and retry.
    post_only_reprice_on_reject: bool = True
    post_only_reprice_max_attempts: int = 3
    post_only_reprice_ticks: int = 2
    entry_expires_in_sec: int = 7 * 24 * 3600  # GTD expiry (seconds)

    # One exit "bracket" (OCO TP+SL)
    tp_pct: Decimal = Decimal("1.5")  # % from VWAP
    sl_pct: Decimal = Decimal("4.0")  # % from VWAP
    exits_as_stop_market: bool = True
    exit_expires_in_sec: int = 7 * 24 * 3600  # GTD expiry (seconds)

    # Safety
    pause_on_sl: bool = False
    # New pause model (recommended): pause trading for N minutes after SL.
    # If 0 -> disabled (unless pause_on_sl=true, which enables a default pause as backward compatibility).
    pause_after_sl_minutes: int = 0
    # Optional: do not trigger pause immediately on startup due to historical stop fills.
    # If true, the bot will mark all reduce-only stop fills up to startup time as "already handled".
    pause_skip_on_startup: bool = False
    # Optional: ignore SL-based pause until the first real position opens after startup.
    pause_ignore_sl_until_first_open: bool = False

    # ---------------------------
    # Strategy: anchor_open
    # ---------------------------
    # Open trade only if abs(delta) >= threshold (%), where:
    #   delta_pct = abs((open_price_new_anchor - last_vwap_prev_anchor) / last_vwap_prev_anchor) * 100
    # NOTE: open_price_new_anchor is the first oracle price seen after anchor switch.
    #       last_vwap_prev_anchor is the last *valid* VWAP computed during the previous anchor.
    anchor_open_min_delta_pct: Decimal = Decimal("0")
    # Stop-loss size as % of TP size, where:
    #   tp_size = abs(first_vwap_new_anchor - last_vwap_prev_anchor)
    anchor_open_sl_pct_of_tp: Decimal = Decimal("100")
    # Close any open position at anchor end (recommended).
    anchor_open_close_on_anchor_end: bool = True

    max_trade_pages: int = 6  # VWAP from public trades: pages*limit trades
    trades_page_limit: int = 200  # API constraint: max 200
    vwap_recalc_threshold: Decimal = Decimal("0.0005")  # 0.05% like original bot
    # VWAP robustness under high trade throughput:
    # - overlap_ms: re-scan a small recent window to avoid missing same-ms trades
    # - overflow_max_trade_pages: temporary higher scan budget when backlog is too large
    trade_overlap_ms: int = 2000
    trade_id_cache_size: int = 5000
    overflow_max_trade_pages: int = 50

    # VWAP data source:
    # - ethereal_trades: VWAP from Ethereal public trades (default)
    # - bybit_klines: VWAP from Bybit kline bars using HLC3*volume (closer to TradingView)
    vwap_source: str = "ethereal_trades"
    bybit_base_url: str = "https://api.bybit.com"
    bybit_category: str = "linear"  # linear|inverse|spot
    bybit_symbol: str = "SOLUSDT"
    bybit_kline_interval: str = "1"  # minutes: 1,3,5,15,30,60,120,240,360,720, D,W,M


class EtherealVWAPStrategy:
    @staticmethod
    def _normalize_direction_value(raw: Any) -> Optional[str]:
        s = str(raw or "").strip().upper()
        if s in {"L", "LONG"}:
            return "LONG"
        if s in {"S", "SHORT"}:
            return "SHORT"
        if s in {"B", "BOTH", "AUTO"}:
            return "BOTH"
        return None

    @staticmethod
    def _normalize_position_side(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        if isinstance(raw, bool):
            return None
        if isinstance(raw, (int, float, Decimal)):
            try:
                v = int(raw)
            except Exception:
                v = None
            if v == 0:
                return "LONG"
            if v == 1:
                return "SHORT"
        s = str(raw).strip().upper()
        if s in {"0", "LONG", "L", "BUY", "BID"}:
            return "LONG"
        if s in {"1", "SHORT", "S", "SELL", "ASK"}:
            return "SHORT"
        return None

    def __init__(self, client: AsyncRESTClient, cfg: StrategyConfig, subaccount_index: int = 0):
        self.client = client
        self.cfg = cfg
        self.started_at_ms = _dt_to_ms(_utc_now())

        d = self._normalize_direction_value(cfg.direction or "LONG")
        if d not in {"LONG", "SHORT", "BOTH"}:
            raise ValueError("direction must be LONG, SHORT, or BOTH")
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

        # Optional: skip pause-on-startup caused by historical reduce-only stop fills.
        # We do it by moving last_handled_reduce_stop_ts forward to startup time.
        if bool(getattr(self.cfg, "pause_skip_on_startup", False)):
            try:
                last_ts = int(self.state.get("last_handled_reduce_stop_ts") or 0)
            except Exception:
                last_ts = 0
            if last_ts < int(self.started_at_ms):
                self.state["startup_ms"] = int(self.started_at_ms)
                self.state["last_handled_reduce_stop_ts"] = int(self.started_at_ms)
                self.state["last_handled_reduce_stop_order_id"] = str(self.state.get("last_handled_reduce_stop_order_id") or "startup")
                self._save_state()
                logger.info(
                    "Startup pause-skip enabled: ticker=%s strategy=%s last_handled_reduce_stop_ts=%s",
                    self.cfg.ticker,
                    self._cfg_strategy(),
                    int(self.started_at_ms),
                )

        # Optional: ignore SL pause until the first real position opens after startup.
        if bool(getattr(self.cfg, "pause_ignore_sl_until_first_open", False)):
            self.state["opened_since_startup"] = False
            self.state["opened_since_startup_ms"] = int(self.started_at_ms)
            self._save_state()

    async def initialize(
        self,
        *,
        subaccount_id: Optional[UUID] = None,
        subaccount_name: Optional[str] = None,
        products_by_ticker: Optional[dict] = None,
    ) -> None:
        # If the user already provided both identifiers, we can skip discovery.
        # Note: Ethereal uses the subaccount *name* (bytes/hex string) for signing.
        if subaccount_id and subaccount_name:
            self.subaccount_id = subaccount_id
            self.subaccount_name = subaccount_name

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

        products = products_by_ticker or (await self.client.products_by_ticker())
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
        # Best-effort cached exit trigger levels (for trailing SL logic without extra API calls).
        self.state.setdefault("exit_levels", {"tp": None, "sl": None})  # strings (Decimals)
        self.state.setdefault("exit_group_id", None)
        # When exits were last placed (ms since epoch), used to avoid reacting to transient 404s.
        self.state.setdefault("exit_placed_ms", 0)
        # Best-effort exit quantity tracking (used for anchor_open immediate protection).
        self.state.setdefault("exit_qty", None)  # str Decimal
        self.state.setdefault("trading_paused", False)
        self.state.setdefault("pause_reason", None)
        self.state.setdefault("pause_source", None)
        self.state.setdefault("pause_strategy", None)
        self.state.setdefault("pause_trade_direction", None)
        self.state.setdefault("paused_at", None)
        # Pause timer (new model): pause_until_ts is a unix timestamp (seconds).
        self.state.setdefault("pause_until_ts", 0)
        self.state.setdefault("pause_duration_min", 0)
        # Throttle pause logs.
        self.state.setdefault("pause_last_log_ms", 0)
        # Backward-compat state key from old pause model (price-touch). Kept for safe migration.
        self.state.setdefault("pause_release_price", None)
        self.state.setdefault("last_anchor_start_ms", 0)
        self.state.setdefault("last_candle_start_ms", 0)
        # VWAP accumulator from anchor (using trades VWAP: sum(price*qty)/sum(qty))
        self.state.setdefault("cum_pq", "0")
        self.state.setdefault("cum_q", "0")
        self.state.setdefault("last_trade_ts", 0)  # ms timestamp of last processed trade
        # Rolling cache of processed trade ids (strings) to dedupe overlap rescans.
        self.state.setdefault("recent_trade_ids", [])
        # For robust position transition detection.
        self.state.setdefault("last_position_size", "0")
        # How many consecutive step() calls saw no open position (used for safe stale-exit cleanup).
        self.state.setdefault("no_position_streak", 0)
        self.state.setdefault("last_close_reason", None)
        self.state.setdefault("last_close_at", None)
        # Robust close detection via recent FILLED reduce-only stop orders (for pause-after-SL).
        self.state.setdefault("last_handled_reduce_stop_order_id", None)
        self.state.setdefault("last_handled_reduce_stop_ts", 0)  # ms
        # Process startup time (ms) for optional pause-skip behavior.
        self.state.setdefault("startup_ms", 0)
        # Track first open after startup (used to optionally ignore SL pause until a real open).
        self.state.setdefault("opened_since_startup", False)
        self.state.setdefault("opened_since_startup_ms", 0)
        # Throttle repeated network error logs.
        self.state.setdefault("last_connect_error_log_ms", 0)
        # Throttle unknown-close diagnostics.
        self.state.setdefault("pause_diag_last_log_ms", 0)

    def _log_network_warning(self, where: str, e: Exception) -> None:
        now_ms = _dt_to_ms(_utc_now())
        last_log = int(self.state.get("last_connect_error_log_ms") or 0)
        if last_log and (now_ms - last_log) < 30_000:
            return
        self.state["last_connect_error_log_ms"] = now_ms
        self._save_state()
        logger.warning("Network error in %s (%s %s): %s", where, self.direction, self.cfg.ticker, type(e).__name__)

        # Candle tracking for trailing stop (prices use reference price from get_oracle_price()).
        self.state.setdefault("candle_open_price", None)  # str Decimal
        self.state.setdefault("candle_close_price", None)  # str Decimal
        self.state.setdefault("candle_high_price", None)  # str Decimal
        self.state.setdefault("candle_low_price", None)  # str Decimal
        self.state.setdefault("candle_start_ms", 0)
        self.state.setdefault("prev_candle_open_price", None)  # str Decimal
        self.state.setdefault("prev_candle_close_price", None)  # str Decimal
        self.state.setdefault("prev_candle_high_price", None)  # str Decimal
        self.state.setdefault("prev_candle_low_price", None)  # str Decimal
        self.state.setdefault("prev_candle_start_ms", 0)
        # Last known oracle price snapshot (for robust anchor transitions in anchor_open).
        self.state.setdefault("last_price", None)  # str Decimal
        self.state.setdefault("last_price_ms", 0)
        self.state.setdefault("last_price_anchor_ms", 0)

        # Last known VWAP snapshot (independent of source) for anchor_open TP.
        self.state.setdefault("last_vwap", None)  # str Decimal
        self.state.setdefault("last_vwap_ms", 0)
        self.state.setdefault("last_vwap_anchor_ms", 0)
        # Last VWAP within *current* anchor (monotonic update while anchor is active).
        # This is the "last VWAP of anchor" persisted for reliable rollover.
        self.state.setdefault("anchor_last_vwap", None)  # str Decimal
        self.state.setdefault("anchor_last_vwap_ms", 0)
        self.state.setdefault("anchor_last_vwap_anchor_ms", 0)

        # Anchor-open strategy state
        self.state.setdefault("prev_anchor_vwap", None)  # str Decimal (TP reference)
        self.state.setdefault("prev_anchor_start_ms", 0)
        self.state.setdefault("anchor_open_open_price", None)  # str Decimal
        self.state.setdefault("anchor_open_open_price_ms", 0)
        self.state.setdefault("anchor_open_decision_anchor_ms", 0)  # anchor_start_ms where decision (opened/skipped) was made
        self.state.setdefault("anchor_open_active", False)
        self.state.setdefault("anchor_open_direction", None)  # LONG|SHORT
        self.state.setdefault("anchor_open_tp_price", None)  # str Decimal (close price prev anchor)
        self.state.setdefault("anchor_open_sl_price", None)  # str Decimal
        self.state.setdefault("anchor_open_tp_size", None)  # str Decimal
        self.state.setdefault("anchor_open_delta_pct", None)  # str Decimal
        self.state.setdefault("anchor_open_opened_at", None)
        self.state.setdefault("anchor_open_closed_at", None)
        self.state.setdefault("anchor_open_close_reason", None)  # TP|SL|ANCHOR_END|MANUAL|UNKNOWN
        self.state.setdefault("anchor_open_last_wait_log_ms", 0)

        # VWAP auto-direction (direction="BOTH")
        self.state.setdefault("vwap_anchor_open_price", None)  # str Decimal
        self.state.setdefault("vwap_anchor_open_price_ms", 0)
        self.state.setdefault("vwap_anchor_decision_anchor_ms", 0)
        self.state.setdefault("vwap_anchor_direction", None)  # LONG|SHORT|None
        self.state.setdefault("vwap_anchor_delta_pct", None)  # str Decimal
        self.state.setdefault("vwap_anchor_last_wait_log_ms", 0)
        self.state.setdefault("vwap_trade_direction", None)  # LONG|SHORT
        self.state.setdefault("vwap_direction_unknown_log_ms", 0)
        self.state.setdefault("vwap_last_auto_direction", None)  # LONG|SHORT
        self.state.setdefault("vwap_start_direction_used_anchor_ms", 0)

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
        p_raw = (self.cfg.anchor_period or "").strip()
        p = p_raw.lower()

        # TradingView-style rolling anchors (timeframe.change):
        #   "Hour"     => timeframe.change("60")
        #   "2 Hours"  => timeframe.change("120")
        #   "4 Hours"  => timeframe.change("240")
        #   "8 Hours"  => timeframe.change("480")
        #   "12 Hours" => timeframe.change("720")
        hour_map = {
            "hour": 1,
            "1 hour": 1,
            "2 hours": 2,
            "4 hours": 4,
            "8 hours": 8,
            "12 hours": 12,
            # Common shorthand aliases
            "1h": 1,
            "2h": 2,
            "4h": 4,
            "8h": 8,
            "12h": 12,
        }
        if p in hour_map:
            interval_ms = hour_map[p] * 3_600_000
            t_ms = _dt_to_ms(t)
            start_ms = t_ms - (t_ms % interval_ms)
            return _ms_to_dt(start_ms)

        if p == "session":
            return t.replace(hour=0, minute=0, second=0, microsecond=0)
        if p == "week":
            start = t - timedelta(days=t.weekday())
            return start.replace(hour=0, minute=0, second=0, microsecond=0)
        if p == "month":
            return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if p == "year":
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

    async def _vwap_from_bybit_klines(self, anchor_start_ms: int, now_ms: int) -> Decimal:
        """
        Compute anchored VWAP using Bybit klines:
          VWAP = sum(typical_price * volume) / sum(volume)
        where typical_price = (high + low + close) / 3.

        This tends to match TradingView's VWAP behavior more closely than trade-tape VWAP,
        and uses the same market feed as TV when the chart is Bybit.
        """
        base_url = (self.cfg.bybit_base_url or "https://api.bybit.com").rstrip("/")
        url = f"{base_url}/v5/market/kline"

        start = int(anchor_start_ms)
        end = int(now_ms)
        if end <= start:
            return Decimal("NaN")

        sum_pv = Decimal("0")
        sum_v = Decimal("0")

        # Bybit v5 returns up to 200 klines per call, newest-first.
        # We'll page forward by moving start time.
        limit = 200
        interval = str(self.cfg.bybit_kline_interval or "1")
        category = str(self.cfg.bybit_category or "linear")
        symbol = str(self.cfg.bybit_symbol or "SOLUSDT")

        async with httpx.AsyncClient(timeout=10) as c:
            cur = start
            safety = 0
            while cur < end and safety < 200:
                params = {
                    "category": category,
                    "symbol": symbol,
                    "interval": interval,
                    "start": cur,
                    "end": end,
                    "limit": limit,
                }
                r = await c.get(url, params=params)
                r.raise_for_status()
                payload = r.json()
                if str(payload.get("retCode")) != "0":
                    raise RuntimeError(f"Bybit error retCode={payload.get('retCode')} retMsg={payload.get('retMsg')}")
                lst = (((payload.get("result") or {}).get("list")) or [])
                if not lst:
                    break

                # Each item: [startTime, open, high, low, close, volume, turnover]
                # Typically newest-first; sort by time ascending for stable pagination.
                rows = sorted(lst, key=lambda x: int(x[0]))
                last_t = None
                for it in rows:
                    try:
                        t0 = int(it[0])
                        h = _as_decimal(it[2])
                        l = _as_decimal(it[3])
                        cl = _as_decimal(it[4])
                        v = _as_decimal(it[5])
                    except Exception:
                        continue
                    if t0 < start or t0 > end:
                        continue
                    if v <= 0:
                        last_t = t0
                        continue
                    tp = (h + l + cl) / Decimal("3")
                    sum_pv += tp * v
                    sum_v += v
                    last_t = t0

                if last_t is None:
                    break
                # Advance start; +1ms to avoid repeating the last candle.
                cur = int(last_t) + 1
                safety += 1

        if sum_v <= 0:
            return Decimal("NaN")
        return sum_pv / sum_v

    async def _get_vwap(self, anchor_start_ms: int, now_ms: int) -> Decimal:
        src = (self.cfg.vwap_source or "ethereal_trades").strip().lower()
        if src == "bybit_klines":
            return await self._vwap_from_bybit_klines(anchor_start_ms, now_ms)
        return await self._sync_vwap_from_trades(anchor_start_ms)

    async def _sync_vwap_from_trades(self, anchor_start_ms: int) -> Decimal:
        """
        Incrementally update VWAP accumulators from public trades.

        Note: On sharp moves, there can be more than max_trade_pages*limit trades between polls.
        To reduce drift we:
        - re-scan a small overlap window and dedupe by trade id
        - if we detect overflow, re-run once with overflow_max_trade_pages
        """

        async def fetch_batch(pages_limit: int) -> tuple[list[dict], bool]:
            last_ts_local = int(self.state.get("last_trade_ts") or 0)
            if last_ts_local <= 0:
                last_ts_local = anchor_start_ms
            cutoff_ts = max(anchor_start_ms, last_ts_local - int(self.cfg.trade_overlap_ms))

            cursor_local: Optional[str] = None
            pages_local = 0
            batch_local: list[dict] = []
            reached_cutoff = False
            saw_more = False

            while pages_local < int(pages_limit):
                trades, next_cursor, has_next = await self._fetch_public_trades_page(cursor_local)
                if not trades:
                    break

                stop = False
                for tr in trades:
                    ts = int(tr.get("createdAt") or 0)
                    if ts and ts < cutoff_ts:
                        stop = True
                        reached_cutoff = True
                        break
                    batch_local.append(tr)

                pages_local += 1
                if stop:
                    break
                if not has_next or not next_cursor:
                    reached_cutoff = True
                    break
                cursor_local = next_cursor
                saw_more = True

            # Overflow = we exhausted our page budget without reaching cutoff, but API indicates more pages exist.
            overflow = bool(saw_more and (not reached_cutoff) and pages_local >= int(pages_limit))
            return batch_local, overflow

        batch, overflow = await fetch_batch(int(self.cfg.max_trade_pages))
        if overflow and int(self.cfg.overflow_max_trade_pages) > int(self.cfg.max_trade_pages):
            logger.warning(
                "VWAP trade backlog overflow (pages=%s, limit=%s). Re-scanning with overflow_max_trade_pages=%s.",
                int(self.cfg.max_trade_pages),
                int(self.cfg.trades_page_limit),
                int(self.cfg.overflow_max_trade_pages),
            )
            batch, _ = await fetch_batch(int(self.cfg.overflow_max_trade_pages))

        if batch:
            # trades were fetched in desc; process in chronological order
            batch.sort(key=lambda x: int(x.get("createdAt") or 0))

            recent_ids_list = list(self.state.get("recent_trade_ids") or [])
            recent_ids = set(str(x) for x in recent_ids_list)

            cum_pq = _as_decimal(self.state.get("cum_pq") or "0")
            cum_q = _as_decimal(self.state.get("cum_q") or "0")
            max_ts = int(self.state.get("last_trade_ts") or 0) or anchor_start_ms

            for tr in batch:
                tid = str(tr.get("id") or "")
                if not tid or tid in recent_ids:
                    continue
                ts = int(tr.get("createdAt") or 0)
                price = _as_decimal(tr.get("price") or "0")
                qty = _as_decimal(tr.get("filled") or "0")
                if ts <= 0 or ts < anchor_start_ms or price <= 0 or qty <= 0:
                    continue
                cum_pq += price * qty
                cum_q += qty
                if ts > max_ts:
                    max_ts = ts
                recent_ids.add(tid)
                recent_ids_list.append(tid)

            # Keep only last N ids to bound memory/state file size.
            keep_n = max(0, int(self.cfg.trade_id_cache_size))
            if keep_n and len(recent_ids_list) > keep_n:
                recent_ids_list = recent_ids_list[-keep_n:]

            self.state["cum_pq"] = str(cum_pq)
            self.state["cum_q"] = str(cum_q)
            self.state["last_trade_ts"] = int(max_ts)
            self.state["recent_trade_ids"] = recent_ids_list
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
        # Retry on transient network errors.
        for attempt in range(3):
            try:
                prices = await self.client.list_market_prices(product_ids=[str(self.product_id)])
                if not prices:
                    return Decimal("NaN")
                p = prices[0]
                # Not all instruments have oraclePrice populated. Fallback to other common fields.
                # We still return a "reference price" used for anchor_open open-price capture.
                for k in (
                    "oraclePrice",
                    "oracle_price",
                    "markPrice",
                    "mark_price",
                    "indexPrice",
                    "index_price",
                    "midPrice",
                    "mid_price",
                    "lastPrice",
                    "last_price",
                    "price",
                ):
                    try:
                        v = _as_decimal(getattr(p, k, None) or "0")
                    except Exception:
                        continue
                    if _is_pos_finite_decimal(v):
                        return v
                return Decimal("NaN")
            except Exception as e:
                if not _is_connect_error(e):
                    raise
                await asyncio.sleep(0.5 * (2**attempt))

        # If we still can't fetch, fall back to last known reference price in state.
        try:
            last = _as_decimal(self.state.get("last_price") or "0")
        except Exception:
            last = Decimal("NaN")
        if _is_pos_finite_decimal(last):
            return last
        return Decimal("NaN")

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

    def _levels(self, vwap: Decimal, *, direction: Optional[str] = None) -> Tuple[Decimal, Decimal, Decimal]:
        """(entry, tp, sl) based on VWAP and config."""
        if not _is_pos_finite_decimal(vwap):
            return Decimal("NaN"), Decimal("NaN"), Decimal("NaN")

        d = self._normalize_direction_value(direction or self.direction)
        if d not in {"LONG", "SHORT"}:
            return Decimal("NaN"), Decimal("NaN"), Decimal("NaN")

        p_entry = self.cfg.entry_distance_long_pct if d == "LONG" else self.cfg.entry_distance_short_pct
        p_tp = self.cfg.tp_pct
        p_sl = self.cfg.sl_pct

        if d == "LONG":
            entry = vwap * (Decimal("1") - p_entry / Decimal("100"))
            tp = vwap * (Decimal("1") + p_tp / Decimal("100"))
            sl = vwap * (Decimal("1") - p_sl / Decimal("100"))
        else:
            entry = vwap * (Decimal("1") + p_entry / Decimal("100"))
            tp = vwap * (Decimal("1") - p_tp / Decimal("100"))
            sl = vwap * (Decimal("1") + p_sl / Decimal("100"))

        return self._round_price(entry), self._round_price(tp), self._round_price(sl)

    def _opposite_entry_level(self, vwap: Decimal, *, direction: Optional[str] = None) -> Decimal:
        """
        Entry level for the *opposite* direction (used as pause-release condition).

        Example:
        - Strategy SHORT: after SL, pause until price touches LONG entry level.
        - Strategy LONG:  after SL, pause until price touches SHORT entry level.
        """
        if not (vwap == vwap) or vwap <= 0:
            return Decimal("NaN")

        d = self._normalize_direction_value(direction or self.direction)
        if d not in {"LONG", "SHORT"}:
            return Decimal("NaN")
        if d == "SHORT":
            # LONG entry: below VWAP using long distance.
            p = self.cfg.entry_distance_long_pct
            lvl = vwap * (Decimal("1") - p / Decimal("100"))
        else:
            # SHORT entry: above VWAP using short distance.
            p = self.cfg.entry_distance_short_pct
            lvl = vwap * (Decimal("1") + p / Decimal("100"))
        return self._round_price(lvl)

    # ---------------------------
    # Position + orders
    # ---------------------------
    async def _get_open_position(self) -> Optional[dict]:
        assert self.subaccount_id is not None and self.product_id is not None
        try:
            positions = await self.client.list_positions(
                subaccount_id=str(self.subaccount_id),
                product_ids=[str(self.product_id)],
                open=True,
            )
        except Exception as e:
            if _is_connect_error(e):
                self._log_network_warning("list_positions", e)
                return None
            raise
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
        self.state["exit_levels"] = {"tp": None, "sl": None}
        self.state["exit_group_id"] = None
        self.state["exit_placed_ms"] = 0
        self.state["exit_qty"] = None

    def _clear_exit_state_local(self) -> None:
        """
        Clear exit-related state without calling cancel_orders().
        Used to recover from cases where position is confirmed closed but stale exit ids block new entries.
        """
        self.state["exit_order_ids"] = []
        self.state["exit_orders"] = {"tp": None, "sl": None}
        self.state["exit_levels"] = {"tp": None, "sl": None}
        self.state["exit_group_id"] = None
        self.state["exit_placed_ms"] = 0
        self.state["exit_qty"] = None

    async def _confirm_order_status(self, order_id: str) -> Optional[str]:
        """Fetch order by id and return status (upper)."""
        try:
            o = await self.client.get_order(id=UUID(order_id))
            st = getattr(o, "status", "")
            out = _norm_status(st)
            return out or None
        except Exception as e:
            if _is_connect_error(e):
                self._log_network_warning("get_order(status)", e)
            return None

    @staticmethod
    def _filled_qty_from_order(o: Any) -> Decimal:
        """
        Best-effort filled quantity extraction.
        Ethereal SDK tends to expose `filled`, but tolerate alternative attribute names.
        """
        for k in ("filled", "filledQty", "filled_qty", "filledQuantity", "filled_quantity", "executed", "executedQty"):
            try:
                v = getattr(o, k, None)
                if v is None:
                    continue
                d = _as_decimal(v)
                if _is_pos_finite_decimal(d):
                    return d
            except Exception:
                continue
        return Decimal("0")

    def _order_stop_price(self, o: Any) -> Optional[Decimal]:
        for k in ("stop_price", "stopPrice", "trigger_price", "triggerPrice", "stopPriceX18", "stop_price_x18"):
            try:
                v = getattr(o, k, None)
                if v is None:
                    continue
                d = _as_decimal(v)
                if _is_pos_finite_decimal(d) and d > 0:
                    return d
            except Exception:
                continue
        return None

    def _infer_exit_reason_from_order(self, *, oid: str, o: Any) -> Optional[str]:
        """
        Infer SL/TP from order fields when stop_type is missing/unreliable.
        Priority:
        - stop_type (normalized)
        - stop_price match against cached exit_levels
        - client_order_id hint (TP/SL)
        - state exit_orders id mapping
        """
        st_obj = getattr(o, "stop_type", None)
        stv_int = self._stop_type_int(st_obj)
        if stv_int == 1:
            return "SL"
        if stv_int == 0:
            return "TP"

        # stop_price comparison vs cached exit trigger levels
        sp = self._order_stop_price(o)
        if sp is not None and _is_pos_finite_decimal(sp):
            levels = self.state.get("exit_levels") or {}
            try:
                tp = _as_decimal(levels.get("tp")) if levels.get("tp") is not None else None
            except Exception:
                tp = None
            try:
                sl = _as_decimal(levels.get("sl")) if levels.get("sl") is not None else None
            except Exception:
                sl = None
            tol = self.product_tick_size if _is_pos_finite_decimal(self.product_tick_size) else Decimal("0")
            if tol <= 0:
                tol = Decimal("0.00000001")
            tol = tol * Decimal("2")
            if tp is not None and _is_pos_finite_decimal(tp) and (sp - tp).copy_abs() <= tol:
                return "TP"
            if sl is not None and _is_pos_finite_decimal(sl) and (sp - sl).copy_abs() <= tol:
                return "SL"

        # client_order_id marker (our ids include "TP"/"SL")
        try:
            cid = str(getattr(o, "client_order_id", "") or getattr(o, "clientOrderId", "") or "")
        except Exception:
            cid = ""
        cid_u = cid.upper()
        if "SL" in cid_u and "TP" not in cid_u:
            return "SL"
        if "TP" in cid_u and "SL" not in cid_u:
            return "TP"

        # fallback to state mapping by id
        eo = self.state.get("exit_orders") or {}
        if oid and oid == eo.get("sl"):
            return "SL"
        if oid and oid == eo.get("tp"):
            return "TP"
        return None

    @staticmethod
    def _order_debug_kv(o: Any) -> dict[str, str]:
        """
        Compact order debug fields (safe for logs).
        """
        out: dict[str, str] = {}
        for k in (
            "id",
            "status",
            "group_id",
            "groupId",
            "client_order_id",
            "clientOrderId",
            "reduce_only",
            "reduceOnly",
            "close",
            "isClose",
            "stop_type",
            "stopType",
            "stop_price",
            "stopPrice",
            "trigger_price",
            "triggerPrice",
            "filled",
            "filledQty",
            "filled_quantity",
            "filledQuantity",
            "filledAt",
            "filled_at",
            "updatedAt",
            "updated_at",
            "createdAt",
            "created_at",
            "price",
            "average_price",
            "avg_price",
            "avgFillPrice",
        ):
            try:
                v = getattr(o, k, None)
            except Exception:
                v = None
            if v is None:
                continue
            out[k] = str(v)
        return out

    @staticmethod
    def _is_filled_status(status: str) -> bool:
        """
        Status normalization for "filled" in Ethereal API responses.
        We treat several terminal statuses as filled because SDKs/exchanges sometimes differ.
        """
        s = (status or "").strip().upper()
        return s in {"FILLED", "CLOSED", "EXECUTED", "DONE", "SUCCESS"}

    @classmethod
    def _order_is_filled(cls, o: Any) -> bool:
        """
        Consider order filled if status says so, or if filled quantity is > 0.
        """
        try:
            st = (getattr(o, "status", "") or "").strip().upper()
        except Exception:
            st = ""
        if cls._is_filled_status(st):
            return True
        # Some APIs expose filledAt even if status isn't normalized.
        for k in ("filledAt", "filled_at"):
            try:
                v = getattr(o, k, None)
                if v is None:
                    continue
                if cls._to_ms_ts(v) > 0:
                    return True
            except Exception:
                continue
        try:
            fq = cls._filled_qty_from_order(o)
            return _is_pos_finite_decimal(fq) and fq > 0
        except Exception:
            return False

    async def _exit_orders_working(self) -> bool:
        """
        Best-effort check whether recorded exit orders still exist and are working/filled.
        Returns True if we should treat exits as present (to avoid duplicate placements).
        Returns False if exits are missing/stale and should be cleared/recreated.
        """
        ids = list(self.state.get("exit_order_ids") or [])
        if not ids:
            return False

        missing = 0
        terminal = 0
        for oid in ids:
            try:
                st = await self._confirm_order_status(str(oid))
            except Exception:
                st = None
            if st is None:
                # Unknown (network/propagation). Treat as present to avoid thrashing.
                return True
            if self._is_filled_status(st):
                return True
            if st.upper() in {"NEW", "PENDING", "FILLED_PARTIAL"}:
                return True
            if st.upper() in {"CANCELED", "EXPIRED", "REJECTED"}:
                terminal += 1
                continue
            # If we can't classify, treat as present.
            return True

        # If all are terminal/missing, treat as not working.
        return not (terminal >= len(ids) or missing >= len(ids))

    async def _ensure_exits_state_is_fresh(self) -> None:
        """
        If state thinks exits exist but API says they're gone, clear exits state so we can recreate.
        """
        if not self.state.get("exit_order_ids"):
            return
        try:
            working = await self._exit_orders_working()
        except Exception:
            working = True
        if not working:
            logger.warning("Exits appear stale/missing; clearing exit state to recreate TP/SL.")
            await self._cancel_exits()
            self._save_state()

    async def _hydrate_exit_levels_from_api(self) -> None:
        """
        Best-effort: if we have exit order ids but lost cached exit_levels, fetch stop prices
        from the API so we can apply "SL only tightens" logic safely.
        """
        levels = dict(self.state.get("exit_levels") or {"tp": None, "sl": None})
        if levels.get("tp") and levels.get("sl"):
            return
        eo = self.state.get("exit_orders") or {}
        ids = {"tp": eo.get("tp"), "sl": eo.get("sl")}
        now_ms = _dt_to_ms(_utc_now())
        placed_ms = int(self.state.get("exit_placed_ms") or 0)
        too_soon = placed_ms and (now_ms - placed_ms) < 10_000
        not_found = False
        changed = False
        for kind, oid in ids.items():
            if not oid or levels.get(kind):
                continue
            try:
                o = await self.client.get_order(id=UUID(str(oid)))
                sp = (
                    getattr(o, "stop_price", None)
                    or getattr(o, "stopPrice", None)
                    or getattr(o, "stop_price", None)
                )
                if sp is None:
                    continue
                px = _as_decimal(sp)
                if _is_pos_finite_decimal(px):
                    levels[kind] = str(px)
                    changed = True
            except Exception as e:
                if _is_connect_error(e):
                    self._log_network_warning("get_order(exit_levels)", e)
                    continue
                if _is_order_not_found_error(e):
                    not_found = True
                continue
        if changed:
            self.state["exit_levels"] = levels
            self._save_state()
        # If API reports exits are missing (404) and it's not immediately after placement,
        # clear exit ids so the bot can recreate TP/SL and avoid staying unprotected.
        if not_found and (not too_soon) and self.state.get("exit_order_ids"):
            logger.warning("Exit orders not found in API; clearing cached exit ids to recreate TP/SL.")
            await self._cancel_exits()
            self._save_state()

    @staticmethod
    def _is_working_status(status: Optional[str]) -> bool:
        if not status:
            return True
        return status.upper() in {"NEW", "PENDING", "FILLED_PARTIAL"}

    @staticmethod
    def _is_terminal_status(status: Optional[str]) -> bool:
        if not status:
            return False
        s = status.upper()
        return s in {"CANCELED", "EXPIRED", "REJECTED", "FILLED", "CLOSED", "EXECUTED", "DONE", "SUCCESS"}

    async def _adopt_existing_entry_if_any(self, side: int) -> bool:
        """If there is already a working entry LIMIT for this product+side, adopt it into state."""
        if not self.subaccount_id or not self.product_id:
            return False
        try:
            orders = await self.client.list_orders(
                subaccount_id=str(self.subaccount_id),
                product_ids=[str(self.product_id)],
                is_working=True,
                side=side,
                limit=200,
                order="desc",
                order_by="createdAt",
            )
        except Exception:
            return False

        candidates = []
        for o in orders or []:
            try:
                otype = getattr(o, "type", None)
                otype_val = str(getattr(otype, "value", otype) or "").upper()
                if otype_val != "LIMIT":
                    continue
                if bool(getattr(o, "reduce_only", False) or getattr(o, "reduceOnly", False)):
                    continue
                if bool(getattr(o, "close", False)):
                    continue
                stop_price = str(getattr(o, "stop_price", "") or getattr(o, "stopPrice", "") or "")
                if stop_price and stop_price not in {"0", "0.0", "0.00", "0.000", "0.0000", "0.000000000"}:
                    continue
                candidates.append(o)
            except Exception:
                continue

        if not candidates:
            return False

        # Adopt the newest
        newest = sorted(candidates, key=lambda x: getattr(x, "created_at", 0) or getattr(x, "createdAt", 0) or 0)[-1]
        oid = str(getattr(newest, "id", "") or "")
        cid = str(getattr(newest, "client_order_id", "") or getattr(newest, "clientOrderId", "") or "")
        if oid:
            self.state["entry_order_id"] = oid
            self.state["entry_client_order_id"] = cid or None
            self._save_state()
            if len(candidates) > 1:
                logger.warning("Found %d existing entry orders; adopting latest id=%s (no new orders will be placed).", len(candidates), oid)
            else:
                logger.info("Adopted existing entry order id=%s (no new order placed).", oid)
            return True
        return False

    @staticmethod
    def _result_value(x: Any) -> str:
        """Normalize SDK enum/string results (e.g. Result.ok -> 'Ok')."""
        try:
            v = getattr(x, "value", None)
            if v is not None:
                return str(v)
        except Exception:
            pass
        return str(x or "")

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

    async def _ensure_entry_order(
        self,
        entry_px: Decimal,
        *,
        ref_price: Optional[Decimal] = None,
        direction: Optional[str] = None,
    ) -> None:
        existing_oid = self.state.get("entry_order_id")
        if existing_oid:
            st = await self._confirm_order_status(str(existing_oid))
            # If we can't confirm yet, assume it's still propagating.
            if st in {None, "", "NEW", "PENDING", "FILLED_PARTIAL"}:
                return
            # If order is no longer working, clear and allow re-placement.
            if st in {"CANCELED", "EXPIRED", "REJECTED", "FILLED"}:
                self.state["entry_order_id"] = None
                self.state["entry_client_order_id"] = None
                self._save_state()
        qty = self._round_qty(self.cfg.entry_quantity)
        d = self._normalize_direction_value(direction or self.direction)
        if d not in {"LONG", "SHORT"}:
            return
        if not _is_pos_finite_decimal(qty) or not _is_pos_finite_decimal(entry_px):
            return

        side = 0 if d == "LONG" else 1

        # If state lost the order id, but UI/API still has a working entry order, adopt it to avoid duplicating.
        if not self.state.get("entry_order_id"):
            adopted = await self._adopt_existing_entry_if_any(side)
            if adopted:
                if self._is_auto_vwap_direction():
                    self.state["vwap_trade_direction"] = d
                    self._save_state()
                return
        async def place(px: Decimal) -> Optional[tuple[str, str, str]]:
            """Return (order_id, result, filled) or None."""
            cid_local = self._mk_client_order_id("E")
            try:
                sender = getattr(getattr(self.client, "chain", None), "address", None)
                expires_at = int(time.time()) + int(self.cfg.entry_expires_in_sec)
                o = await self.client.create_order(
                    order_type="LIMIT",
                    product_id=self.product_id,
                    ticker=self.cfg.ticker,
                    side=side,
                    quantity=float(qty),
                    price=float(px),
                    post_only=bool(self.cfg.post_only),
                    time_in_force="GTD",
                    expires_at=expires_at,
                    client_order_id=cid_local,
                    sender=sender,
                    subaccount=self.subaccount_name,
                )
                oid_local = str(getattr(o, "id", "") or "")
                result_local = self._result_value(getattr(o, "result", "") or "")
                filled_local = str(getattr(o, "filled", "") or "")
                # persist client id for this attempt only if accepted
                if oid_local:
                    self.state["entry_client_order_id"] = cid_local
                return oid_local, result_local, filled_local
            except Exception as e:
                logger.error("Failed to place ENTRY: %r", e)
                if "401" in str(e) or "Unauthorized" in str(e):
                    logger.error(
                        "Got 401 Unauthorized. Check ETHEREAL_TESTNET/ETHEREAL_BASE_URL match, and that this key "
                        "is allowed to trade on this subaccount. Linked signers are only needed if trading via a separate signer."
                    )
                return None

        # Post-only repricing retry on ImmediateMatchPostOnly
        attempt = 0
        px = self._round_price(entry_px)
        while True:
            attempt += 1
            placed = await place(px)
            if not placed:
                return
            oid, result, filled = placed

            # Important: API can return an id even when result != Ok (e.g. InsufficientBalance, PostOnly reject, etc.)
            if result and result.strip().lower() != "ok":
                logger.error(
                    "ENTRY rejected by API: result=%s filled=%s (qty=%s px=%s)",
                    result,
                    filled,
                    qty,
                    px,
                )
                if (
                    bool(self.cfg.post_only)
                    and bool(getattr(self.cfg, "post_only_reprice_on_reject", True))
                    and str(result).strip().lower() == "immediatematchpostonly"
                    and attempt < max(1, int(getattr(self.cfg, "post_only_reprice_max_attempts", 3) or 3))
                ):
                    # Reprice away from market using reference price (oracle/mark/index) if available.
                    rp = ref_price
                    if rp is None or not _is_pos_finite_decimal(rp):
                        try:
                            rp = _as_decimal(self.state.get("last_price") or "0")
                        except Exception:
                            rp = None
                    ticks = max(1, int(getattr(self.cfg, "post_only_reprice_ticks", 2) or 2))
                    off = (self.product_tick_size * Decimal(str(ticks))) if _is_pos_finite_decimal(self.product_tick_size) else Decimal("0")
                    if rp is not None and _is_pos_finite_decimal(rp) and off > 0:
                        if d == "LONG":
                            px2 = min(px, rp - off)
                        else:
                            px2 = max(px, rp + off)
                        px2 = self._round_price(px2)
                        if _is_pos_finite_decimal(px2) and px2 != px:
                            logger.warning(
                                "Post-only repricing after ImmediateMatchPostOnly: %s %s attempt=%s px=%s -> %s (ref=%s ticks=%s)",
                                d,
                                self.cfg.ticker,
                                attempt,
                                str(px),
                                str(px2),
                                str(rp),
                                ticks,
                            )
                            px = px2
                            continue
                return
            if not oid:
                logger.error("ENTRY submit returned empty order id (result=%s)", result or "UNKNOWN")
                return

            status = await self._confirm_order_status(oid)
            if status in {"REJECTED", "CANCELED", "EXPIRED"}:
                logger.error(
                    "ENTRY not working after submit: status=%s (order_id=%s result=%s filled=%s)",
                    status,
                    oid,
                    result or "UNKNOWN",
                    filled or "0",
                )
                return

            self.state["entry_order_id"] = oid
            # Pending entry (used for stats when a position becomes visible).
            if self._is_auto_vwap_direction():
                self.state["vwap_trade_direction"] = d
            self._save_state()
            logger.info(
                "ENTRY placed: %s qty=%s px=%s (order_id=%s status=%s result=%s filled=%s)",
                d,
                qty,
                px,
                oid,
                status or "UNKNOWN",
                result or "UNKNOWN",
                filled or "0",
            )
            return

    async def _ensure_oco_exits(self, pos: dict, tp_px: Decimal, sl_px: Decimal, *, direction: Optional[str] = None) -> None:
        if self.state.get("exit_order_ids"):
            return

        size = _as_decimal(pos.get("size") or "0").copy_abs()
        qty = self._round_qty(size)
        if not _is_pos_finite_decimal(qty):
            return

        d = self._normalize_direction_value(direction or self.direction)
        if d not in {"LONG", "SHORT"}:
            return

        # Don't place exits if TP/SL are invalid.
        if not _is_pos_finite_decimal(tp_px) or not _is_pos_finite_decimal(sl_px):
            return

        # Enforce tickSize rounding for stop prices (and store rounded values).
        tp_px = self._round_price(tp_px)
        sl_px = self._round_price(sl_px)

        # Close direction
        exit_side = 1 if d == "LONG" else 0
        group_id = str(uuid.uuid4())

        # stop_type: 0=GAIN (TP), 1=LOSS (SL)
        try:
            sender = getattr(getattr(self.client, "chain", None), "address", None)
            order_type = "MARKET" if self.cfg.exits_as_stop_market else "LIMIT"
            expires_at = int(time.time()) + int(self.cfg.exit_expires_in_sec)
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
                expires_at=expires_at,
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
                expires_at=expires_at,
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
            self.state["exit_levels"] = {"tp": str(tp_px), "sl": str(sl_px)}
            self.state["exit_group_id"] = group_id
            self.state["exit_placed_ms"] = _dt_to_ms(_utc_now())
            self.state["exit_qty"] = str(qty)
            self._save_state()
            logger.info("EXITS placed (OCO): TP=%s SL=%s qty=%s group=%s", tp_px, sl_px, qty, group_id)
        except Exception as e:
            logger.error("Failed to place exits: %s", e)

    async def _place_oco_exits_for_qty(self, *, qty: Decimal, tp_px: Decimal, sl_px: Decimal, direction: str) -> None:
        """
        Place OCO exits using an explicit quantity (used for anchor_open immediate protection).
        This avoids waiting for position visibility, so a new anchor_open entry isn't left without TP/SL.
        """
        # If state contains stale exit ids, clear them first.
        await self._ensure_exits_state_is_fresh()
        if self.state.get("exit_order_ids"):
            return
        q = self._round_qty(qty)
        if not _is_pos_finite_decimal(q):
            return
        d = (direction or "LONG").strip().upper()
        if d not in {"LONG", "SHORT"}:
            d = "LONG"
        if not _is_pos_finite_decimal(tp_px) or not _is_pos_finite_decimal(sl_px):
            return
        tp_px = self._round_price(tp_px)
        sl_px = self._round_price(sl_px)
        exit_side = 1 if d == "LONG" else 0
        group_id = str(uuid.uuid4())
        try:
            sender = getattr(getattr(self.client, "chain", None), "address", None)
            order_type = "MARKET" if self.cfg.exits_as_stop_market else "LIMIT"
            expires_at = int(time.time()) + int(self.cfg.exit_expires_in_sec)
            tp = await self.client.create_order(
                order_type=order_type,
                product_id=self.product_id,
                ticker=self.cfg.ticker,
                side=exit_side,
                quantity=float(q),
                reduce_only=True,
                stop_type=0,
                stop_price=float(tp_px),
                price=(float(tp_px) if order_type == "LIMIT" else None),
                time_in_force="GTD",
                expires_at=expires_at,
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
                quantity=float(q),
                reduce_only=True,
                stop_type=1,
                stop_price=float(sl_px),
                price=(float(sl_px) if order_type == "LIMIT" else None),
                time_in_force="GTD",
                expires_at=expires_at,
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
            self.state["exit_levels"] = {"tp": str(tp_px), "sl": str(sl_px)}
            self.state["exit_group_id"] = group_id
            self.state["exit_placed_ms"] = _dt_to_ms(_utc_now())
            self.state["exit_qty"] = str(q)
            self._save_state()
            logger.info("EXITS placed (OCO): TP=%s SL=%s qty=%s group=%s", tp_px, sl_px, q, group_id)
        except Exception as e:
            logger.error("Failed to place exits: %s", e)

    async def _detect_close_reason(self, exit_ids: list[str]) -> Optional[str]:
        """Return 'TP'|'SL'|None based on which exit filled."""
        if not exit_ids:
            return None
        try:
            filled: list[tuple[str, Any]] = []
            for oid in exit_ids:
                try:
                    o = await self.client.get_order(id=UUID(str(oid)))
                except Exception as e:
                    # One leg of OCO can disappear/cancel quickly; don't fail the whole detection.
                    if _is_order_not_found_error(e):
                        continue
                    raise
                if not self._order_is_filled(o):
                    continue
                filled.append((str(oid), o))

            if not filled:
                return None

            # Infer per filled order (stop_type/stop_price/client_id/state mapping).
            for oid, o in filled:
                r = self._infer_exit_reason_from_order(oid=oid, o=o)
                if r in {"SL", "TP"}:
                    return r
            # If we still can't infer, but we saw a fill, default to TP (less risky than pausing by mistake).
            return "TP"
        except Exception:
            return None

    async def _detect_close_reason_from_recent_orders(self) -> Optional[str]:
        """
        Fallback close-reason detector.

        Sometimes state can lose exit ids (e.g. restart, manual cancels, etc.). To still detect SL,
        we look at the most recent FILLED reduce-only stop orders for this product and (if present)
        match against our last known OCO group id.
        """
        if not self.subaccount_id or not self.product_id:
            return None
        orders = None
        try:
            orders = await self.client.list_orders(
                subaccount_id=str(self.subaccount_id),
                product_ids=[str(self.product_id)],
                limit=200,
                order="desc",
                order_by="updatedAt",
            )
        except Exception as e:
            if _is_connect_error(e):
                self._log_network_warning("list_orders(recent_reduce_stop)", e)
                return None
        if orders is None:
            try:
                orders = await self.client.list_orders(
                    subaccount_id=str(self.subaccount_id),
                    product_ids=[str(self.product_id)],
                    limit=200,
                    order="desc",
                    order_by="createdAt",
                )
            except Exception:
                return None
        if not orders:
            return None

        want_group = str(self.state.get("exit_group_id") or "")
        for o in orders:
            try:
                if not self._order_is_filled(o):
                    continue
                # match group if we have it (preferred)
                og = str(getattr(o, "group_id", "") or getattr(o, "groupId", "") or "")
                if want_group and og and og != want_group:
                    continue
                reduce_only = bool(
                    getattr(o, "reduce_only", False)
                    or getattr(o, "reduceOnly", False)
                    or getattr(o, "close", False)
                    or getattr(o, "isClose", False)
                )
                if not reduce_only:
                    continue
                oid = str(getattr(o, "id", "") or "")
                r = self._infer_exit_reason_from_order(oid=oid, o=o)
                if r in {"SL", "TP"}:
                    return r
            except Exception:
                continue
        return None

    @staticmethod
    def _to_ms_ts(v: Any) -> int:
        """
        Best-effort timestamp -> milliseconds since epoch.
        Supports: int/float (sec or ms), numeric strings, ISO-8601 strings, datetime.
        """
        try:
            if v is None:
                return 0
            if isinstance(v, datetime):
                return _dt_to_ms(v if v.tzinfo else v.replace(tzinfo=timezone.utc))
            if isinstance(v, (int, float)):
                iv = int(v)
                if iv <= 0:
                    return 0
                # heuristic: seconds vs ms
                return iv * 1000 if iv < 10_000_000_000 else iv
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return 0
                # numeric string?
                try:
                    iv = int(s)
                    return iv * 1000 if iv < 10_000_000_000 else iv
                except Exception:
                    pass
                try:
                    fv = float(s)
                    iv = int(fv)
                    return iv * 1000 if iv < 10_000_000_000 else iv
                except Exception:
                    pass
                # ISO-8601 string?
                try:
                    ss = s.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(ss)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return _dt_to_ms(dt.astimezone(timezone.utc))
                except Exception:
                    return 0
        except Exception:
            return 0
        return 0

    @staticmethod
    def _stop_type_int(st_obj: Any) -> Optional[int]:
        """
        Normalize stop_type to int where possible.
        Returns 0 (TP/GAIN), 1 (SL/LOSS) or None.
        """
        try:
            stv = getattr(st_obj, "value", st_obj)
            try:
                stv_int = int(stv)
                return stv_int if stv_int in {0, 1} else None
            except Exception:
                pass
            s = str(stv).strip().lower()
            if not s:
                return None
            # tolerate enum/string variants
            if "loss" in s or s == "sl" or s.endswith("_loss"):
                return 1
            if "gain" in s or s == "tp" or s.endswith("_gain"):
                return 0
            return None
        except Exception:
            return None

    @classmethod
    def _order_ts_ms(cls, o: Any) -> int:
        """
        Best-effort order timestamp (ms).
        Prefer filledAt/updatedAt/createdAt if present.
        """
        for k in ("filledAt", "filled_at", "updatedAt", "updated_at", "createdAt", "created_at"):
            try:
                v = getattr(o, k, None)
                if v is None:
                    continue
                iv = cls._to_ms_ts(v)
                if iv > 0:
                    return iv
            except Exception:
                continue
        return 0

    async def _detect_recent_reduce_only_stop_fill(self) -> Optional[tuple[str, str, int]]:
        """
        Scan recent orders and return (reason, order_id, ts_ms) for the latest *new* FILLED reduce-only stop order.
        reason: 'SL' if stop_type==1, 'TP' if stop_type==0
        """
        if not self.subaccount_id or not self.product_id:
            return None
        last_ts = int(self.state.get("last_handled_reduce_stop_ts") or 0)
        last_id = str(self.state.get("last_handled_reduce_stop_order_id") or "")
        want_group = str(self.state.get("exit_group_id") or "")
        placed_ms = int(self.state.get("exit_placed_ms") or 0)

        orders = None
        try:
            orders = await self.client.list_orders(
                subaccount_id=str(self.subaccount_id),
                product_ids=[str(self.product_id)],
                limit=200,
                order="desc",
                order_by="updatedAt",
            )
        except Exception:
            orders = None
        if orders is None:
            try:
                orders = await self.client.list_orders(
                    subaccount_id=str(self.subaccount_id),
                    product_ids=[str(self.product_id)],
                    limit=200,
                    order="desc",
                    order_by="createdAt",
                )
            except Exception:
                return None
        if not orders:
            return None

        best: Optional[tuple[str, str, int]] = None
        for o in orders:
            try:
                if not self._order_is_filled(o):
                    continue
                reduce_only = bool(
                    getattr(o, "reduce_only", False)
                    or getattr(o, "reduceOnly", False)
                    or getattr(o, "close", False)
                    or getattr(o, "isClose", False)
                )
                if not reduce_only:
                    continue
                # If we have an OCO group id, prefer matching it to avoid false positives.
                og = str(getattr(o, "group_id", "") or getattr(o, "groupId", "") or "")
                if want_group and og and og != want_group:
                    continue
                st = getattr(o, "stop_type", None)
                stv_int = self._stop_type_int(st)
                if stv_int not in {0, 1}:
                    continue

                oid = str(getattr(o, "id", "") or "")
                ts = self._order_ts_ms(o)
                if not oid or ts <= 0:
                    continue
                # If we know when exits were placed, ignore fills that predate it (avoid old historical order match).
                if placed_ms and ts < (placed_ms - 60_000):
                    continue
                # Dedup: ignore already handled
                if oid == last_id:
                    continue
                if ts <= last_ts:
                    continue

                reason = "SL" if stv_int == 1 else "TP"
                cand = (reason, oid, ts)
                if best is None or ts > best[2]:
                    best = cand
            except Exception:
                continue

        return best

    async def _check_and_handle_exit_fills(
        self,
        vwap: Decimal,
        entry_px: Decimal,
        tp_px: Decimal,
        sl_px: Decimal,
        *,
        trade_direction: Optional[str] = None,
    ) -> bool:
        """
        Check current OCO exit orders recorded in state and handle a fill immediately.

        This is more reliable than position-based detection because a position can open+close
        between polling intervals, while the FILLED order remains queryable.

        Returns True if we handled a fill (and caller should stop further actions this step).
        """
        exit_ids = list(self.state.get("exit_order_ids") or [])
        if not exit_ids:
            return False

        reason = await self._detect_close_reason(exit_ids)
        if not reason:
            # Fallback: handle cases where OCO legs become 404 or state mapping drifted.
            try:
                reason = await self._detect_close_reason_from_recent_orders()
            except Exception:
                reason = None
        if not reason:
            # Last resort: recent reduce-only stop scan (filtered by group/placed_ms inside).
            try:
                recent = await self._detect_recent_reduce_only_stop_fill()
                if recent:
                    reason = str(recent[0] or "")
            except Exception:
                reason = None
        if not reason:
            # If both exits are no longer working (stale ids), clear them to avoid blocking entries forever.
            try:
                statuses = [await self._confirm_order_status(str(oid)) for oid in exit_ids]
                if statuses and all(self._is_terminal_status(s) and (s or "").upper() != "FILLED" for s in statuses):
                    await self._cancel_exits()
                    self._save_state()
            except Exception:
                pass
            return False

        # Exit filled: cancel leftovers and record close reason.
        await self._cancel_exits()
        await self._cancel_entry()
        self.state["last_close_reason"] = reason
        self.state["last_close_at"] = _utc_now().isoformat()

        # Capture context for pause logs before we potentially clear anchor_open state.
        pause_td = self._pause_trade_direction(trade_direction)
        pause_strategy = self._cfg_strategy()

        # If anchor_open strategy trade was active, mark it closed.
        if self.state.get("anchor_open_active"):
            self._clear_anchor_open_state(reason=reason)
        self._clear_vwap_trade_direction()

        # Update stats (best-effort) using exit trigger prices.
        if reason == "SL":
            pause_min = self._pause_after_sl_minutes()
            if pause_min > 0:
                if self._should_ignore_sl_pause():
                    logger.info(
                        "SL pause ignored until first open: ticker=%s strategy=%s dir=%s source=exit_fill",
                        self.cfg.ticker,
                        pause_strategy,
                        pause_td,
                    )
                else:
                    await self._cancel_entry()
                    self._set_pause_after_sl(
                        pause_min,
                        vwap=vwap,
                        entry_px=entry_px,
                        tp_px=tp_px,
                        sl_px=sl_px,
                        source="exit_fill",
                        trade_direction=pause_td,
                        strategy=pause_strategy,
                    )
                    return True

        self._save_state()
        logger.info(
            "Position closed by %s (from exit order fill). vwap=%s entry=%s tp=%s sl=%s",
            reason,
            str(vwap),
            str(entry_px),
            str(tp_px),
            str(sl_px),
        )
        return False

    async def _handle_no_position_close_and_pause(
        self,
        *,
        prev_pos_size: Decimal,
        pos_size: Decimal,
        vwap: Decimal,
        entry_px: Decimal,
        tp_px: Decimal,
        sl_px: Decimal,
        trade_direction: Optional[str] = None,
    ) -> bool:
        """
        Common (all strategies) close detection when position is not visible.
        Goal: reliably detect SL/TP, set pause on SL, and clear stale exit state without risking unprotecting a live position.
        Returns True if caller should stop further actions this step.
        """
        # If we have neither a close transition nor any exit context, do nothing.
        exit_ids = list(self.state.get("exit_order_ids") or [])
        has_exit_ctx = bool(exit_ids or (self.state.get("exit_group_id") or "") or int(self.state.get("exit_placed_ms") or 0))
        closed_transition = _is_pos_finite_decimal(prev_pos_size) and prev_pos_size > 0 and (not _is_pos_finite_decimal(pos_size) or pos_size <= 0)

        recent_reason: Optional[str] = None
        recent_oid: Optional[str] = None
        recent_ts: Optional[int] = None
        # Avoid false positives on startup when we have no exit context and no close transition.
        if has_exit_ctx or closed_transition:
            try:
                recent = await self._detect_recent_reduce_only_stop_fill()
                if recent:
                    recent_reason, recent_oid, recent_ts = str(recent[0]), str(recent[1]), int(recent[2])
            except Exception:
                recent_reason = None

        # If we have a recent reduce-only stop fill (for our group/placed window), treat as a close event.
        if recent_reason in {"SL", "TP"}:
            # Capture context before clearing anchor state
            pause_td = self._pause_trade_direction(trade_direction)
            pause_strategy = self._cfg_strategy()
            if recent_oid and recent_ts:
                self.state["last_handled_reduce_stop_order_id"] = recent_oid
                self.state["last_handled_reduce_stop_ts"] = int(recent_ts)
                self.state["last_close_reason"] = recent_reason
                self.state["last_close_at"] = _ms_to_dt(int(recent_ts)).isoformat()
            # Clear exits/entry and pause if needed.
            await self._cancel_exits()
            await self._cancel_entry()
            if self.state.get("anchor_open_active"):
                self._clear_anchor_open_state(reason=recent_reason)
            self._clear_vwap_trade_direction()
            self._save_state()
            if recent_reason == "SL":
                pause_min = self._pause_after_sl_minutes()
                if pause_min > 0:
                    if self._should_ignore_sl_pause():
                        logger.info(
                            "SL pause ignored until first open: ticker=%s strategy=%s dir=%s source=recent_stop_order",
                            self.cfg.ticker,
                            pause_strategy,
                            pause_td,
                        )
                    else:
                        self._set_pause_after_sl(
                            pause_min,
                            vwap=vwap,
                            entry_px=entry_px,
                            tp_px=tp_px,
                            sl_px=sl_px,
                            source=f"recent_stop_order:{recent_oid or ''}".strip(":"),
                            trade_direction=pause_td,
                            strategy=pause_strategy,
                        )
                        return True
            return False

        # If position likely closed (transition) and we have exit context, try to infer close reason.
        if closed_transition and has_exit_ctx:
            reason = None
            pause_td = self._pause_trade_direction(trade_direction)
            pause_strategy = self._cfg_strategy()
            try:
                reason = await self._detect_close_reason(exit_ids) if exit_ids else None
            except Exception:
                reason = None
            if reason is None:
                try:
                    reason = await self._detect_close_reason_from_recent_orders()
                except Exception:
                    reason = None
            if reason:
                await self._cancel_exits()
                await self._cancel_entry()
                self.state["last_close_reason"] = reason
                self.state["last_close_at"] = _utc_now().isoformat()
                if self.state.get("anchor_open_active"):
                    self._clear_anchor_open_state(reason=reason)
                self._clear_vwap_trade_direction()
                self._save_state()
                if reason == "SL":
                    pause_min = self._pause_after_sl_minutes()
                    if pause_min > 0:
                        if self._should_ignore_sl_pause():
                            logger.info(
                                "SL pause ignored until first open: ticker=%s strategy=%s dir=%s source=position_close",
                                self.cfg.ticker,
                                pause_strategy,
                                pause_td,
                            )
                        else:
                            self._set_pause_after_sl(
                                pause_min,
                                vwap=vwap,
                                entry_px=entry_px,
                                tp_px=tp_px,
                                sl_px=sl_px,
                                source="position_close",
                                trade_direction=pause_td,
                                strategy=pause_strategy,
                            )
                            return True
            else:
                # Diagnostic (throttled): we saw a close transition but couldn't infer SL/TP.
                now_ms = _dt_to_ms(_utc_now())
                last_ms = int(self.state.get("pause_diag_last_log_ms") or 0)
                if not last_ms or (now_ms - last_ms) >= 60_000:
                    self.state["pause_diag_last_log_ms"] = int(now_ms)
                    self._save_state()
                    # Best-effort: dump exit order details to understand what the API returns on stop fills.
                    try:
                        dump = []
                        for x in exit_ids:
                            try:
                                oo = await self.client.get_order(id=UUID(str(x)))
                                dump.append({"id": str(x), "ok": "1", **self._order_debug_kv(oo)})
                            except Exception as ee:
                                dump.append({"id": str(x), "ok": "0", "err": str(ee)})
                        # Also include cached exit levels for matching.
                        levels = self.state.get("exit_levels") or {}
                        logger.warning(
                            "CLOSE reason unknown details: ticker=%s levels(tp=%s sl=%s) exit_orders=%s",
                            self.cfg.ticker,
                            str(levels.get("tp")),
                            str(levels.get("sl")),
                            str(self.state.get("exit_orders") or {}),
                        )
                        # Log per-id summaries (compact).
                        for row in dump[:6]:
                            logger.warning("CLOSE reason unknown exit_order: %s", str(row))
                    except Exception:
                        pass
                    logger.warning(
                        "CLOSE detected but reason unknown: ticker=%s strategy=%s dir=%s prev_pos=%s now_pos=%s exit_ids=%s group=%s placed_ms=%s",
                        self.cfg.ticker,
                        pause_strategy,
                        pause_td,
                        str(prev_pos_size),
                        str(pos_size),
                        ",".join(str(x) for x in exit_ids) if exit_ids else "",
                        str(self.state.get("exit_group_id") or ""),
                        str(int(self.state.get("exit_placed_ms") or 0)),
                    )
                # Recovery: if position stays absent for a few consecutive polls, clear exits state
                # so stale exit ids don't block new entries indefinitely.
                try:
                    streak = int(self.state.get("no_position_streak") or 0)
                except Exception:
                    streak = 0
                if streak >= 2 and exit_ids:
                    statuses: list[Optional[str]] = []
                    for oid in exit_ids:
                        try:
                            statuses.append(await self._confirm_order_status(str(oid)))
                        except Exception:
                            statuses.append(None)
                    # Only act if we could confirm at least one status (avoid acting on pure network issues).
                    if statuses and any(s is not None for s in statuses):
                        confirmed = [s for s in statuses if s is not None]
                        if confirmed and all(self._is_terminal_status(s) for s in confirmed):
                            logger.warning(
                                "Clearing stale exit state after close (reason unknown): ticker=%s streak=%s statuses=%s",
                                self.cfg.ticker,
                                streak,
                                ",".join(str(s) for s in statuses),
                            )
                            self._clear_exit_state_local()
                            self._save_state()
        return False

    def _cfg_strategy(self) -> str:
        s = (getattr(self.cfg, "strategy", None) or "vwap").strip().lower()
        if s in {"anchor", "anchoropen", "anchor_open", "anchor-open"}:
            return "anchor_open"
        if s in {"vwap", "vwap_entry", "vwap-limit", "vwap_limit"}:
            return "vwap"
        return s or "vwap"

    def _is_auto_vwap_direction(self) -> bool:
        return self._cfg_strategy() == "vwap" and self.direction == "BOTH"

    def _auto_vwap_threshold_pct(self) -> Decimal:
        thr = _as_decimal(getattr(self.cfg, "vwap_both_min_delta_pct", Decimal("0.1")) or "0")
        if thr < 0:
            return Decimal("0")
        return thr

    def _auto_vwap_max_threshold_pct(self) -> Decimal:
        thr = _as_decimal(getattr(self.cfg, "vwap_both_max_delta_pct", Decimal("0")) or "0")
        if thr <= 0:
            return Decimal("0")
        return thr

    def _auto_vwap_start_direction(self) -> Optional[str]:
        raw = str(getattr(self.cfg, "vwap_both_start_direction", "NONE") or "NONE").strip().upper()
        if raw in {"", "NONE", "WAIT", "DISABLED", "OFF"}:
            return None
        d = self._normalize_direction_value(raw)
        return d if d in {"LONG", "SHORT"} else None

    def _auto_vwap_mode(self) -> str:
        raw = str(getattr(self.cfg, "vwap_both_mode", "trend") or "trend").strip().lower()
        if raw in {"counter", "countertrend", "contra", "contrarian", "reverse", "reversal", "opposite", "ct"}:
            return "countertrend"
        return "trend"

    @staticmethod
    def _color_direction_label(d: str) -> str:
        if d == "LONG":
            return "\x1b[32mLONG\x1b[0m"
        if d == "SHORT":
            return "\x1b[31mSHORT\x1b[0m"
        return str(d or "")

    def _log_vwap_direction_change(self, *, new_dir: str, prev_dir: Optional[str], reason: str) -> None:
        new_norm = self._normalize_direction_value(new_dir)
        if new_norm not in {"LONG", "SHORT"}:
            return
        prev_norm = self._normalize_direction_value(prev_dir)
        if prev_norm in {"LONG", "SHORT"} and prev_norm != new_norm:
            logger.info(
                "VWAP BOTH direction change: ticker=%s prev=%s new=%s reason=%s",
                self.cfg.ticker,
                prev_norm,
                self._color_direction_label(new_norm),
                reason,
            )
        else:
            logger.info(
                "VWAP BOTH direction: ticker=%s dir=%s reason=%s",
                self.cfg.ticker,
                self._color_direction_label(new_norm),
                reason,
            )

    def _pause_trade_direction(self, fallback: Optional[str] = None) -> str:
        d = (
            self.state.get("anchor_open_direction")
            or self.state.get("vwap_trade_direction")
            or fallback
            or self.direction
            or "LONG"
        )
        out = self._normalize_direction_value(d)
        return out if out in {"LONG", "SHORT"} else "LONG"

    def _should_ignore_sl_pause(self) -> bool:
        if not bool(getattr(self.cfg, "pause_ignore_sl_until_first_open", False)):
            return False
        return not bool(self.state.get("opened_since_startup"))

    def _infer_direction_from_position(self, pos: Optional[dict]) -> Optional[str]:
        if not pos:
            return None
        for key in ("side", "position_side", "positionSide", "direction"):
            d = self._normalize_position_side(pos.get(key))
            if d in {"LONG", "SHORT"}:
                return d
        # Fallback: if size sign is available, use it.
        try:
            size_raw = _as_decimal(pos.get("size") or "0")
            if size_raw < 0:
                return "SHORT"
            if size_raw > 0:
                return "LONG"
        except Exception:
            return None
        return None

    def _maybe_capture_vwap_anchor_open_price(self, *, anchor_start_ms: int, now_ms: int, price: Decimal) -> None:
        if not self._is_auto_vwap_direction():
            return
        if int(self.state.get("vwap_anchor_decision_anchor_ms") or 0) == int(anchor_start_ms):
            return
        if self.state.get("vwap_anchor_open_price") is not None:
            return
        prev_vwap = self._decimal_from_state("prev_anchor_vwap")
        if prev_vwap is None or not _is_pos_finite_decimal(prev_vwap):
            last_ms = int(self.state.get("vwap_anchor_last_wait_log_ms") or 0)
            if not last_ms or (now_ms - last_ms) >= 60_000:
                self.state["vwap_anchor_last_wait_log_ms"] = int(now_ms)
                self._save_state()
                logger.info(
                    "VWAP BOTH: waiting for prev_anchor_vwap for %s (need 1 completed anchor).",
                    self.cfg.ticker,
                )
            return
        if not _is_pos_finite_decimal(price):
            last_ms = int(self.state.get("vwap_anchor_last_wait_log_ms") or 0)
            if not last_ms or (now_ms - last_ms) >= 60_000:
                self.state["vwap_anchor_last_wait_log_ms"] = int(now_ms)
                self._save_state()
                logger.info("VWAP BOTH: waiting for open price at new anchor (%s).", self.cfg.ticker)
            return

        self.state["vwap_anchor_open_price"] = str(price)
        self.state["vwap_anchor_open_price_ms"] = int(now_ms)
        self._save_state()
        logger.info(
            "VWAP BOTH: captured anchor open price for %s open=%s prev_anchor_vwap=%s",
            self.cfg.ticker,
            str(price),
            str(prev_vwap),
        )

    def _resolve_vwap_auto_direction(
        self,
        *,
        anchor_start_ms: int,
        now_ms: int,
        price: Decimal,
        has_pos: bool,
        pos: Optional[dict],
    ) -> Optional[str]:
        if not self._is_auto_vwap_direction():
            d = self._normalize_direction_value(self.direction)
            return d if d in {"LONG", "SHORT"} else None

        # Keep direction for active position/order when known.
        active_dir = self._normalize_direction_value(self.state.get("vwap_trade_direction"))
        if active_dir in {"LONG", "SHORT"}:
            if has_pos or self.state.get("entry_order_id") or self.state.get("exit_order_ids"):
                return active_dir

        # Try to infer direction from an open position if we lost state.
        if has_pos and (active_dir not in {"LONG", "SHORT"}):
            inferred = self._infer_direction_from_position(pos)
            if inferred in {"LONG", "SHORT"}:
                self.state["vwap_trade_direction"] = inferred
                self._save_state()
                logger.warning(
                    "VWAP BOTH: inferred trade direction from position: %s %s",
                    inferred,
                    self.cfg.ticker,
                )
                return inferred
            last_ms = int(self.state.get("vwap_direction_unknown_log_ms") or 0)
            if not last_ms or (now_ms - last_ms) >= 60_000:
                self.state["vwap_direction_unknown_log_ms"] = int(now_ms)
                self._save_state()
                logger.warning(
                    "VWAP BOTH: active position but direction unknown (ticker=%s). Skipping exit refresh.",
                    self.cfg.ticker,
                )
            return None

        # Ensure open price is captured for this anchor.
        self._maybe_capture_vwap_anchor_open_price(anchor_start_ms=anchor_start_ms, now_ms=now_ms, price=price)

        # If decision already made for this anchor, reuse it.
        decision_anchor = int(self.state.get("vwap_anchor_decision_anchor_ms") or 0)
        if decision_anchor == int(anchor_start_ms):
            d = self._normalize_direction_value(self.state.get("vwap_anchor_direction"))
            return d if d in {"LONG", "SHORT"} else None

        prev_vwap = self._decimal_from_state("prev_anchor_vwap")
        if prev_vwap is None or not _is_pos_finite_decimal(prev_vwap):
            start_dir = self._auto_vwap_start_direction()
            if start_dir in {"LONG", "SHORT"}:
                used_anchor = int(self.state.get("vwap_start_direction_used_anchor_ms") or 0)
                if used_anchor != int(anchor_start_ms):
                    prev_dir = self.state.get("vwap_last_auto_direction")
                    self.state["vwap_last_auto_direction"] = start_dir
                    self.state["vwap_start_direction_used_anchor_ms"] = int(anchor_start_ms)
                    self._save_state()
                    self._log_vwap_direction_change(new_dir=start_dir, prev_dir=prev_dir, reason="startup")
                return start_dir
            return None

        open_px = self._price_from_state("vwap_anchor_open_price")
        if open_px is None or not _is_pos_finite_decimal(open_px):
            return None

        delta = open_px - prev_vwap
        if delta == 0:
            self.state["vwap_anchor_decision_anchor_ms"] = int(anchor_start_ms)
            self.state["vwap_anchor_direction"] = None
            self.state["vwap_anchor_delta_pct"] = "0"
            self._save_state()
            logger.info("VWAP BOTH skip: delta=0 (prev_vwap=%s open=%s)", str(prev_vwap), str(open_px))
            return None

        delta_pct = (delta.copy_abs() / prev_vwap) * Decimal("100")
        thr = self._auto_vwap_threshold_pct()
        if delta_pct < thr:
            self.state["vwap_anchor_decision_anchor_ms"] = int(anchor_start_ms)
            self.state["vwap_anchor_direction"] = None
            self.state["vwap_anchor_delta_pct"] = str(delta_pct)
            self._save_state()
            logger.info(
                "VWAP BOTH skip: delta_pct=%s < thr=%s (prev_vwap=%s open=%s)",
                str(delta_pct),
                str(thr),
                str(prev_vwap),
                str(open_px),
            )
            return None

        max_thr = self._auto_vwap_max_threshold_pct()
        if max_thr > 0 and delta_pct > max_thr:
            self.state["vwap_anchor_decision_anchor_ms"] = int(anchor_start_ms)
            self.state["vwap_anchor_direction"] = None
            self.state["vwap_anchor_delta_pct"] = str(delta_pct)
            self._save_state()
            logger.info(
                "VWAP BOTH skip: delta_pct=%s > max=%s (prev_vwap=%s open=%s)",
                str(delta_pct),
                str(max_thr),
                str(prev_vwap),
                str(open_px),
            )
            return None

        trend_dir = "LONG" if open_px > prev_vwap else "SHORT"
        mode = self._auto_vwap_mode()
        trade_dir = trend_dir if mode == "trend" else ("SHORT" if trend_dir == "LONG" else "LONG")
        prev_dir = self.state.get("vwap_last_auto_direction")
        self.state["vwap_anchor_decision_anchor_ms"] = int(anchor_start_ms)
        self.state["vwap_anchor_direction"] = trade_dir
        self.state["vwap_anchor_delta_pct"] = str(delta_pct)
        self.state["vwap_last_auto_direction"] = trade_dir
        self._save_state()
        reason = "delta" if mode == "trend" else "delta_countertrend"
        self._log_vwap_direction_change(new_dir=trade_dir, prev_dir=prev_dir, reason=reason)
        return trade_dir

    def _clear_vwap_trade_direction(self) -> None:
        if not self._is_auto_vwap_direction():
            return
        if self.state.get("vwap_trade_direction") is None:
            return
        self.state["vwap_trade_direction"] = None
        self._save_state()


    def _clear_anchor_open_state(self, *, reason: str) -> None:
        self.state["anchor_open_active"] = False
        self.state["anchor_open_closed_at"] = _utc_now().isoformat()
        self.state["anchor_open_close_reason"] = str(reason or "UNKNOWN")
        self.state["anchor_open_direction"] = None
        self.state["anchor_open_tp_price"] = None
        self.state["anchor_open_sl_price"] = None
        self.state["anchor_open_tp_size"] = None
        self.state["anchor_open_delta_pct"] = None
        self.state["anchor_open_opened_at"] = None
        # Keep prev_anchor_vwap for next anchor.
        self._save_state()

    def _price_from_state(self, key: str) -> Optional[Decimal]:
        raw = self.state.get(key)
        if raw is None:
            return None
        try:
            d = _as_decimal(raw)
        except Exception:
            return None
        if not (d == d) or d <= 0:
            return None
        return d

    def _decimal_from_state(self, key: str) -> Optional[Decimal]:
        raw = self.state.get(key)
        if raw is None:
            return None
        try:
            d = _as_decimal(raw)
        except Exception:
            return None
        if not (d == d) or d <= 0:
            return None
        return d

    async def _place_market_order(self, *, side: int, qty: Decimal, reduce_only: bool, tag: str) -> Optional[str]:
        if not _is_pos_finite_decimal(qty):
            return None
        try:
            sender = getattr(getattr(self.client, "chain", None), "address", None)
            o = await self.client.create_order(
                order_type="MARKET",
                product_id=self.product_id,
                ticker=self.cfg.ticker,
                side=int(side),
                quantity=float(qty),
                reduce_only=bool(reduce_only),
                sender=sender,
                subaccount=self.subaccount_name,
                client_order_id=self._mk_client_order_id(tag),
            )
            oid = str(getattr(o, "id", "") or "")
            result = self._result_value(getattr(o, "result", "") or "")
            if result and result.strip().lower() != "ok":
                logger.error("%s MARKET rejected: result=%s qty=%s", tag, result, qty)
                return None
            if not oid:
                logger.error("%s MARKET returned empty order id (result=%s)", tag, result or "UNKNOWN")
                return None
            return oid
        except Exception as e:
            logger.error("%s MARKET failed: %r", tag, e)
            return None

    async def _close_position_market(self, *, direction: str, pos: dict, reason: str) -> None:
        """
        Close full position by market with reduce_only=True.
        direction is the trade direction (LONG|SHORT) we are closing.
        """
        size = _as_decimal(pos.get("size") or "0").copy_abs()
        qty = self._round_qty(size)
        if not _is_pos_finite_decimal(qty):
            return
        d = (direction or "").strip().upper()
        exit_side = 1 if d == "LONG" else 0
        oid = await self._place_market_order(side=exit_side, qty=qty, reduce_only=True, tag="X")
        if oid:
            logger.warning("CLOSE by market: %s %s qty=%s reason=%s (order_id=%s)", d, self.cfg.ticker, qty, reason, oid)

    async def _anchor_open_step(
        self,
        *,
        now: datetime,
        now_ms: int,
        anchor_start_ms: int,
        prev_anchor_ms: int,
        price: Decimal,
        vwap: Decimal,
        pos: Optional[dict],
        has_pos: bool,
        is_new_candle: bool,
    ) -> None:
        """
        Strategy "anchor_open":
        - On new anchor: (optionally) close existing position at anchor end.
        - Once first VWAP of new anchor is available: decide to open based on VWAP delta threshold.
        - Entry is MARKET.
        - TP = last VWAP of previous anchor.
        - SL distance = TP distance * anchor_open_sl_pct_of_tp.
        - If anchor ends and position still open: close by MARKET.
        """
        # 1) If anchor changed and we have an active anchor_open trade, close it at anchor end.
        if prev_anchor_ms and prev_anchor_ms != anchor_start_ms and bool(self.state.get("anchor_open_active")):
            if bool(getattr(self.cfg, "anchor_open_close_on_anchor_end", True)) and has_pos and pos:
                d = str(self.state.get("anchor_open_direction") or "").upper() or "LONG"
                await self._cancel_exits()
                await self._close_position_market(direction=d, pos=pos, reason="ANCHOR_END")
            self._clear_anchor_open_state(reason="ANCHOR_END")

        # 2) If we have a position, ensure exits exist using stored TP/SL.
        if has_pos and pos:
            # If exits are recorded but disappeared, clear state so we can recreate.
            await self._ensure_exits_state_is_fresh()
            if not self.state.get("exit_order_ids"):
                try:
                    tp = _as_decimal(self.state.get("anchor_open_tp_price"))
                    sl = _as_decimal(self.state.get("anchor_open_sl_price"))
                    d = str(self.state.get("anchor_open_direction") or "").upper() or "LONG"
                except Exception:
                    tp = Decimal("NaN")
                    sl = Decimal("NaN")
                    d = "LONG"
                if (tp == tp) and tp > 0 and (sl == sl) and sl > 0:
                    await self._ensure_oco_exits(pos, self._round_price(tp), self._round_price(sl), direction=d)
            else:
                # If exits were placed with a provisional qty but position size differs, refresh exits to exact size.
                try:
                    want = _as_decimal(self.state.get("exit_qty") or "0")
                except Exception:
                    want = Decimal("0")
                have = _as_decimal(pos.get("size") or "0").copy_abs()
                have_q = self._round_qty(have)
                want_q = self._round_qty(want) if _is_pos_finite_decimal(want) else have_q
                if _is_pos_finite_decimal(have_q) and _is_pos_finite_decimal(want_q) and have_q != want_q:
                    try:
                        tp = _as_decimal(self.state.get("anchor_open_tp_price"))
                        sl = _as_decimal(self.state.get("anchor_open_sl_price"))
                    except Exception:
                        tp = Decimal("NaN")
                        sl = Decimal("NaN")
                    if _is_pos_finite_decimal(tp) and _is_pos_finite_decimal(sl):
                        logger.warning("ANCHOR_OPEN exits qty refresh: %s %s %s -> %s", d, self.cfg.ticker, str(want_q), str(have_q))
                        await self._cancel_exits()
                        await self._ensure_oco_exits(pos, self._round_price(tp), self._round_price(sl), direction=d)
                        return

                # Trailing stop for anchor_open: adjust SL on new candle based on previous candle body.
                if is_new_candle and bool(getattr(self.cfg, "trailing_stop_enabled", False)):
                    d = str(self.state.get("anchor_open_direction") or "").upper() or "LONG"
                    if d not in {"LONG", "SHORT"}:
                        d = "LONG"
                    try:
                        po = _as_decimal(self.state.get("prev_candle_open_price"))
                        pc = _as_decimal(self.state.get("prev_candle_close_price"))
                    except Exception:
                        po, pc = Decimal("NaN"), Decimal("NaN")
                    # Ensure we know current SL.
                    if not (self.state.get("exit_levels") or {}).get("sl"):
                        await self._hydrate_exit_levels_from_api()
                    try:
                        cur_sl_raw = ((self.state.get("exit_levels") or {}).get("sl"))
                        cur_sl = _as_decimal(cur_sl_raw) if cur_sl_raw is not None else None
                    except Exception:
                        cur_sl = None
                    if cur_sl is None or not _is_pos_finite_decimal(cur_sl):
                        logger.info("ANCHOR_OPEN trailing skipped: current SL unknown (ticker=%s)", self.cfg.ticker)
                        return

                    # Compute trailing candidate; it is only produced for "opposite" candles by definition.
                    trail_info = await self._trailing_decision_async(trade_direction=d, current_sl=cur_sl)
                    self._trailing_debug(
                        "TRAIL DEBUG: anchor_open trailing decision ticker=%s dir=%s eligible=%s reason=%s prev_o=%s prev_h=%s prev_l=%s prev_c=%s body=%s current_sl=%s candidate_raw=%s",
                        self.cfg.ticker,
                        d,
                        str(trail_info.get("eligible")),
                        str(trail_info.get("reason")),
                        str(trail_info.get("prev_open")),
                        str(trail_info.get("prev_high")),
                        str(trail_info.get("prev_low")),
                        str(trail_info.get("prev_close")),
                        str(trail_info.get("body")),
                        str(cur_sl),
                        str(trail_info.get("candidate_raw")),
                    )
                    trail = None
                    if bool(trail_info.get("eligible")):
                        try:
                            trail_raw = trail_info.get("candidate_raw")
                            trail = _as_decimal(trail_raw) if trail_raw is not None else None
                        except Exception:
                            trail = None
                    if trail is None or not _is_pos_finite_decimal(trail):
                        logger.info(
                            "ANCHOR_OPEN trailing no-op: prev_open=%s prev_close=%s direction=%s (ticker=%s)",
                            str(po),
                            str(pc),
                            d,
                            self.cfg.ticker,
                        )
                        return

                    # Round trailing candidate to tick size before applying.
                    trail_r = self._round_price(trail)
                    new_sl = self._tightened_sl(current_sl=cur_sl, desired_sl=trail_r, trade_direction=d)

                    # Use cached TP (prefer exit_levels, fallback anchor_open_tp_price).
                    tp = self._decimal_from_state("anchor_open_tp_price") or self._decimal_from_state("prev_anchor_vwap")
                    try:
                        cur_tp_raw = ((self.state.get("exit_levels") or {}).get("tp"))
                        cur_tp = _as_decimal(cur_tp_raw) if cur_tp_raw is not None else None
                    except Exception:
                        cur_tp = None
                    tp_use = cur_tp if (cur_tp is not None and _is_pos_finite_decimal(cur_tp)) else tp
                    if tp_use is None or not _is_pos_finite_decimal(tp_use):
                        logger.info("ANCHOR_OPEN trailing skipped: TP unknown (ticker=%s)", self.cfg.ticker)
                        return

                    if self._round_price(new_sl) == self._round_price(cur_sl):
                        return

                    logger.info(
                        "ANCHOR_OPEN trailing SL: prev_open=%s prev_close=%s %s %s current_sl=%s trail=%s -> new_sl=%s",
                        str(po),
                        str(pc),
                        d,
                        self.cfg.ticker,
                        str(cur_sl),
                        str(trail_r),
                        str(self._round_price(new_sl)),
                    )
                    await self._cancel_exits()
                    await self._ensure_oco_exits(pos, tp_use, new_sl, direction=d)
            return

        # 3) If no position and no active trade, decide once per anchor.
        decision_anchor = int(self.state.get("anchor_open_decision_anchor_ms") or 0)
        if decision_anchor == int(anchor_start_ms):
            return

        prev_vwap = self._decimal_from_state("prev_anchor_vwap")
        if prev_vwap is None:
            # Most common reason: bot started mid-hour and hasn't completed a full anchor yet,
            # so there is no "last VWAP of previous anchor" to target.
            last_ms = int(self.state.get("anchor_open_last_wait_log_ms") or 0)
            if not last_ms or (now_ms - last_ms) >= 60_000:
                self.state["anchor_open_last_wait_log_ms"] = int(now_ms)
                self._save_state()
                logger.info(
                    "ANCHOR_OPEN: no prev_anchor_vwap yet for %s (need at least 1 completed anchor with valid VWAP).",
                    self.cfg.ticker,
                )
            return

        # Capture the "open price" of this anchor (first oracle price we see after anchor start).
        if self.state.get("anchor_open_open_price") is None:
            if not _is_pos_finite_decimal(price):
                last_ms = int(self.state.get("anchor_open_last_wait_log_ms") or 0)
                if not last_ms or (now_ms - last_ms) >= 60_000:
                    self.state["anchor_open_last_wait_log_ms"] = int(now_ms)
                    self._save_state()
                    logger.info("ANCHOR_OPEN: waiting for oracle price at new anchor (%s). price=%s", self.cfg.ticker, str(price))
                return
            self.state["anchor_open_open_price"] = str(price)
            self.state["anchor_open_open_price_ms"] = int(now_ms)
            self._save_state()
            logger.info(
                "ANCHOR_OPEN: captured open price for %s open=%s prev_anchor_vwap=%s",
                self.cfg.ticker,
                str(price),
                str(prev_vwap),
            )

        open_px = self._price_from_state("anchor_open_open_price")
        if open_px is None:
            return

        delta = (open_px - prev_vwap)
        if delta == 0:
            self.state["anchor_open_decision_anchor_ms"] = int(anchor_start_ms)
            self._save_state()
            logger.info("ANCHOR_OPEN skip: delta=0 (prev_vwap=%s open=%s)", str(prev_vwap), str(open_px))
            return

        delta_pct = (delta.copy_abs() / prev_vwap) * Decimal("100")
        thr = _as_decimal(getattr(self.cfg, "anchor_open_min_delta_pct", Decimal("0")) or "0")
        if thr < 0:
            thr = Decimal("0")
        if delta_pct < thr:
            self.state["anchor_open_decision_anchor_ms"] = int(anchor_start_ms)
            self.state["anchor_open_delta_pct"] = str(delta_pct)
            self._save_state()
            logger.info(
                "ANCHOR_OPEN skip: delta_pct=%s < thr=%s (prev_vwap=%s open=%s)",
                str(delta_pct),
                str(thr),
                str(prev_vwap),
                str(open_px),
            )
            return

        # Direction: if open > prev VWAP => SHORT (to TP down to prev VWAP). Else LONG.
        trade_dir = "SHORT" if open_px > prev_vwap else "LONG"
        side = 0 if trade_dir == "LONG" else 1

        # TP and SL based on open price vs previous anchor VWAP.
        tp_price = prev_vwap
        tp_size = (open_px - prev_vwap).copy_abs()
        if tp_size <= 0:
            self.state["anchor_open_decision_anchor_ms"] = int(anchor_start_ms)
            self._save_state()
            return

        sl_pct_of_tp = _as_decimal(getattr(self.cfg, "anchor_open_sl_pct_of_tp", Decimal("100")) or "100")
        if sl_pct_of_tp < 0:
            sl_pct_of_tp = Decimal("0")
        sl_size = tp_size * (sl_pct_of_tp / Decimal("100"))
        # Stop is placed around the entry reference (open price).
        sl_price = (open_px - sl_size) if trade_dir == "LONG" else (open_px + sl_size)

        qty = self._round_qty(self.cfg.entry_quantity)
        if qty <= 0:
            self.state["anchor_open_decision_anchor_ms"] = int(anchor_start_ms)
            self._save_state()
            logger.error("ANCHOR_OPEN: qty<=0; check entry_quantity")
            return

        oid = await self._place_market_order(side=side, qty=qty, reduce_only=False, tag="E")
        if not oid:
            return

        self.state["anchor_open_active"] = True
        self.state["anchor_open_direction"] = trade_dir
        self.state["anchor_open_tp_price"] = str(tp_price)
        self.state["anchor_open_sl_price"] = str(sl_price)
        self.state["anchor_open_tp_size"] = str(tp_size)
        self.state["anchor_open_delta_pct"] = str(delta_pct)
        self.state["anchor_open_opened_at"] = now.isoformat()
        self.state["anchor_open_decision_anchor_ms"] = int(anchor_start_ms)
        self._save_state()

        # Immediate protection: place TP/SL using configured entry qty (do not wait for position visibility).
        # This prevents anchor_open entries from being unprotected when the position endpoint lags.
        await self._place_oco_exits_for_qty(
            qty=qty,
            tp_px=tp_price,
            sl_px=sl_price,
            direction=trade_dir,
        )

        logger.warning(
            "ANCHOR_OPEN ENTRY: %s %s qty=%s (order_id=%s) prev_vwap=%s open=%s delta_pct=%s thr=%s TP=%s SL=%s sl_pct_of_tp=%s tp_size=%s",
            trade_dir,
            self.cfg.ticker,
            qty,
            oid,
            str(prev_vwap),
            str(open_px),
            str(delta_pct),
            str(thr),
            str(self._round_price(tp_price)),
            str(self._round_price(sl_price)),
            str(sl_pct_of_tp),
            str(tp_size),
        )


    def _pause_after_sl_minutes(self) -> int:
        """
        Returns pause duration in minutes after SL.

        New model: cfg.pause_after_sl_minutes (preferred).
        Backward compatibility: if cfg.pause_on_sl is true and pause_after_sl_minutes<=0 -> default to 60 minutes.
        """
        try:
            m = int(getattr(self.cfg, "pause_after_sl_minutes", 0) or 0)
        except Exception:
            m = 0
        if m > 0:
            return m
        if bool(getattr(self.cfg, "pause_on_sl", False)):
            return 60
        return 0

    def _tightened_sl(
        self,
        *,
        current_sl: Optional[Decimal],
        desired_sl: Decimal,
        trade_direction: Optional[str] = None,
    ) -> Decimal:
        """
        VWAP strategy rule: after entry, SL can only tighten (reduce risk).
        - LONG: SL price can only move up (increase numerically).
        - SHORT: SL price can only move down (decrease numerically).
        """
        if current_sl is None or not _is_pos_finite_decimal(current_sl):
            return desired_sl
        if not _is_pos_finite_decimal(desired_sl):
            return current_sl
        d = self._normalize_direction_value(trade_direction or self.direction)
        if d not in {"LONG", "SHORT"}:
            return current_sl
        if d == "LONG":
            return desired_sl if desired_sl > current_sl else current_sl
        # SHORT
        return desired_sl if desired_sl < current_sl else current_sl

    def _trailing_sl_candidate(self, *, trade_direction: str, current_sl: Optional[Decimal]) -> Optional[Decimal]:
        """
        Compute trailing SL candidate from the previous candle body, according to the requested rule.
        Uses prev_candle_open_price/prev_candle_close_price tracked from reference price.
        """
        if current_sl is None or not _is_pos_finite_decimal(current_sl):
            return None
        if not bool(getattr(self.cfg, "trailing_stop_enabled", False)):
            return None
        mode = str(getattr(self.cfg, "trailing_candle_filter", "all") or "all").strip().lower()
        if mode not in {"all", "opposite"}:
            mode = "all"

        d = (trade_direction or "").strip().upper()
        if d not in {"LONG", "SHORT"}:
            return None

        try:
            o = _as_decimal(self.state.get("prev_candle_open_price"))
            c = _as_decimal(self.state.get("prev_candle_close_price"))
        except Exception:
            return None
        if not _is_pos_finite_decimal(o) or not _is_pos_finite_decimal(c) or o == c:
            return None

        # "all" mode: always tighten by abs body.
        if mode == "all":
            body = (c - o).copy_abs()
            if body <= 0:
                return None
            return (current_sl + body) if d == "LONG" else (current_sl - body)

        # "opposite" mode (original rule):
        # LONG: move SL up only if prev candle bearish (close < open)
        if d == "LONG" and c < o:
            body = o - c
            return current_sl + body
        # SHORT: move SL down only if prev candle bullish (close > open)
        if d == "SHORT" and c > o:
            body = c - o
            return current_sl - body
        return None

    def _trailing_debug(self, msg: str, *args: object) -> None:
        if bool(getattr(self.cfg, "trailing_debug_logs", False)):
            logger.info(msg, *args)

    def _bybit_interval_for_timeframe(self) -> Optional[str]:
        """
        Map bot timeframe (e.g. 1m/5m/1h/4h/1d) to Bybit kline interval.
        Returns interval string accepted by Bybit v5.
        """
        tf = (self.cfg.timeframe or "").strip().lower()
        if not tf:
            return None
        num = ""
        unit = ""
        for ch in tf:
            if ch.isdigit():
                num += ch
            else:
                unit += ch
        if not num or not unit:
            return None
        n = int(num)
        if unit == "m":
            return str(n)
        if unit == "h":
            return str(n * 60)
        if unit == "d":
            return "D" if n == 1 else None
        return None

    async def _bybit_prev_candle_ohlc(self, prev_start_ms: int, tf_ms: int) -> Optional[tuple[Decimal, Decimal, Decimal, Decimal]]:
        """
        Fetch previous candle open/close from Bybit kline API.
        Uses cfg.bybit_* settings.
        """
        interval = self._bybit_interval_for_timeframe()
        if not interval:
            return None
        base_url = (self.cfg.bybit_base_url or "https://api.bybit.com").rstrip("/")
        url = f"{base_url}/v5/market/kline"
        category = str(self.cfg.bybit_category or "linear")
        symbol = str(self.cfg.bybit_symbol or "")
        if not symbol:
            return None
        start = int(prev_start_ms)
        end = int(prev_start_ms + tf_ms - 1)
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(
                    url,
                    params={
                        "category": category,
                        "symbol": symbol,
                        "interval": interval,
                        "start": start,
                        "end": end,
                        "limit": 2,
                    },
                )
                r.raise_for_status()
                payload = r.json()
                if str(payload.get("retCode")) != "0":
                    return None
                lst = (((payload.get("result") or {}).get("list")) or [])
                if not lst:
                    return None
                # Each item: [startTime, open, high, low, close, volume, turnover]
                rows = sorted(lst, key=lambda x: int(x[0]))
                # Find candle matching prev_start_ms
                for it in rows:
                    if int(it[0]) != start:
                        continue
                    o = _as_decimal(it[1])
                    h = _as_decimal(it[2])
                    l = _as_decimal(it[3])
                    cl = _as_decimal(it[4])
                    if _is_pos_finite_decimal(o) and _is_pos_finite_decimal(h) and _is_pos_finite_decimal(l) and _is_pos_finite_decimal(cl):
                        return o, h, l, cl
                # Fallback to first row if exact match not found.
                it = rows[0]
                o = _as_decimal(it[1])
                h = _as_decimal(it[2])
                l = _as_decimal(it[3])
                cl = _as_decimal(it[4])
                if _is_pos_finite_decimal(o) and _is_pos_finite_decimal(h) and _is_pos_finite_decimal(l) and _is_pos_finite_decimal(cl):
                    return o, h, l, cl
        except Exception:
            return None
        return None

    async def _trailing_decision_async(self, *, trade_direction: str, current_sl: Optional[Decimal]) -> dict:
        """
        Returns a dict with detailed trailing decision info for debugging.
        """
        out: dict[str, object] = {"direction": trade_direction, "eligible": False, "reason": None}
        if current_sl is None or not _is_pos_finite_decimal(current_sl):
            out["reason"] = "current_sl_invalid"
            return out
        if not bool(getattr(self.cfg, "trailing_stop_enabled", False)):
            out["reason"] = "disabled"
            return out
        mode = str(getattr(self.cfg, "trailing_candle_filter", "all") or "all").strip().lower()
        if mode not in {"all", "opposite"}:
            mode = "all"
        out["mode"] = mode
        src = str(getattr(self.cfg, "trailing_prev_candle_source", "auto") or "auto").strip().lower()
        if src not in {"state", "bybit_klines", "auto"}:
            src = "auto"
        out["candle_source"] = src

        d = (trade_direction or "").strip().upper()
        if d not in {"LONG", "SHORT"}:
            out["reason"] = "direction_invalid"
            return out

        o: Optional[Decimal] = None
        h: Optional[Decimal] = None
        l: Optional[Decimal] = None
        c: Optional[Decimal] = None
        # 1) Try state candle first (unless forced bybit_klines)
        if src in {"state", "auto"}:
            try:
                o0 = _as_decimal(self.state.get("prev_candle_open_price"))
                c0 = _as_decimal(self.state.get("prev_candle_close_price"))
                if _is_pos_finite_decimal(o0) and _is_pos_finite_decimal(c0):
                    o, c = o0, c0
                    # High/low are optional for body-based trailing; fill if present, else infer from O/C.
                    try:
                        h0 = _as_decimal(self.state.get("prev_candle_high_price"))
                        if _is_pos_finite_decimal(h0):
                            h = h0
                    except Exception:
                        pass
                    try:
                        l0 = _as_decimal(self.state.get("prev_candle_low_price"))
                        if _is_pos_finite_decimal(l0):
                            l = l0
                    except Exception:
                        pass
                    if h is None:
                        h = max(o, c)
                    if l is None:
                        l = min(o, c)
                    out["candle_source_used"] = "state"
            except Exception:
                pass

        # 2) Fallback to Bybit klines if requested/needed
        if (o is None or c is None) and src in {"bybit_klines", "auto"}:
            # Prefer bybit fallback when vwap_source is bybit_klines (typical case).
            vsrc = (self.cfg.vwap_source or "").strip().lower()
            # If user wants candles from Ethereal market prices, don't silently switch to Bybit in auto mode.
            candle_price_src = str(getattr(self.cfg, "trailing_candle_price_source", "market") or "market").strip().lower()
            allow_auto_bybit = not (src == "auto" and candle_price_src == "market")
            if src == "bybit_klines" or (vsrc == "bybit_klines" and allow_auto_bybit):
                tf_ms = self._timeframe_ms(self.cfg.timeframe)
                cur_start_ms = int(self.state.get("last_candle_start_ms") or 0)
                prev_start_ms = int(self.state.get("prev_candle_start_ms") or 0) or (cur_start_ms - tf_ms)
                if prev_start_ms > 0 and tf_ms > 0:
                    ohlc = await self._bybit_prev_candle_ohlc(prev_start_ms, tf_ms)
                    if ohlc is not None:
                        o, h, l, c = ohlc
                    out["candle_source_used"] = "bybit_klines"

        if o is None or c is None:
            out["reason"] = "prev_candle_missing"
            return out

        out["prev_open"] = o
        out["prev_high"] = h
        out["prev_low"] = l
        out["prev_close"] = c
        if o == c:
            out["reason"] = "doji"
            return out

        # Determine candle sign
        out["candle_sign"] = "bull" if c > o else "bear"

        body = (c - o).copy_abs()
        out["body"] = body
        if body <= 0:
            out["reason"] = "zero_body"
            return out

        # "all" mode: always tighten by abs body.
        if mode == "all":
            cand = (current_sl + body) if d == "LONG" else (current_sl - body)
            out["candidate_raw"] = cand
            out["eligible"] = True
            out["reason"] = "all_candles"
            return out

        # "opposite" mode (original rule)
        # LONG: act only on bearish candle
        if d == "LONG":
            if c >= o:
                out["reason"] = "not_bearish_for_long"
                return out
            cand = current_sl + (o - c)
            out["candidate_raw"] = cand
            out["eligible"] = True
            return out

        # SHORT: act only on bullish candle
        if c <= o:
            out["reason"] = "not_bullish_for_short"
            return out
        cand = current_sl - (c - o)
        out["candidate_raw"] = cand
        out["eligible"] = True
        return out

    def _cached_exit_levels(self) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """Return cached (tp, sl) from state.exit_levels if both are valid."""
        levels = self.state.get("exit_levels") or {}
        tp_raw = levels.get("tp")
        sl_raw = levels.get("sl")
        try:
            tp = _as_decimal(tp_raw) if tp_raw is not None else None
        except Exception:
            tp = None
        try:
            sl = _as_decimal(sl_raw) if sl_raw is not None else None
        except Exception:
            sl = None
        if tp is not None and not _is_pos_finite_decimal(tp):
            tp = None
        if sl is not None and not _is_pos_finite_decimal(sl):
            sl = None
        return tp, sl

    def _set_pause_after_sl(
        self,
        minutes: int,
        *,
        vwap: Decimal,
        entry_px: Decimal,
        tp_px: Decimal,
        sl_px: Decimal,
        source: str,
        trade_direction: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> None:
        minutes = max(0, int(minutes))
        if minutes <= 0:
            return
        now = _utc_now()
        now_ts = int(now.timestamp())
        until_ts = now_ts + minutes * 60
        # If we're already paused for SL and the new deadline is nearly identical, skip duplicate pause logs.
        if self.state.get("trading_paused") and str(self.state.get("pause_reason") or "") == "SL_TIMER":
            cur_until = int(self.state.get("pause_until_ts") or 0)
            if cur_until and (until_ts - cur_until) <= 30:
                return
        sname = (strategy or self._cfg_strategy() or "").strip().lower() or "vwap"
        td = self._normalize_direction_value(
            trade_direction or self.state.get("anchor_open_direction") or self.state.get("vwap_trade_direction") or self.direction
        )
        if td not in {"LONG", "SHORT"}:
            td = "LONG"

        self.state["trading_paused"] = True
        self.state["pause_reason"] = "SL_TIMER"
        self.state["pause_source"] = str(source or "")
        self.state["pause_strategy"] = sname
        self.state["pause_trade_direction"] = td
        self.state["paused_at"] = now.isoformat()
        self.state["pause_until_ts"] = int(until_ts)
        self.state["pause_duration_min"] = int(minutes)
        self.state["pause_last_log_ms"] = 0
        # Clear old pause model target to avoid confusion.
        self.state["pause_release_price"] = None
        self._save_state()

        logger.warning(
            "PAUSED after SL: ticker=%s strategy=%s dir=%s minutes=%s until=%s source=%s vwap=%s entry=%s tp=%s sl=%s",
            self.cfg.ticker,
            sname,
            td,
            minutes,
            _ms_to_dt(until_ts * 1000).isoformat(),
            str(source or ""),
            str(vwap),
            str(entry_px),
            str(tp_px),
            str(sl_px),
        )

    def _pause_expired(self, now_ts: int) -> bool:
        until_ts = int(self.state.get("pause_until_ts") or 0)
        if until_ts <= 0:
            # If we have a pause flag without a deadline (legacy/corrupted state), auto-clear it.
            return True
        return now_ts >= until_ts

    def _log_pause_status(self, now_ms: int) -> None:
        """
        Log pause details with throttling (about once per minute).
        """
        last_ms = int(self.state.get("pause_last_log_ms") or 0)
        if last_ms and (now_ms - last_ms) < 60_000:
            return
        self.state["pause_last_log_ms"] = int(now_ms)
        self._save_state()

        until_ts = int(self.state.get("pause_until_ts") or 0)
        now_ts = int(now_ms / 1000)
        remaining = max(0, until_ts - now_ts) if until_ts > 0 else 0
        src = str(self.state.get("pause_source") or "")
        strat = str(self.state.get("pause_strategy") or self._cfg_strategy() or "")
        td = str(self.state.get("pause_trade_direction") or self.direction or "")
        paused_at = str(self.state.get("paused_at") or "")
        logger.info(
            "PAUSED: ticker=%s strategy=%s dir=%s remaining=%ss until=%s source=%s paused_at=%s reason=%s",
            self.cfg.ticker,
            strat,
            td,
            int(remaining),
            (_ms_to_dt(until_ts * 1000).isoformat() if until_ts > 0 else "unknown"),
            src,
            paused_at,
            str(self.state.get("pause_reason") or ""),
        )

    async def step(self) -> None:
        assert self.subaccount_id is not None and self.product_id is not None

        now = _utc_now()
        now_ms = _dt_to_ms(now)

        # Anchor-based VWAP (like original).
        anchor_start = self._anchor_start(now)
        anchor_start_ms = _dt_to_ms(anchor_start)
        prev_anchor = int(self.state.get("last_anchor_start_ms") or 0)
        anchor_changed = prev_anchor != anchor_start_ms
        if anchor_changed:
            # Persist last VWAP of previous anchor (TP reference for anchor_open).
            prev_vwap = self._decimal_from_state("anchor_last_vwap") or self._decimal_from_state("last_vwap")
            if prev_vwap is not None:
                self.state["prev_anchor_vwap"] = str(prev_vwap)
                self.state["prev_anchor_start_ms"] = int(prev_anchor or 0)

            # Reset anchor_open per-anchor markers
            self.state["anchor_open_open_price"] = None
            self.state["anchor_open_open_price_ms"] = 0
            self.state["anchor_open_decision_anchor_ms"] = 0
            self.state["anchor_open_last_wait_log_ms"] = 0

            # Reset VWAP auto-direction markers
            self.state["vwap_anchor_open_price"] = None
            self.state["vwap_anchor_open_price_ms"] = 0
            self.state["vwap_anchor_decision_anchor_ms"] = 0
            self.state["vwap_anchor_direction"] = None
            self.state["vwap_anchor_delta_pct"] = None
            self.state["vwap_anchor_last_wait_log_ms"] = 0

            # Reset per-anchor VWAP tracker for the new anchor.
            self.state["anchor_last_vwap"] = None
            self.state["anchor_last_vwap_ms"] = 0
            self.state["anchor_last_vwap_anchor_ms"] = int(anchor_start_ms)

            # Reset VWAP accumulators for new anchor period
            self.state["last_anchor_start_ms"] = anchor_start_ms
            self.state["cum_pq"] = "0"
            self.state["cum_q"] = "0"
            self.state["last_trade_ts"] = anchor_start_ms
            self._save_state()
            await self._cancel_entry()
            # Important safety rule:
            # do NOT cancel TP/SL on anchor change — VWAP can be NaN for a while at new anchor start.
            # We'll refresh exits later when we have valid levels.
            logger.info("New anchor period: %s", anchor_start.isoformat())

        # Candle boundary (timeframe) for "full refresh" logic (like original).
        tf_ms = self._timeframe_ms(self.cfg.timeframe)
        candle_start_ms = now_ms - (now_ms % tf_ms)
        is_new_candle = int(self.state.get("last_candle_start_ms") or 0) != candle_start_ms
        if is_new_candle:
            # Finalize previous candle tracking (for trailing stop)
            self.state["prev_candle_open_price"] = self.state.get("candle_open_price")
            self.state["prev_candle_close_price"] = self.state.get("candle_close_price")
            self.state["prev_candle_high_price"] = self.state.get("candle_high_price")
            self.state["prev_candle_low_price"] = self.state.get("candle_low_price")
            self.state["prev_candle_start_ms"] = int(self.state.get("candle_start_ms") or 0)
            # Reset current candle
            self.state["candle_open_price"] = None
            self.state["candle_close_price"] = None
            self.state["candle_high_price"] = None
            self.state["candle_low_price"] = None
            self.state["candle_start_ms"] = int(candle_start_ms)

            self.state["last_candle_start_ms"] = candle_start_ms
            self._save_state()
            if self.state.get("trading_paused"):
                until_ts = int(self.state.get("pause_until_ts") or 0)
                now_ts = int(now.timestamp())
                remaining = max(0, until_ts - now_ts) if until_ts > 0 else 0
                logger.info(
                    "NEW CANDLE %s — PAUSED ticker=%s strategy=%s dir=%s remaining=%ss until=%s",
                    _ms_to_dt(candle_start_ms).isoformat(),
                    self.cfg.ticker,
                    str(self.state.get("pause_strategy") or self._cfg_strategy() or ""),
                    str(self.state.get("pause_trade_direction") or self.direction or ""),
                    int(remaining),
                    (_ms_to_dt(until_ts * 1000).isoformat() if until_ts > 0 else "unknown"),
                )
            else:
                logger.info("NEW CANDLE %s — full refresh", _ms_to_dt(candle_start_ms).isoformat())
            self._trailing_debug(
                "TRAIL DEBUG: new_candle_start=%s prev_candle_start=%s prev_open=%s prev_close=%s",
                _ms_to_dt(candle_start_ms).isoformat(),
                (_ms_to_dt(int(self.state.get("prev_candle_start_ms") or 0)).isoformat() if int(self.state.get("prev_candle_start_ms") or 0) else "0"),
                str(self.state.get("prev_candle_open_price")),
                str(self.state.get("prev_candle_close_price")),
            )

        # Network errors can happen; keep trading loop alive.
        try:
            price = await self.get_oracle_price()
        except Exception as e:
            if _is_connect_error(e):
                now_ms_local = _dt_to_ms(_utc_now())
                last_log = int(self.state.get("last_connect_error_log_ms") or 0)
                if not last_log or (now_ms_local - last_log) > 30_000:
                    self.state["last_connect_error_log_ms"] = now_ms_local
                    self._save_state()
                    logger.warning("Network error fetching price (%s %s): %s", self.direction, self.cfg.ticker, type(e).__name__)
                price = Decimal("NaN")
            else:
                raise
        try:
            vwap = await self._get_vwap(anchor_start_ms, now_ms)
        except Exception as e:
            if _is_connect_error(e):
                now_ms_local = _dt_to_ms(_utc_now())
                last_log = int(self.state.get("last_connect_error_log_ms") or 0)
                if not last_log or (now_ms_local - last_log) > 30_000:
                    self.state["last_connect_error_log_ms"] = now_ms_local
                    self._save_state()
                    logger.warning("Network error fetching vwap (%s %s): %s", self.direction, self.cfg.ticker, type(e).__name__)
                vwap = Decimal("NaN")
            else:
                raise
        # Reference price for post-only repricing and (optionally) candle tracking:
        # Prefer market price; if unavailable (0/NaN), fall back to VWAP (useful when vwap_source=bybit_klines).
        ref_price = price if _is_pos_finite_decimal(price) else (vwap if _is_pos_finite_decimal(vwap) else Decimal("NaN"))

        # For VWAP auto-direction: capture anchor open price as soon as we have a valid reference price.
        self._maybe_capture_vwap_anchor_open_price(anchor_start_ms=anchor_start_ms, now_ms=now_ms, price=ref_price)

        # Candle price source (for trailing candles):
        candle_price_src = str(getattr(self.cfg, "trailing_candle_price_source", "market") or "market").strip().lower()
        if candle_price_src not in {"market", "ref"}:
            candle_price_src = "market"
        candle_price = price if candle_price_src == "market" else ref_price

        # Persist last known reference price snapshot for robust anchor transitions and repricing.
        if _is_pos_finite_decimal(ref_price):
            prev_saved = self._price_from_state("last_price") or Decimal("0")
            self.state["last_price"] = str(ref_price)
            self.state["last_price_ms"] = int(now_ms)
            self.state["last_price_anchor_ms"] = int(anchor_start_ms)
            if prev_saved <= 0 or (ref_price - prev_saved).copy_abs() / ref_price > Decimal("0.0005"):
                self._save_state()

        # Update candle open/high/low/close tracking from selected candle price.
        if _is_pos_finite_decimal(candle_price):
            if int(self.state.get("candle_start_ms") or 0) != int(candle_start_ms):
                self.state["candle_start_ms"] = int(candle_start_ms)
                self.state["candle_open_price"] = None
                self.state["candle_close_price"] = None
                self.state["candle_high_price"] = None
                self.state["candle_low_price"] = None
            if self.state.get("candle_open_price") is None:
                self.state["candle_open_price"] = str(candle_price)
                self.state["candle_high_price"] = str(candle_price)
                self.state["candle_low_price"] = str(candle_price)
            self.state["candle_close_price"] = str(candle_price)
            # Update high/low
            try:
                hi = _as_decimal(self.state.get("candle_high_price"))
            except Exception:
                hi = candle_price
            try:
                lo = _as_decimal(self.state.get("candle_low_price"))
            except Exception:
                lo = candle_price
            if _is_pos_finite_decimal(hi) and candle_price > hi:
                self.state["candle_high_price"] = str(candle_price)
            if _is_pos_finite_decimal(lo) and candle_price < lo:
                self.state["candle_low_price"] = str(candle_price)
            self._trailing_debug(
                "TRAIL DEBUG: candle_update ticker=%s candle_start=%s o=%s h=%s l=%s c=%s ref=%s (market=%s vwap=%s)",
                self.cfg.ticker,
                _ms_to_dt(int(self.state.get("candle_start_ms") or 0)).isoformat(),
                str(self.state.get("candle_open_price")),
                str(self.state.get("candle_high_price")),
                str(self.state.get("candle_low_price")),
                str(self.state.get("candle_close_price")),
                str(candle_price),
                str(price),
                str(vwap),
            )

        # Persist last known VWAP snapshot for anchor_open TP (works for any vwap_source).
        if (vwap == vwap) and vwap > 0:
            prev_saved_vwap = self._decimal_from_state("last_vwap") or Decimal("0")
            self.state["last_vwap"] = str(vwap)
            self.state["last_vwap_ms"] = int(now_ms)
            self.state["last_vwap_anchor_ms"] = int(anchor_start_ms)
            if prev_saved_vwap <= 0 or (vwap - prev_saved_vwap).copy_abs() / vwap > Decimal("0.0005"):
                self._save_state()

            # Also persist "last VWAP of this anchor" (updated only when anchor matches).
            prev_anchor_vwap = self._decimal_from_state("anchor_last_vwap") or Decimal("0")
            self.state["anchor_last_vwap"] = str(vwap)
            self.state["anchor_last_vwap_ms"] = int(now_ms)
            self.state["anchor_last_vwap_anchor_ms"] = int(anchor_start_ms)
            if prev_anchor_vwap <= 0 or (vwap - prev_anchor_vwap).copy_abs() / vwap > Decimal("0.0005"):
                self._save_state()

        pos = await self._get_open_position()
        pos_size = _as_decimal((pos or {}).get("size") or "0").copy_abs() if pos else Decimal("0")
        has_pos = bool(pos and pos_size > 0)

        # Persist last position size to detect transitions reliably.
        prev_size = _as_decimal(self.state.get("last_position_size") or "0").copy_abs()
        self.state["last_position_size"] = str(pos_size)
        # Track consecutive "no position" observations to avoid acting on transient position API glitches.
        try:
            streak = int(self.state.get("no_position_streak") or 0)
        except Exception:
            streak = 0
        if has_pos:
            streak = 0
        else:
            streak = min(10_000, streak + 1)
        self.state["no_position_streak"] = int(streak)
        # NOTE: we save state only on meaningful events to avoid excessive writes.

        opened_transition = (not _is_pos_finite_decimal(prev_size) or prev_size <= 0) and pos_size > 0
        if has_pos and not bool(self.state.get("opened_since_startup")):
            self.state["opened_since_startup"] = True
            self.state["opened_since_startup_ms"] = int(now_ms)
            self._save_state()

        trade_dir = self._resolve_vwap_auto_direction(
            anchor_start_ms=anchor_start_ms,
            now_ms=now_ms,
            price=ref_price,
            has_pos=has_pos,
            pos=pos,
        )
        if opened_transition:
            od = trade_dir or self._infer_direction_from_position(pos) or self._pause_trade_direction()
            logger.info("POSITION OPENED: ticker=%s dir=%s size=%s", self.cfg.ticker, str(od), str(pos_size))
        entry_px, tp_px, sl_px = self._levels(vwap, direction=trade_dir)

        # Handle SL/TP fills even if position visibility lags.
        handled = await self._check_and_handle_exit_fills(
            vwap=vwap,
            entry_px=entry_px,
            tp_px=tp_px,
            sl_px=sl_px,
            trade_direction=trade_dir,
        )
        if handled:
            return

        # Common close detection (all strategies): if position isn't visible, detect SL/TP and apply pause.
        if not has_pos:
            handled_close = await self._handle_no_position_close_and_pause(
                prev_pos_size=prev_size,
                pos_size=pos_size,
                vwap=vwap,
                entry_px=entry_px,
                tp_px=tp_px,
                sl_px=sl_px,
                trade_direction=trade_dir,
            )
            if handled_close:
                return

        # Handle pause-after-SL (timer model)
        if self.state.get("trading_paused"):
            now_ts = int(now.timestamp())
            if self._pause_expired(now_ts):
                # Clear pause and resume.
                self.state["trading_paused"] = False
                self.state["pause_reason"] = None
                self.state["paused_at"] = None
                self.state["pause_until_ts"] = 0
                self.state["pause_duration_min"] = 0
                self.state["pause_last_log_ms"] = 0
                self.state["pause_release_price"] = None
                self._save_state()
                logger.warning(
                    "PAUSE cleared: %s %s resumed.",
                    str(self.state.get("pause_trade_direction") or self._pause_trade_direction()),
                    self.cfg.ticker,
                )
            else:
                self._log_pause_status(now_ms)
                return

        # Strategy switch
        if self._cfg_strategy() == "anchor_open":
            await self._anchor_open_step(
                now=now,
                now_ms=now_ms,
                anchor_start_ms=anchor_start_ms,
                prev_anchor_ms=prev_anchor,
                price=ref_price,
                vwap=vwap,
                pos=pos,
                has_pos=has_pos,
                is_new_candle=is_new_candle,
            )
            return

        if self._cfg_strategy() == "vwap" and trade_dir is None:
            if has_pos:
                return
            if self.state.get("exit_order_ids"):
                return
            if self.state.get("entry_order_id"):
                await self._cancel_entry()
                self._save_state()
            self._clear_vwap_trade_direction()
            return

        if not has_pos:
            # If we still have active exits recorded, do not place new entries (position visibility can lag).
            if self.state.get("exit_order_ids"):
                return

            # On every new candle, re-place entry at fresh level (like original).
            if is_new_candle:
                await self._cancel_entry()
                logger.info(
                    "NEW CANDLE refresh: vwap=%s entry=%s tp=%s sl=%s (source=%s)",
                    str(vwap),
                    str(entry_px),
                    str(tp_px),
                    str(sl_px),
                    str(self.cfg.vwap_source),
                )
            await self._ensure_entry_order(entry_px, ref_price=ref_price, direction=trade_dir)
            return

        # Position exists:
        # - Cancel stale entry if still recorded
        if self.state.get("entry_order_id"):
            await self._cancel_entry()
            self._save_state()

        # Ensure exits exist (OCO TP+SL).
        # On each new candle we refresh:
        # - TP always follows the newly computed level
        # - SL only tightens (reduces risk); widening is ignored.
        if is_new_candle:
            # If we don't have valid VWAP-derived levels, keep existing exits untouched.
            if not _is_pos_finite_decimal(tp_px) or not _is_pos_finite_decimal(sl_px):
                if self.state.get("exit_order_ids"):
                    logger.info("EXITS unchanged: levels unavailable (tp=%s sl=%s). Keeping existing TP/SL.", str(tp_px), str(sl_px))
                else:
                    # If exits are missing but we have cached levels, place from cache for safety.
                    c_tp, c_sl = self._cached_exit_levels()
                    if c_tp is not None and c_sl is not None:
                        logger.warning(
                            "EXITS missing and levels unavailable; restoring from cache TP=%s SL=%s",
                            str(c_tp),
                            str(c_sl),
                        )
                        await self._ensure_oco_exits(pos, c_tp, c_sl, direction=trade_dir)
                    else:
                        logger.warning("EXITS missing and levels unavailable (tp=%s sl=%s). Position may be unprotected.", str(tp_px), str(sl_px))
                return

            # If exit levels cache is missing but we have exit orders, hydrate it from API to avoid widening SL.
            if self.state.get("exit_order_ids") and (not (self.state.get("exit_levels") or {}).get("sl")):
                await self._hydrate_exit_levels_from_api()

            cur_sl: Optional[Decimal] = None
            try:
                cur_sl_raw = ((self.state.get("exit_levels") or {}).get("sl"))
                cur_sl = _as_decimal(cur_sl_raw) if cur_sl_raw is not None else None
            except Exception:
                cur_sl = None

            # If we still don't know the current SL but exits exist, do not refresh (could widen accidentally).
            if self.state.get("exit_order_ids") and not _is_pos_finite_decimal(cur_sl or Decimal("NaN")):
                logger.info("EXITS unchanged: current SL unknown (missing cache). Keeping existing TP/SL.")
                return

            # Start with VWAP-derived SL tightening.
            new_sl = self._tightened_sl(current_sl=cur_sl, desired_sl=sl_px, trade_direction=trade_dir)
            # Apply trailing stop rule (relative to current SL, using prev candle body).
            trail_info = await self._trailing_decision_async(trade_direction=trade_dir or "", current_sl=cur_sl)
            trail = None
            if bool(trail_info.get("eligible")):
                try:
                    trail_raw = trail_info.get("candidate_raw")
                    trail = _as_decimal(trail_raw) if trail_raw is not None else None
                except Exception:
                    trail = None
            self._trailing_debug(
                "TRAIL DEBUG: vwap trailing decision ticker=%s dir=%s eligible=%s reason=%s prev_o=%s prev_h=%s prev_l=%s prev_c=%s body=%s current_sl=%s candidate_raw=%s",
                self.cfg.ticker,
                trade_dir,
                str(trail_info.get("eligible")),
                str(trail_info.get("reason")),
                str(trail_info.get("prev_open")),
                str(trail_info.get("prev_high")),
                str(trail_info.get("prev_low")),
                str(trail_info.get("prev_close")),
                str(trail_info.get("body")),
                str(cur_sl),
                str(trail_info.get("candidate_raw")),
            )
            if trail is not None and _is_pos_finite_decimal(trail):
                # Round trailing candidate to tick size before comparing/applying.
                trail_r = self._round_price(trail)
                new_sl = self._tightened_sl(current_sl=new_sl, desired_sl=trail_r, trade_direction=trade_dir)

            # If we have exits, refresh them with the tightened SL logic.
            if self.state.get("exit_order_ids"):
                # If nothing changes after rounding, keep exits (avoid cancel/recreate gaps).
                try:
                    cur_tp_raw = ((self.state.get("exit_levels") or {}).get("tp"))
                    cur_tp = _as_decimal(cur_tp_raw) if cur_tp_raw is not None else None
                except Exception:
                    cur_tp = None
                if _is_pos_finite_decimal(cur_tp or Decimal("NaN")) and _is_pos_finite_decimal(cur_sl or Decimal("NaN")):
                    r_tp_new = self._round_price(tp_px)
                    r_sl_new = self._round_price(new_sl)
                    r_tp_cur = self._round_price(cur_tp)  # type: ignore[arg-type]
                    r_sl_cur = self._round_price(cur_sl)  # type: ignore[arg-type]
                    if r_tp_new == r_tp_cur and r_sl_new == r_sl_cur:
                        return

                # Log when we refuse to widen SL
                if cur_sl is not None and _is_pos_finite_decimal(cur_sl) and _is_pos_finite_decimal(sl_px):
                    if trade_dir == "LONG" and sl_px < cur_sl:
                        logger.info("SL not widened (LONG): current_sl=%s desired_sl=%s -> keeping %s", str(cur_sl), str(sl_px), str(new_sl))
                    if trade_dir == "SHORT" and sl_px > cur_sl:
                        logger.info("SL not widened (SHORT): current_sl=%s desired_sl=%s -> keeping %s", str(cur_sl), str(sl_px), str(new_sl))
                if cur_sl is not None and _is_pos_finite_decimal(cur_sl) and trail is not None and _is_pos_finite_decimal(trail):
                    logger.info(
                        "Trailing SL candidate: prev_open=%s prev_close=%s current_sl=%s trail=%s -> chosen=%s",
                        str(trail_info.get("prev_open")),
                        str(trail_info.get("prev_close")),
                        str(cur_sl),
                        str(self._round_price(trail)),
                        str(self._round_price(new_sl)),
                    )

                await self._cancel_exits()
                await self._ensure_oco_exits(pos, tp_px, new_sl, direction=trade_dir)
            else:
                await self._ensure_oco_exits(pos, tp_px, new_sl, direction=trade_dir)
        else:
            # Outside new-candle refresh, still ensure we have exits.
            if not self.state.get("exit_order_ids"):
                if _is_pos_finite_decimal(tp_px) and _is_pos_finite_decimal(sl_px):
                    await self._ensure_oco_exits(pos, tp_px, sl_px, direction=trade_dir)
                else:
                    c_tp, c_sl = self._cached_exit_levels()
                    if c_tp is not None and c_sl is not None:
                        logger.warning("EXITS missing; restoring from cache TP=%s SL=%s", str(c_tp), str(c_sl))
                        await self._ensure_oco_exits(pos, c_tp, c_sl, direction=trade_dir)
            else:
                await self._ensure_oco_exits(pos, tp_px, sl_px, direction=trade_dir)

    async def run(self) -> None:
        logger.info("Starting VWAP strategy: %s %s", self.direction, self.cfg.ticker)
        while True:
            try:
                await self.step()
            except KeyboardInterrupt:
                raise
            except Exception:
                # Some exceptions (e.g., AssertionError) have empty str(e); keep full traceback.
                logger.exception("Loop error in %s %s", self.direction, self.cfg.ticker)
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
                strategy=str(raw.get("strategy", "vwap")),
                trailing_stop_enabled=bool(raw.get("trailing_stop_enabled", False)),
                trailing_candle_filter=str(raw.get("trailing_candle_filter", "all")),
                trailing_candle_price_source=str(raw.get("trailing_candle_price_source", "market")),
                trailing_prev_candle_source=str(raw.get("trailing_prev_candle_source", "auto")),
                ticker=str(raw.get("ticker", "SOLUSD")),
                direction=str(raw.get("direction", "LONG")).upper(),
                vwap_both_min_delta_pct=_as_decimal(raw.get("vwap_both_min_delta_pct", "0.1")),
                vwap_both_max_delta_pct=_as_decimal(raw.get("vwap_both_max_delta_pct", "0")),
                vwap_both_start_direction=str(raw.get("vwap_both_start_direction", "NONE")),
                vwap_both_mode=str(raw.get("vwap_both_mode", "trend")),
                poll_interval_sec=int(raw.get("poll_interval_sec", 3)),
                timeframe=str(raw.get("timeframe", "1h")),
                anchor_period=str(raw.get("anchor_period", "Session")),
                entry_distance_long_pct=_as_decimal(raw.get("entry_distance_long_pct", "1.0")),
                entry_distance_short_pct=_as_decimal(raw.get("entry_distance_short_pct", "1.5")),
                entry_quantity=_as_decimal(raw.get("entry_quantity", "0.001")),
                post_only=bool(raw.get("post_only", True)),
                entry_expires_in_sec=int(raw.get("entry_expires_in_sec", 7 * 24 * 3600)),
                tp_pct=_as_decimal(raw.get("tp_pct", "1.5")),
                sl_pct=_as_decimal(raw.get("sl_pct", "4.0")),
                exits_as_stop_market=bool(raw.get("exits_as_stop_market", True)),
                exit_expires_in_sec=int(raw.get("exit_expires_in_sec", 7 * 24 * 3600)),
                pause_on_sl=bool(raw.get("pause_on_sl", False)),
                pause_after_sl_minutes=int(raw.get("pause_after_sl_minutes", 0) or 0),
                pause_skip_on_startup=bool(raw.get("pause_skip_on_startup", False)),
                pause_ignore_sl_until_first_open=bool(raw.get("pause_ignore_sl_until_first_open", False)),
                anchor_open_min_delta_pct=_as_decimal(raw.get("anchor_open_min_delta_pct", raw.get("anchor_open_min_vwap_delta_pct", "0"))),
                anchor_open_sl_pct_of_tp=_as_decimal(raw.get("anchor_open_sl_pct_of_tp", "100")),
                anchor_open_close_on_anchor_end=bool(raw.get("anchor_open_close_on_anchor_end", True)),
                max_trade_pages=int(raw.get("max_trade_pages", 6)),
                trades_page_limit=trades_page_limit,
                vwap_recalc_threshold=_as_decimal(raw.get("vwap_recalc_threshold", "0.0005")),
                trade_overlap_ms=int(raw.get("trade_overlap_ms", 2000)),
                trade_id_cache_size=int(raw.get("trade_id_cache_size", 5000)),
                overflow_max_trade_pages=int(raw.get("overflow_max_trade_pages", 50)),
                vwap_source=str(raw.get("vwap_source", "ethereal_trades")),
                bybit_base_url=str(raw.get("bybit_base_url", "https://api.bybit.com")),
                bybit_category=str(raw.get("bybit_category", "linear")),
                bybit_symbol=str(raw.get("bybit_symbol", "SOLUSDT")),
                bybit_kline_interval=str(raw.get("bybit_kline_interval", "1")),
            ),
            False,
        )

    cfg = StrategyConfig()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "strategy": cfg.strategy,
                "trailing_stop_enabled": cfg.trailing_stop_enabled,
                "trailing_candle_filter": cfg.trailing_candle_filter,
                "trailing_candle_price_source": cfg.trailing_candle_price_source,
                "trailing_prev_candle_source": cfg.trailing_prev_candle_source,
                "ticker": cfg.ticker,
                "direction": cfg.direction,
                "vwap_both_min_delta_pct": str(cfg.vwap_both_min_delta_pct),
                "vwap_both_max_delta_pct": str(getattr(cfg, "vwap_both_max_delta_pct", Decimal("0"))),
                "vwap_both_start_direction": str(getattr(cfg, "vwap_both_start_direction", "NONE")),
                "vwap_both_mode": str(getattr(cfg, "vwap_both_mode", "trend")),
                "poll_interval_sec": cfg.poll_interval_sec,
                "timeframe": cfg.timeframe,
                "anchor_period": cfg.anchor_period,
                "entry_distance_long_pct": str(cfg.entry_distance_long_pct),
                "entry_distance_short_pct": str(cfg.entry_distance_short_pct),
                "entry_quantity": str(cfg.entry_quantity),
                "post_only": cfg.post_only,
                "entry_expires_in_sec": cfg.entry_expires_in_sec,
                "tp_pct": str(cfg.tp_pct),
                "sl_pct": str(cfg.sl_pct),
                "exits_as_stop_market": cfg.exits_as_stop_market,
                "exit_expires_in_sec": cfg.exit_expires_in_sec,
                "pause_on_sl": cfg.pause_on_sl,
                "pause_after_sl_minutes": cfg.pause_after_sl_minutes,
                "pause_skip_on_startup": bool(getattr(cfg, "pause_skip_on_startup", False)),
                "pause_ignore_sl_until_first_open": bool(getattr(cfg, "pause_ignore_sl_until_first_open", False)),
                "anchor_open_min_delta_pct": str(cfg.anchor_open_min_delta_pct),
                "anchor_open_sl_pct_of_tp": str(cfg.anchor_open_sl_pct_of_tp),
                "anchor_open_close_on_anchor_end": cfg.anchor_open_close_on_anchor_end,
                "max_trade_pages": cfg.max_trade_pages,
                "trades_page_limit": cfg.trades_page_limit,
                "vwap_recalc_threshold": str(cfg.vwap_recalc_threshold),
                "trade_overlap_ms": cfg.trade_overlap_ms,
                "trade_id_cache_size": cfg.trade_id_cache_size,
                "overflow_max_trade_pages": cfg.overflow_max_trade_pages,
                "vwap_source": cfg.vwap_source,
                "bybit_base_url": cfg.bybit_base_url,
                "bybit_category": cfg.bybit_category,
                "bybit_symbol": cfg.bybit_symbol,
                "bybit_kline_interval": cfg.bybit_kline_interval,
            },
            f,
            indent=2,
        )
    logger.info("Config created: %s (edit it and re-run)", path)
    return cfg, True


def _truthy_env(name: str) -> bool:
    return (os.getenv(name, "") or "").strip().lower() in {"1", "true", "y", "yes", "on"}


def _parse_csv_upper(s: str) -> list[str]:
    return [x.strip().upper() for x in (s or "").split(",") if x.strip()]


async def _retry_on_connect_error(name: str, coro_factory, *, base_delay: float = 1.0, max_delay: float = 30.0) -> Any:
    """
    Retry network calls on transient connect errors.
    coro_factory: zero-arg callable that returns an awaitable.
    """
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except Exception as e:
            if not _is_connect_error(e):
                raise
            attempt += 1
            delay = min(max_delay, base_delay * (2 ** min(attempt, 6)))
            logger.warning("Network error during %s (attempt=%s). Retrying in %.1fs.", name, attempt, delay)
            await asyncio.sleep(delay)


def _strategy_config_from_json(raw: dict) -> StrategyConfig:
    trades_page_limit = int(raw.get("trades_page_limit", 200))
    trades_page_limit = min(max(trades_page_limit, 1), 200)  # clamp to API max
    return StrategyConfig(
        strategy=str(raw.get("strategy", "vwap")),
        trailing_stop_enabled=bool(raw.get("trailing_stop_enabled", False)),
        trailing_candle_filter=str(raw.get("trailing_candle_filter", "all")),
        trailing_candle_price_source=str(raw.get("trailing_candle_price_source", "market")),
        trailing_prev_candle_source=str(raw.get("trailing_prev_candle_source", "auto")),
        ticker=str(raw.get("ticker", "SOLUSD")).strip().upper(),
        direction=str(raw.get("direction", "LONG")).strip().upper(),
        vwap_both_min_delta_pct=_as_decimal(raw.get("vwap_both_min_delta_pct", "0.1")),
        vwap_both_max_delta_pct=_as_decimal(raw.get("vwap_both_max_delta_pct", "0")),
        vwap_both_start_direction=str(raw.get("vwap_both_start_direction", "NONE")),
        vwap_both_mode=str(raw.get("vwap_both_mode", "trend")),
        poll_interval_sec=int(raw.get("poll_interval_sec", 3)),
        timeframe=str(raw.get("timeframe", "1h")),
        anchor_period=str(raw.get("anchor_period", "Session")),
        entry_distance_long_pct=_as_decimal(raw.get("entry_distance_long_pct", "1.0")),
        entry_distance_short_pct=_as_decimal(raw.get("entry_distance_short_pct", "1.5")),
        entry_quantity=_as_decimal(raw.get("entry_quantity", "0.001")),
        post_only=bool(raw.get("post_only", True)),
        entry_expires_in_sec=int(raw.get("entry_expires_in_sec", 7 * 24 * 3600)),
        tp_pct=_as_decimal(raw.get("tp_pct", "1.5")),
        sl_pct=_as_decimal(raw.get("sl_pct", "4.0")),
        exits_as_stop_market=bool(raw.get("exits_as_stop_market", True)),
        exit_expires_in_sec=int(raw.get("exit_expires_in_sec", 7 * 24 * 3600)),
        pause_on_sl=bool(raw.get("pause_on_sl", False)),
        pause_after_sl_minutes=int(raw.get("pause_after_sl_minutes", 0) or 0),
        pause_skip_on_startup=bool(raw.get("pause_skip_on_startup", False)),
        pause_ignore_sl_until_first_open=bool(raw.get("pause_ignore_sl_until_first_open", False)),
        anchor_open_min_delta_pct=_as_decimal(raw.get("anchor_open_min_delta_pct", raw.get("anchor_open_min_vwap_delta_pct", "0"))),
        anchor_open_sl_pct_of_tp=_as_decimal(raw.get("anchor_open_sl_pct_of_tp", "100")),
        anchor_open_close_on_anchor_end=bool(raw.get("anchor_open_close_on_anchor_end", True)),
        max_trade_pages=int(raw.get("max_trade_pages", 6)),
        trades_page_limit=trades_page_limit,
        vwap_recalc_threshold=_as_decimal(raw.get("vwap_recalc_threshold", "0.0005")),
        trade_overlap_ms=int(raw.get("trade_overlap_ms", 2000)),
        trade_id_cache_size=int(raw.get("trade_id_cache_size", 5000)),
        overflow_max_trade_pages=int(raw.get("overflow_max_trade_pages", 50)),
        vwap_source=str(raw.get("vwap_source", "ethereal_trades")),
        bybit_base_url=str(raw.get("bybit_base_url", "https://api.bybit.com")),
        bybit_category=str(raw.get("bybit_category", "linear")),
        bybit_symbol=str(raw.get("bybit_symbol", "SOLUSDT")),
        bybit_kline_interval=str(raw.get("bybit_kline_interval", "1")),
    )


def _strategy_config_to_json(cfg: StrategyConfig) -> dict:
    return {
        "strategy": str(getattr(cfg, "strategy", "vwap")),
        "trailing_stop_enabled": bool(getattr(cfg, "trailing_stop_enabled", False)),
        "trailing_candle_filter": str(getattr(cfg, "trailing_candle_filter", "all")),
        "trailing_candle_price_source": str(getattr(cfg, "trailing_candle_price_source", "market")),
        "trailing_prev_candle_source": str(getattr(cfg, "trailing_prev_candle_source", "auto")),
        "ticker": cfg.ticker,
        "direction": cfg.direction,
        "vwap_both_min_delta_pct": str(getattr(cfg, "vwap_both_min_delta_pct", Decimal("0.1"))),
        "vwap_both_max_delta_pct": str(getattr(cfg, "vwap_both_max_delta_pct", Decimal("0"))),
        "vwap_both_start_direction": str(getattr(cfg, "vwap_both_start_direction", "NONE")),
        "vwap_both_mode": str(getattr(cfg, "vwap_both_mode", "trend")),
        "poll_interval_sec": cfg.poll_interval_sec,
        "timeframe": cfg.timeframe,
        "anchor_period": cfg.anchor_period,
        "entry_distance_long_pct": str(cfg.entry_distance_long_pct),
        "entry_distance_short_pct": str(cfg.entry_distance_short_pct),
        "entry_quantity": str(cfg.entry_quantity),
        "post_only": cfg.post_only,
        "entry_expires_in_sec": cfg.entry_expires_in_sec,
        "tp_pct": str(cfg.tp_pct),
        "sl_pct": str(cfg.sl_pct),
        "exits_as_stop_market": cfg.exits_as_stop_market,
        "exit_expires_in_sec": cfg.exit_expires_in_sec,
        "pause_on_sl": cfg.pause_on_sl,
        "pause_after_sl_minutes": int(getattr(cfg, "pause_after_sl_minutes", 0) or 0),
        "pause_skip_on_startup": bool(getattr(cfg, "pause_skip_on_startup", False)),
        "pause_ignore_sl_until_first_open": bool(getattr(cfg, "pause_ignore_sl_until_first_open", False)),
        "anchor_open_min_delta_pct": str(getattr(cfg, "anchor_open_min_delta_pct", Decimal("0"))),
        "anchor_open_sl_pct_of_tp": str(getattr(cfg, "anchor_open_sl_pct_of_tp", Decimal("100"))),
        "anchor_open_close_on_anchor_end": bool(getattr(cfg, "anchor_open_close_on_anchor_end", True)),
        "max_trade_pages": cfg.max_trade_pages,
        "trades_page_limit": cfg.trades_page_limit,
        "vwap_recalc_threshold": str(cfg.vwap_recalc_threshold),
        "trade_overlap_ms": cfg.trade_overlap_ms,
        "trade_id_cache_size": cfg.trade_id_cache_size,
        "overflow_max_trade_pages": cfg.overflow_max_trade_pages,
        "vwap_source": cfg.vwap_source,
        "bybit_base_url": cfg.bybit_base_url,
        "bybit_category": cfg.bybit_category,
        "bybit_symbol": cfg.bybit_symbol,
        "bybit_kline_interval": cfg.bybit_kline_interval,
    }


def _load_or_create_multi_config(path: str) -> tuple[list[StrategyConfig], int, bool]:
    """
    Multi-instrument config format:

    {
      "subaccount_index": 0,
      "strategies": [
        { ... StrategyConfig fields ... },
        { ... }
      ]
    }
    """
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
        sub_idx = int(raw.get("subaccount_index", 0))
        items = raw.get("strategies") or []
        cfgs = [_strategy_config_from_json(x or {}) for x in items]
        # de-dupe accidental empty entries
        cfgs = [c for c in cfgs if (c.ticker or "").strip()]
        return cfgs, sub_idx, False

    # Create a template file.
    tickers = _parse_csv_upper(os.getenv("ETHEREAL_TICKERS", ""))
    if not tickers:
        tickers = [(os.getenv("ETHEREAL_TICKER") or "SOLUSD").strip().upper()]
    sub_idx = int(os.getenv("ETHEREAL_SUBACCOUNT_INDEX", "0"))
    template_cfgs: list[StrategyConfig] = []
    for t in tickers:
        c = StrategyConfig(ticker=t)
        template_cfgs.append(c)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "subaccount_index": sub_idx,
                "strategies": [_strategy_config_to_json(c) for c in template_cfgs],
            },
            f,
            indent=2,
        )
    logger.info("Multi-config created: %s (edit it and re-run)", path)
    return template_cfgs, sub_idx, True


async def amain() -> None:
    testnet = (os.getenv("ETHEREAL_TESTNET", "1").strip().lower() in {"1", "true", "y", "yes"})
    network = "testnet" if testnet else "mainnet"
    base_url = os.getenv("ETHEREAL_BASE_URL") or ("https://api.etherealtest.net" if testnet else "https://api.ethereal.trade")
    rpc_url = os.getenv("ETHEREAL_RPC_URL") or ("https://rpc.etherealtest.net" if testnet else "https://rpc.ethereal.trade")
    private_key = (os.getenv("ETHEREAL_PRIVATE_KEY") or "").strip()
    if not private_key:
        raise RuntimeError("Set ETHEREAL_PRIVATE_KEY (EVM private key) in env.")

    client = await _retry_on_connect_error(
        "AsyncRESTClient.create()",
        lambda: AsyncRESTClient.create(
            {
                "network": network,
                "base_url": base_url,
                "chain_config": {
                    "rpc_url": rpc_url,
                    "private_key": private_key,
                },
            }
        ),
    )
    try:
        use_multi = os.path.exists(MULTI_CONFIG_FILE) or _truthy_env("ETHEREAL_MULTI") or bool(os.getenv("ETHEREAL_TICKERS"))

        if use_multi:
            cfgs, sub_idx_from_file, created = _load_or_create_multi_config(MULTI_CONFIG_FILE)
            if created:
                # Avoid continuing with defaults on first run (prevents confusing errors).
                return
            if not cfgs:
                raise RuntimeError(f"{MULTI_CONFIG_FILE} has no strategies. Add at least 1 strategy and re-run.")

            # Safety: this bot is NOT hedge-mode. Running multiple strategies on the same ticker in the same
            # subaccount will cause exit/position management conflicts and can stall trading (stale exits, cancels, etc.).
            # If duplicates are identical, keep the first entry and warn (helps recover from accidental copy/paste).
            by_ticker: dict[str, StrategyConfig] = {}
            dup: dict[str, list[StrategyConfig]] = {}
            mismatched: dict[str, list[StrategyConfig]] = {}
            deduped_cfgs: list[StrategyConfig] = []
            for c in cfgs:
                t = (c.ticker or "").strip().upper()
                if not t:
                    continue
                existing = by_ticker.get(t)
                if existing is None:
                    by_ticker[t] = c
                    deduped_cfgs.append(c)
                    continue
                dup.setdefault(t, [existing]).append(c)
                if c != existing:
                    mismatched[t] = dup[t]
            if mismatched:
                details = "; ".join(
                    f"{t}=" + ",".join(f"{(x.strategy or 'vwap')}:{(x.direction or '').upper()}" for x in lst)
                    for t, lst in sorted(mismatched.items())
                )
                raise RuntimeError(
                    "Invalid multi config: multiple different strategies for the same ticker in one subaccount. "
                    "This bot supports only ONE strategy per ticker per subaccount. "
                    "Remove duplicates or use separate subaccounts. "
                    f"Duplicates: {details}"
                )
            if dup:
                details = "; ".join(
                    f"{t}=" + ",".join(f"{(x.strategy or 'vwap')}:{(x.direction or '').upper()}" for x in lst)
                    for t, lst in sorted(dup.items())
                )
                logger.warning(
                    "Duplicate ticker entries in multi config; keeping first entry per ticker. "
                    "Remove duplicates or use separate subaccounts. Duplicates: %s",
                    details,
                )
                cfgs = deduped_cfgs

            # Optional global override for multi-mode.
            # We keep it explicit to avoid unintentionally overwriting per-instrument config.
            if os.getenv("ETHEREAL_ENTRY_QTY") and _truthy_env("ETHEREAL_ENTRY_QTY_ALL"):
                q = _as_decimal(os.getenv("ETHEREAL_ENTRY_QTY"))
                cfgs = [StrategyConfig(**{**c.__dict__, "entry_quantity": q}) for c in cfgs]
            elif os.getenv("ETHEREAL_ENTRY_QTY") and not _truthy_env("ETHEREAL_ENTRY_QTY_ALL"):
                logger.warning(
                    "ETHEREAL_ENTRY_QTY is set but ignored in multi-mode. "
                    "Set ETHEREAL_ENTRY_QTY_ALL=1 to apply it to all instruments."
                )

            sub_idx = int(os.getenv("ETHEREAL_SUBACCOUNT_INDEX", str(sub_idx_from_file)))

            # Shared discovery to reduce API calls for multi-run.
            subs = await _retry_on_connect_error("subaccounts()", lambda: client.subaccounts())
            if not subs:
                raise RuntimeError(
                    "No subaccounts found for this key. On Ethereal, a subaccount is created only after you "
                    "deposit USDe. Deposit (testnet: https://deposit.etherealtest.net, mainnet: https://deposit.ethereal.trade) "
                    "then re-run the bot."
                )
            if sub_idx < 0 or sub_idx >= len(subs):
                raise RuntimeError(f"subaccount_index={sub_idx} is out of range. Found {len(subs)} subaccounts.")
            sub_id = subs[sub_idx].id
            sub_name = subs[sub_idx].name

            products = await _retry_on_connect_error("products_by_ticker()", lambda: client.products_by_ticker())

            strategies: list[EtherealVWAPStrategy] = []
            for i, cfg in enumerate(cfgs):
                s = EtherealVWAPStrategy(client, cfg, subaccount_index=sub_idx)
                try:
                    await _retry_on_connect_error(
                        f"initialize({cfg.ticker})",
                        lambda: s.initialize(subaccount_id=sub_id, subaccount_name=sub_name, products_by_ticker=products),
                    )
                except RuntimeError as e:
                    logger.error("[%s %s] %s", cfg.direction, cfg.ticker, str(e))
                    continue
                strategies.append(s)
                # Small stagger to reduce bursts when many symbols start together.
                await asyncio.sleep(0.15 * i)

            if not strategies:
                raise RuntimeError("All strategies failed to initialize. Check tickers and config.")

            logger.info("Starting %d strategies in parallel.", len(strategies))
            await asyncio.gather(*(s.run() for s in strategies))
            return

        # Single-instrument mode (backward compatible)
        ticker = (os.getenv("ETHEREAL_TICKER") or "SOLUSD").strip().upper()
        direction = (os.getenv("ETHEREAL_DIRECTION") or "LONG").strip().upper()
        config_path = f"strategy_config_{direction}_{ticker}.json"
        cfg, created = _load_or_create_config(config_path)
        if created:
            return

        # allow env overrides (optional)
        if os.getenv("ETHEREAL_ENTRY_QTY"):
            cfg = StrategyConfig(**{**cfg.__dict__, "entry_quantity": _as_decimal(os.getenv("ETHEREAL_ENTRY_QTY"))})

        sub_idx = int(os.getenv("ETHEREAL_SUBACCOUNT_INDEX", "0"))
        strat = EtherealVWAPStrategy(client, cfg, subaccount_index=sub_idx)
        try:
            await _retry_on_connect_error("initialize(single)", lambda: strat.initialize())
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

