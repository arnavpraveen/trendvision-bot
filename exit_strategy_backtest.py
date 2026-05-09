#!/usr/bin/env python3
"""
Exit Strategy Backtester
========================
Pulls 1-minute intraday bars from IBKR for every traded ticker in detailed_trades.csv,
then simulates multiple exit strategies against actual price paths to find the optimal one.

Usage:
    python exit_strategy_backtest.py [--port 4002] [--trades detailed_trades.csv] [--output results/]

Requirements:
    pip install ib_insync pandas numpy
"""

import argparse
import csv
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from ib_insync import IB, Stock, util


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ExitStrategy:
    """Defines a single exit strategy to simulate."""
    name: str
    take_profit_pct: Optional[float] = None   # e.g. 20.0 means +20%
    stop_loss_pct: Optional[float] = None      # e.g. 8.0 means -8%
    trailing_stop_pct: Optional[float] = None  # e.g. 5.0 means 5% from peak
    time_stop_minutes: Optional[float] = None  # exit after N minutes
    eod_close: bool = True                     # force close at 15:45 ET if still open
    short: bool = False                        # True = short sell at entry, buy to cover at exit
    # NEW: time-window stop — SL only active for first N minutes after entry.
    # After window expires, SL is suppressed (trade rides to EOD or other exit).
    # Mirrors the live bot's SHORT_STOP_WINDOW_MIN behaviour.
    stop_window_minutes: Optional[float] = None
    # NEW: breakeven trigger — once price moves favorably by this %, the SL
    # moves to entry_price (locking in breakeven).  Independent of the time
    # window: once breakeven fires it stays armed for the rest of the hold.
    # Tests the "lock in unrealized profits before they reverse" hypothesis.
    breakeven_trigger_pct: Optional[float] = None

    def describe(self) -> str:
        parts = []
        if self.short:
            parts.append("SHORT")
        if self.take_profit_pct is not None:
            parts.append(f"TP +{self.take_profit_pct}%")
        if self.stop_loss_pct is not None:
            sl = f"SL -{self.stop_loss_pct}%"
            if self.stop_window_minutes is not None:
                sl += f"@≤{self.stop_window_minutes:g}min"
            parts.append(sl)
        if self.breakeven_trigger_pct is not None:
            parts.append(f"BE@+{self.breakeven_trigger_pct}%")
        if self.trailing_stop_pct is not None:
            parts.append(f"Trail -{self.trailing_stop_pct}%")
        if self.time_stop_minutes is not None:
            parts.append(f"TimeStop {self.time_stop_minutes}min")
        if self.eod_close:
            parts.append("EOD")
        return " / ".join(parts) if parts else "Hold-to-EOD"


# All strategies to test — add/remove/tweak as needed
STRATEGIES = [
    # Baseline: EOD only (what the bot did before brackets)
    ExitStrategy(name="EOD_ONLY", eod_close=True),

    # Current bracket: TP +20% / SL -8%
    ExitStrategy(name="TP20_SL8", take_profit_pct=20.0, stop_loss_pct=8.0),

    # Tighter TP variants
    ExitStrategy(name="TP10_SL8", take_profit_pct=10.0, stop_loss_pct=8.0),
    ExitStrategy(name="TP12_SL8", take_profit_pct=12.0, stop_loss_pct=8.0),
    ExitStrategy(name="TP15_SL8", take_profit_pct=15.0, stop_loss_pct=8.0),

    # Wider SL variants
    ExitStrategy(name="TP20_SL10", take_profit_pct=20.0, stop_loss_pct=10.0),
    ExitStrategy(name="TP20_SL12", take_profit_pct=20.0, stop_loss_pct=12.0),
    ExitStrategy(name="TP20_SL15", take_profit_pct=20.0, stop_loss_pct=15.0),

    # Balanced combos
    ExitStrategy(name="TP10_SL5", take_profit_pct=10.0, stop_loss_pct=5.0),
    ExitStrategy(name="TP10_SL10", take_profit_pct=10.0, stop_loss_pct=10.0),
    ExitStrategy(name="TP15_SL10", take_profit_pct=15.0, stop_loss_pct=10.0),
    ExitStrategy(name="TP8_SL4", take_profit_pct=8.0, stop_loss_pct=4.0),
    ExitStrategy(name="TP5_SL3", take_profit_pct=5.0, stop_loss_pct=3.0),

    # Trailing stops (no fixed TP — let winners run)
    ExitStrategy(name="TRAIL5", trailing_stop_pct=5.0),
    ExitStrategy(name="TRAIL8", trailing_stop_pct=8.0),
    ExitStrategy(name="TRAIL10", trailing_stop_pct=10.0),

    # Trailing + fixed SL floor
    ExitStrategy(name="TRAIL5_SL8", trailing_stop_pct=5.0, stop_loss_pct=8.0),
    ExitStrategy(name="TRAIL8_SL12", trailing_stop_pct=8.0, stop_loss_pct=12.0),

    # Time-based exits
    ExitStrategy(name="TIME5", time_stop_minutes=5),
    ExitStrategy(name="TIME10", time_stop_minutes=10),
    ExitStrategy(name="TIME15", time_stop_minutes=15),
    ExitStrategy(name="TIME30", time_stop_minutes=30),
    ExitStrategy(name="TIME60", time_stop_minutes=60),

    # Time + bracket combos
    ExitStrategy(name="TP10_SL5_T30", take_profit_pct=10.0, stop_loss_pct=5.0, time_stop_minutes=30),
    ExitStrategy(name="TP15_SL8_T60", take_profit_pct=15.0, stop_loss_pct=8.0, time_stop_minutes=60),

    # ── SHORT STRATEGIES ──
    # Short sell at entry, buy to cover at exit
    # TP = price drops X% below entry (profit), SL = price rises X% above entry (loss)

    # Short baseline: hold to EOD
    ExitStrategy(name="S_EOD_ONLY", eod_close=True, short=True),

    # Short brackets — TP is the fade target, SL caps squeeze risk
    ExitStrategy(name="S_TP5_SL5", take_profit_pct=5.0, stop_loss_pct=5.0, short=True),
    ExitStrategy(name="S_TP5_SL8", take_profit_pct=5.0, stop_loss_pct=8.0, short=True),
    ExitStrategy(name="S_TP8_SL5", take_profit_pct=8.0, stop_loss_pct=5.0, short=True),
    ExitStrategy(name="S_TP8_SL8", take_profit_pct=8.0, stop_loss_pct=8.0, short=True),
    ExitStrategy(name="S_TP10_SL5", take_profit_pct=10.0, stop_loss_pct=5.0, short=True),
    ExitStrategy(name="S_TP10_SL8", take_profit_pct=10.0, stop_loss_pct=8.0, short=True),
    ExitStrategy(name="S_TP10_SL10", take_profit_pct=10.0, stop_loss_pct=10.0, short=True),
    ExitStrategy(name="S_TP15_SL8", take_profit_pct=15.0, stop_loss_pct=8.0, short=True),
    ExitStrategy(name="S_TP15_SL10", take_profit_pct=15.0, stop_loss_pct=10.0, short=True),
    ExitStrategy(name="S_TP20_SL8", take_profit_pct=20.0, stop_loss_pct=8.0, short=True),
    ExitStrategy(name="S_TP20_SL10", take_profit_pct=20.0, stop_loss_pct=10.0, short=True),
    ExitStrategy(name="S_TP3_SL3", take_profit_pct=3.0, stop_loss_pct=3.0, short=True),
    ExitStrategy(name="S_TP5_SL3", take_profit_pct=5.0, stop_loss_pct=3.0, short=True),

    # Short trailing stops (trail up from trough)
    ExitStrategy(name="S_TRAIL5", trailing_stop_pct=5.0, short=True),
    ExitStrategy(name="S_TRAIL8", trailing_stop_pct=8.0, short=True),
    ExitStrategy(name="S_TRAIL10", trailing_stop_pct=10.0, short=True),

    # Short time-based
    ExitStrategy(name="S_TIME5", time_stop_minutes=5, short=True),
    ExitStrategy(name="S_TIME15", time_stop_minutes=15, short=True),
    ExitStrategy(name="S_TIME30", time_stop_minutes=30, short=True),
    ExitStrategy(name="S_TIME60", time_stop_minutes=60, short=True),

    # Short bracket + time
    ExitStrategy(name="S_TP10_SL5_T30", take_profit_pct=10.0, stop_loss_pct=5.0, time_stop_minutes=30, short=True),
    ExitStrategy(name="S_TP10_SL8_T60", take_profit_pct=10.0, stop_loss_pct=8.0, time_stop_minutes=60, short=True),

    # ──────────────────────────────────────────────────────────────────────
    # POST–MAY 7 ANALYSIS: stop-policy comparison for SHORT mode
    # ──────────────────────────────────────────────────────────────────────
    # Question 1: How does stop level alone affect P&L? (no window, hold to EOD)
    ExitStrategy(name="S_SL10",      stop_loss_pct=10.0, short=True),
    ExitStrategy(name="S_SL12",      stop_loss_pct=12.0, short=True),
    ExitStrategy(name="S_SL15",      stop_loss_pct=15.0, short=True),
    ExitStrategy(name="S_SL20",      stop_loss_pct=20.0, short=True),  # pre-window-rule bot
    ExitStrategy(name="S_SL25",      stop_loss_pct=25.0, short=True),
    ExitStrategy(name="S_SL30",      stop_loss_pct=30.0, short=True),

    # Question 2: Does the time-window rule actually help, vs always-on SL?
    # Compare against current bot (S_SL20_W20) and across windows.
    ExitStrategy(name="S_SL20_W5",   stop_loss_pct=20.0, stop_window_minutes=5,  short=True),
    ExitStrategy(name="S_SL20_W10",  stop_loss_pct=20.0, stop_window_minutes=10, short=True),
    ExitStrategy(name="S_SL20_W15",  stop_loss_pct=20.0, stop_window_minutes=15, short=True),
    ExitStrategy(name="S_SL20_W20",  stop_loss_pct=20.0, stop_window_minutes=20, short=True),  # CURRENT BOT
    ExitStrategy(name="S_SL20_W30",  stop_loss_pct=20.0, stop_window_minutes=30, short=True),
    ExitStrategy(name="S_SL20_W60",  stop_loss_pct=20.0, stop_window_minutes=60, short=True),

    # Question 3: Tighter stops with the same window — does combining them help?
    ExitStrategy(name="S_SL12_W20",  stop_loss_pct=12.0, stop_window_minutes=20, short=True),
    ExitStrategy(name="S_SL15_W20",  stop_loss_pct=15.0, stop_window_minutes=20, short=True),
    ExitStrategy(name="S_SL10_W10",  stop_loss_pct=10.0, stop_window_minutes=10, short=True),
    ExitStrategy(name="S_SL15_W10",  stop_loss_pct=15.0, stop_window_minutes=10, short=True),

    # Question 4: Breakeven trigger (the "trail-the-MFE" idea).  Once price
    # has dropped X% from entry (favorable for short), SL moves to entry_price.
    # Pure BE — no fixed disaster stop.
    ExitStrategy(name="S_BE3",       breakeven_trigger_pct=3.0,  short=True),
    ExitStrategy(name="S_BE5",       breakeven_trigger_pct=5.0,  short=True),
    ExitStrategy(name="S_BE8",       breakeven_trigger_pct=8.0,  short=True),
    ExitStrategy(name="S_BE10",      breakeven_trigger_pct=10.0, short=True),

    # Question 5: BE trigger + always-on disaster stop (catches both runaway
    # squeezes AND captures unrealized gains before reversal).
    ExitStrategy(name="S_BE3_SL20",  breakeven_trigger_pct=3.0,  stop_loss_pct=20.0, short=True),
    ExitStrategy(name="S_BE5_SL20",  breakeven_trigger_pct=5.0,  stop_loss_pct=20.0, short=True),
    ExitStrategy(name="S_BE8_SL20",  breakeven_trigger_pct=8.0,  stop_loss_pct=20.0, short=True),
    ExitStrategy(name="S_BE5_SL15",  breakeven_trigger_pct=5.0,  stop_loss_pct=15.0, short=True),

    # Question 6: BE + window-stop combo (most interesting — combines all 3 ideas)
    ExitStrategy(name="S_BE5_SL20_W20", breakeven_trigger_pct=5.0, stop_loss_pct=20.0, stop_window_minutes=20, short=True),
    ExitStrategy(name="S_BE3_SL20_W20", breakeven_trigger_pct=3.0, stop_loss_pct=20.0, stop_window_minutes=20, short=True),
]

EOD_CLOSE_HOUR = 15
EOD_CLOSE_MINUTE = 45


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════════════

def load_trades(path: str) -> list[dict]:
    """Load OPEN entries from detailed_trades.csv."""
    with open(path) as f:
        rows = list(csv.DictReader(f))
    opens = [r for r in rows if r["action"] == "OPEN"]
    print(f"Loaded {len(opens)} OPEN trades from {path}")
    return opens


def group_trades_by_ticker_date(trades: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """Group trades by (ticker, date) so we fetch bars once per ticker-day."""
    groups = defaultdict(list)
    for t in trades:
        date = t["entry_time"][:10]
        groups[(t["ticker"], date)].append(t)
    print(f"Unique (ticker, date) combos: {len(groups)}")
    return groups


# ═══════════════════════════════════════════════════════════════════════════════
# IBKR DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════════════

BARS_CACHE_DIR = "bars_cache"

def cache_path(ticker: str, date: str) -> str:
    return os.path.join(BARS_CACHE_DIR, f"{ticker}_{date}.csv")


def fetch_bars(ib: IB, ticker: str, date: str) -> Optional[pd.DataFrame]:
    """
    Fetch 1-min bars for a ticker on a given date from IBKR.
    Caches to disk so re-runs don't re-fetch.
    Returns DataFrame with columns: date, open, high, low, close, volume
    indexed by datetime, or None on failure.
    """
    cp = cache_path(ticker, date)
    if os.path.exists(cp):
        try:
            df = pd.read_csv(cp, parse_dates=["date"], index_col="date")
        except (ValueError, pd.errors.EmptyDataError):
            df = pd.DataFrame()
        if len(df) > 0:
            return df
        return None

    # Build contract — use SMART exchange, USD
    contract = Stock(ticker, "SMART", "USD")

    try:
        qualified = ib.qualifyContracts(contract)
        if not qualified:
            print(f"  ✗ {ticker}: could not qualify contract")
            # Write empty cache to avoid re-trying
            pd.DataFrame().to_csv(cp)
            return None
    except Exception as e:
        print(f"  ✗ {ticker}: qualify error: {e}")
        pd.DataFrame().to_csv(cp)
        return None

    # Request 1-min bars for the full day
    # endDateTime = end of trading day, duration = 1 day
    end_dt = datetime.strptime(date, "%Y-%m-%d").replace(hour=20, minute=0)
    end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")

    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_str,
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=False,           # include pre-market / after-hours
            formatDate=1,
            timeout=30,
        )
    except Exception as e:
        print(f"  ✗ {ticker}: historical data error: {e}")
        pd.DataFrame().to_csv(cp)
        return None

    if not bars:
        print(f"  ✗ {ticker} {date}: no bars returned")
        pd.DataFrame().to_csv(cp)
        return None

    df = util.df(bars)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df = df[["open", "high", "low", "close", "volume"]]

    # Filter to requested date only
    df = df[df.index.strftime("%Y-%m-%d") == date]

    df.to_csv(cp)
    print(f"  ✓ {ticker} {date}: {len(df)} bars cached")
    return df if len(df) > 0 else None


def fetch_all_bars(ib: IB, groups: dict, rate_limit_sec: float = 0.5) -> dict[tuple[str, str], pd.DataFrame]:
    """Fetch bars for all (ticker, date) groups, with rate limiting."""
    os.makedirs(BARS_CACHE_DIR, exist_ok=True)
    bars_map = {}
    total = len(groups)
    cached = 0
    fetched = 0
    failed = 0

    for i, (ticker, date) in enumerate(sorted(groups.keys())):
        cp = cache_path(ticker, date)
        is_cached = os.path.exists(cp)

        df = fetch_bars(ib, ticker, date)
        if df is not None:
            bars_map[(ticker, date)] = df
            if is_cached:
                cached += 1
            else:
                fetched += 1
        else:
            failed += 1

        # Rate limit only for fresh fetches
        if not is_cached:
            time.sleep(rate_limit_sec)

        if (i + 1) % 25 == 0:
            print(f"  Progress: {i+1}/{total} (cached={cached}, fetched={fetched}, failed={failed})")

    print(f"\nBars fetching complete: {len(bars_map)} succeeded, {failed} failed, {cached} from cache")
    return bars_map


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY SIMULATION
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TradeResult:
    ticker: str
    date: str
    entry_price: float
    exit_price: float
    shares: int
    pnl: float
    pnl_pct: float
    exit_reason: str          # TP, SL, TRAIL, TIME, EOD, NO_BARS
    hold_time_minutes: float
    peak_pnl_pct: float       # max unrealized gain
    trough_pnl_pct: float     # max unrealized loss
    entry_time: str
    exit_time: str
    # Alert metadata for filtering
    mcap: str = ""
    mon_vol: str = ""
    pct_change: str = ""
    rel_vol: str = ""
    zero_borrow: str = ""


def simulate_exit(
    bars: pd.DataFrame,
    entry_time_str: str,
    entry_price: float,
    shares: int,
    strategy: ExitStrategy,
    trade_meta: dict,
) -> TradeResult:
    """
    Simulate a single trade through the bars using the given exit strategy.
    For LONG:  uses HIGH for TP checks and LOW for SL checks (conservative).
    For SHORT: uses LOW for TP checks and HIGH for SL checks (conservative).
    """
    entry_time = pd.Timestamp(entry_time_str)

    # Match timezone awareness of bars index
    if bars.index.tz is not None and entry_time.tz is None:
        entry_time = entry_time.tz_localize(bars.index.tz)

    is_short = strategy.short

    # Find bars at or after entry
    future_bars = bars[bars.index >= entry_time]
    if len(future_bars) == 0:
        # No bars after entry — use entry price as exit (shouldn't happen often)
        return TradeResult(
            ticker=trade_meta["ticker"], date=entry_time_str[:10],
            entry_price=entry_price, exit_price=entry_price, shares=shares,
            pnl=0.0, pnl_pct=0.0, exit_reason="NO_BARS",
            hold_time_minutes=0.0, peak_pnl_pct=0.0, trough_pnl_pct=0.0,
            entry_time=entry_time_str, exit_time=entry_time_str,
            mcap=trade_meta.get("mcap", ""), mon_vol=trade_meta.get("mon_vol", ""),
            pct_change=trade_meta.get("pct_change", ""), rel_vol=trade_meta.get("rel_vol", ""),
            zero_borrow=trade_meta.get("zero_borrow", ""),
        )

    # Compute thresholds
    if is_short:
        # SHORT: TP when price drops, SL when price rises
        tp_price = entry_price * (1 - strategy.take_profit_pct / 100) if strategy.take_profit_pct else None
        sl_price = entry_price * (1 + strategy.stop_loss_pct / 100) if strategy.stop_loss_pct else None
    else:
        # LONG: TP when price rises, SL when price drops
        tp_price = entry_price * (1 + strategy.take_profit_pct / 100) if strategy.take_profit_pct else None
        sl_price = entry_price * (1 - strategy.stop_loss_pct / 100) if strategy.stop_loss_pct else None

    time_stop_ts = entry_time + timedelta(minutes=strategy.time_stop_minutes) if strategy.time_stop_minutes else None
    eod_ts = entry_time.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE, second=0)

    # Trailing stop state
    if is_short:
        # Track trough (lowest price), trail stop rises from trough
        trough_price = entry_price
        trail_stop_price = entry_price * (1 + strategy.trailing_stop_pct / 100) if strategy.trailing_stop_pct else None
    else:
        peak_price = entry_price
        trail_stop_price = entry_price * (1 - strategy.trailing_stop_pct / 100) if strategy.trailing_stop_pct else None

    # Track unrealized extremes
    peak_pnl_pct = 0.0
    trough_pnl_pct = 0.0

    # Breakeven-trigger state.  Armed once price has moved favorably by
    # breakeven_trigger_pct; once armed, the SL is moved to entry_price.
    # Stays armed for the rest of the hold (independent of stop window).
    breakeven_armed = False
    breakeven_trigger_price = None
    if strategy.breakeven_trigger_pct is not None:
        if is_short:
            # Favorable for short = price drops; trigger when bar_low ≤ this
            breakeven_trigger_price = entry_price * (1 - strategy.breakeven_trigger_pct / 100)
        else:
            # Favorable for long = price rises; trigger when bar_high ≥ this
            breakeven_trigger_price = entry_price * (1 + strategy.breakeven_trigger_pct / 100)

    exit_price = None
    exit_reason = None
    exit_time = None

    for ts, bar in future_bars.iterrows():
        bar_high = bar["high"]
        bar_low = bar["low"]
        bar_close = bar["close"]

        # Time-window check for original SL.  None means always-on.
        if strategy.stop_window_minutes is not None:
            elapsed_min = (ts - entry_time).total_seconds() / 60
            sl_in_window = elapsed_min <= strategy.stop_window_minutes
        else:
            sl_in_window = True

        if is_short:
            # Update trailing stop from trough
            if strategy.trailing_stop_pct and bar_low < trough_price:
                trough_price = bar_low
                trail_stop_price = trough_price * (1 + strategy.trailing_stop_pct / 100)

            # Arm breakeven trigger if price has dropped to/below trigger level.
            # We arm during the bar, then check the BE stop using bar_high in
            # the same bar — slightly optimistic on same-bar arm+exit but only
            # matters in the rare case where one bar contains both moves.
            if (breakeven_trigger_price is not None and not breakeven_armed
                    and bar_low <= breakeven_trigger_price):
                breakeven_armed = True

            # Track unrealized P&L (inverted for short)
            high_pnl_pct = (entry_price - bar_low) / entry_price * 100    # best case in bar
            low_pnl_pct = (entry_price - bar_high) / entry_price * 100    # worst case in bar
            peak_pnl_pct = max(peak_pnl_pct, high_pnl_pct)
            trough_pnl_pct = min(trough_pnl_pct, low_pnl_pct)

            # 1) Breakeven stop (once armed) — fires when price returns to entry.
            #    Has priority over the original SL because BE_price < SL_price
            #    for shorts, so BE will be hit first on an upward move.
            if breakeven_armed and bar_high >= entry_price:
                exit_price = entry_price
                exit_reason = "BE"
                exit_time = ts
                break

            # 2) Stop-loss: only active inside the configured window
            if sl_price and sl_in_window and bar_high >= sl_price:
                exit_price = sl_price
                exit_reason = "SL"
                exit_time = ts
                break

            # 3) Trailing stop: price rises above trail
            if trail_stop_price and bar_high >= trail_stop_price:
                exit_price = trail_stop_price
                exit_reason = "TRAIL"
                exit_time = ts
                break

            # 4) Take-profit: price drops below TP (check LOW)
            if tp_price and bar_low <= tp_price:
                exit_price = tp_price
                exit_reason = "TP"
                exit_time = ts
                break

        else:
            # LONG logic — mirror image of SHORT
            if strategy.trailing_stop_pct and bar_high > peak_price:
                peak_price = bar_high
                trail_stop_price = peak_price * (1 - strategy.trailing_stop_pct / 100)

            # Arm breakeven trigger when price has risen to trigger level
            if (breakeven_trigger_price is not None and not breakeven_armed
                    and bar_high >= breakeven_trigger_price):
                breakeven_armed = True

            high_pnl_pct = (bar_high - entry_price) / entry_price * 100
            low_pnl_pct = (bar_low - entry_price) / entry_price * 100
            peak_pnl_pct = max(peak_pnl_pct, high_pnl_pct)
            trough_pnl_pct = min(trough_pnl_pct, low_pnl_pct)

            # 1) Breakeven stop (once armed) — fires when price drops back to entry.
            #    BE_price > SL_price for longs, so BE fires before SL on a drop.
            if breakeven_armed and bar_low <= entry_price:
                exit_price = entry_price
                exit_reason = "BE"
                exit_time = ts
                break

            # 2) Stop-loss: only active inside window
            if sl_price and sl_in_window and bar_low <= sl_price:
                exit_price = sl_price
                exit_reason = "SL"
                exit_time = ts
                break

            # 3) Trailing stop
            if trail_stop_price and bar_low <= trail_stop_price:
                exit_price = trail_stop_price
                exit_reason = "TRAIL"
                exit_time = ts
                break

            # 4) Take-profit (check HIGH)
            if tp_price and bar_high >= tp_price:
                exit_price = tp_price
                exit_reason = "TP"
                exit_time = ts
                break

        # 4) Time stop (same for long and short)
        if time_stop_ts and ts >= time_stop_ts:
            exit_price = bar_close
            exit_reason = "TIME"
            exit_time = ts
            break

        # 5) EOD close
        if strategy.eod_close and ts >= eod_ts:
            exit_price = bar_close
            exit_reason = "EOD"
            exit_time = ts
            break

    # If we never triggered an exit (ran out of bars), close at last bar
    if exit_price is None:
        last_bar = future_bars.iloc[-1]
        exit_price = last_bar["close"]
        exit_reason = "END_OF_DATA"
        exit_time = future_bars.index[-1]

    if is_short:
        pnl = (entry_price - exit_price) * shares
        pnl_pct = (entry_price - exit_price) / entry_price * 100
    else:
        pnl = (exit_price - entry_price) * shares
        pnl_pct = (exit_price - entry_price) / entry_price * 100

    hold_minutes = (exit_time - entry_time).total_seconds() / 60

    return TradeResult(
        ticker=trade_meta["ticker"], date=entry_time_str[:10],
        entry_price=entry_price, exit_price=exit_price, shares=shares,
        pnl=pnl, pnl_pct=pnl_pct, exit_reason=exit_reason,
        hold_time_minutes=hold_minutes,
        peak_pnl_pct=peak_pnl_pct, trough_pnl_pct=trough_pnl_pct,
        entry_time=entry_time_str, exit_time=str(exit_time),
        mcap=trade_meta.get("mcap", ""), mon_vol=trade_meta.get("mon_vol", ""),
        pct_change=trade_meta.get("pct_change", ""), rel_vol=trade_meta.get("rel_vol", ""),
        zero_borrow=trade_meta.get("zero_borrow", ""),
    )


def run_backtest(
    trades: list[dict],
    bars_map: dict[tuple[str, str], pd.DataFrame],
    strategies: list[ExitStrategy],
) -> dict[str, list[TradeResult]]:
    """Run all strategies across all trades. Returns {strategy_name: [TradeResult]}."""
    results = {s.name: [] for s in strategies}
    skipped = 0

    for trade in trades:
        ticker = trade["ticker"]
        date = trade["entry_time"][:10]
        key = (ticker, date)

        if key not in bars_map:
            skipped += 1
            continue

        bars = bars_map[key]
        entry_price = float(trade["entry_price"])
        shares = int(trade["shares"])

        for strat in strategies:
            result = simulate_exit(bars, trade["entry_time"], entry_price, shares, strat, trade)
            results[strat.name].append(result)

    print(f"\nBacktest complete: {len(trades) - skipped} trades simulated, {skipped} skipped (no bars)")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS & REPORTING
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StrategyStats:
    name: str
    description: str
    n_trades: int
    total_pnl: float
    mean_pnl: float
    median_pnl: float
    std_pnl: float
    win_rate: float
    n_wins: int
    avg_win: float
    avg_loss: float
    profit_factor: float      # gross_wins / gross_losses
    reward_risk_ratio: float  # avg_win / avg_loss
    sharpe: float             # mean_pnl / std_pnl (per-trade, not annualized)
    max_drawdown_pct: float   # worst single-trade P&L %
    avg_hold_minutes: float
    # Exit breakdown
    exit_counts: dict = field(default_factory=dict)
    # Breakeven win rate needed
    breakeven_wr: float = 0.0


def compute_stats(name: str, desc: str, results: list[TradeResult]) -> StrategyStats:
    if not results:
        return StrategyStats(name=name, description=desc, n_trades=0,
                             total_pnl=0, mean_pnl=0, median_pnl=0, std_pnl=0,
                             win_rate=0, n_wins=0, avg_win=0, avg_loss=0,
                             profit_factor=0, reward_risk_ratio=0, sharpe=0,
                             max_drawdown_pct=0, avg_hold_minutes=0)

    pnls = [r.pnl for r in results]
    pnl_pcts = [r.pnl_pct for r in results]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    total_pnl = sum(pnls)
    mean_pnl = np.mean(pnls)
    median_pnl = np.median(pnls)
    std_pnl = np.std(pnls, ddof=1) if len(pnls) > 1 else 0

    win_rate = len(wins) / len(pnls) if pnls else 0
    avg_win = np.mean(wins) if wins else 0
    avg_loss = abs(np.mean(losses)) if losses else 0

    gross_wins = sum(wins) if wins else 0
    gross_losses = abs(sum(losses)) if losses else 1
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    reward_risk = avg_win / avg_loss if avg_loss > 0 else float("inf")
    sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0

    # Breakeven WR: at what win rate does E[PnL] = 0?
    # E = WR * avg_win - (1-WR) * avg_loss = 0
    # WR = avg_loss / (avg_win + avg_loss)
    breakeven = avg_loss / (avg_win + avg_loss) if (avg_win + avg_loss) > 0 else 0.5

    exit_counts = defaultdict(int)
    for r in results:
        exit_counts[r.exit_reason] += 1

    return StrategyStats(
        name=name, description=desc, n_trades=len(results),
        total_pnl=total_pnl, mean_pnl=mean_pnl, median_pnl=median_pnl,
        std_pnl=std_pnl, win_rate=win_rate, n_wins=len(wins),
        avg_win=avg_win, avg_loss=avg_loss,
        profit_factor=profit_factor, reward_risk_ratio=reward_risk,
        sharpe=sharpe, max_drawdown_pct=min(pnl_pcts),
        avg_hold_minutes=np.mean([r.hold_time_minutes for r in results]),
        exit_counts=dict(exit_counts), breakeven_wr=breakeven,
    )


def print_report(all_stats: list[StrategyStats]):
    """Print a ranked comparison table."""
    # Sort by total P&L descending
    ranked = sorted(all_stats, key=lambda s: s.total_pnl, reverse=True)

    print("\n" + "=" * 120)
    print("EXIT STRATEGY BACKTEST RESULTS — RANKED BY TOTAL P&L")
    print("=" * 120)
    print(f"{'Rank':<5} {'Strategy':<20} {'P&L':>10} {'Mean':>8} {'Med':>8} "
          f"{'WR':>7} {'#W/#T':>8} {'PF':>6} {'R:R':>6} {'Sharpe':>7} "
          f"{'BE WR':>7} {'AvgHold':>8} {'Worst':>8}")
    print("-" * 120)

    for i, s in enumerate(ranked):
        wr_str = f"{s.win_rate:.1%}"
        wt_str = f"{s.n_wins}/{s.n_trades}"
        pf_str = f"{s.profit_factor:.2f}" if s.profit_factor < 100 else "inf"
        rr_str = f"{s.reward_risk_ratio:.2f}" if s.reward_risk_ratio < 100 else "inf"
        hold_str = f"{s.avg_hold_minutes:.0f}m"

        print(f"{i+1:<5} {s.name:<20} ${s.total_pnl:>9.2f} ${s.mean_pnl:>7.2f} ${s.median_pnl:>7.2f} "
              f"{wr_str:>7} {wt_str:>8} {pf_str:>6} {rr_str:>6} {s.sharpe:>7.3f} "
              f"{s.breakeven_wr:>6.1%} {hold_str:>8} {s.max_drawdown_pct:>7.1f}%")

    print("=" * 120)
    print("\nKey: PF = Profit Factor | R:R = Reward:Risk | BE WR = Breakeven Win Rate")
    print("     Sharpe = per-trade (mean/std), not annualized")

    # Exit breakdown for top 5
    print("\n\nEXIT BREAKDOWN (Top 5 strategies)")
    print("-" * 80)
    for s in ranked[:5]:
        exits = " | ".join(f"{k}: {v}" for k, v in sorted(s.exit_counts.items()))
        print(f"  {s.name}: {exits}")

    # Best strategy detail
    best = ranked[0]
    print(f"\n\n★ BEST STRATEGY: {best.name}")
    print(f"  {best.description}")
    print(f"  Total P&L: ${best.total_pnl:.2f} across {best.n_trades} trades")
    print(f"  Win Rate: {best.win_rate:.1%} (need {best.breakeven_wr:.1%} to break even)")
    print(f"  Avg Win: ${best.avg_win:.2f} | Avg Loss: ${best.avg_loss:.2f}")
    print(f"  Profit Factor: {best.profit_factor:.2f}")
    print(f"  Avg Hold: {best.avg_hold_minutes:.0f} minutes")

    # Long vs Short comparison
    longs = [s for s in ranked if not s.name.startswith("S_")]
    shorts = [s for s in ranked if s.name.startswith("S_")]
    if longs and shorts:
        best_long = longs[0]
        best_short = shorts[0]
        print(f"\n\n{'─' * 60}")
        print(f"LONG vs SHORT")
        print(f"{'─' * 60}")
        print(f"  Best Long:  {best_long.name:<20} P&L: ${best_long.total_pnl:>9.2f}  WR: {best_long.win_rate:.1%}  Sharpe: {best_long.sharpe:.3f}")
        print(f"  Best Short: {best_short.name:<20} P&L: ${best_short.total_pnl:>9.2f}  WR: {best_short.win_rate:.1%}  Sharpe: {best_short.sharpe:.3f}")
        avg_long_pnl = np.mean([s.total_pnl for s in longs])
        avg_short_pnl = np.mean([s.total_pnl for s in shorts])
        print(f"  Avg Long P&L:  ${avg_long_pnl:>9.2f} across {len(longs)} strategies")
        print(f"  Avg Short P&L: ${avg_short_pnl:>9.2f} across {len(shorts)} strategies")


def save_detailed_results(results: dict[str, list[TradeResult]], output_dir: str):
    """Save per-trade results for each strategy to CSV."""
    os.makedirs(output_dir, exist_ok=True)

    for strat_name, trades in results.items():
        path = os.path.join(output_dir, f"{strat_name}_trades.csv")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "ticker", "date", "entry_time", "exit_time", "entry_price", "exit_price",
                "shares", "pnl", "pnl_pct", "exit_reason", "hold_time_min",
                "peak_pnl_pct", "trough_pnl_pct", "mcap", "mon_vol", "pct_change",
            ])
            for r in trades:
                w.writerow([
                    r.ticker, r.date, r.entry_time, r.exit_time,
                    f"{r.entry_price:.4f}", f"{r.exit_price:.4f}",
                    r.shares, f"{r.pnl:.2f}", f"{r.pnl_pct:.2f}",
                    r.exit_reason, f"{r.hold_time_minutes:.1f}",
                    f"{r.peak_pnl_pct:.2f}", f"{r.trough_pnl_pct:.2f}",
                    r.mcap, r.mon_vol, r.pct_change,
                ])

    # Save summary
    summary_path = os.path.join(output_dir, "strategy_summary.csv")
    with open(summary_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "rank", "strategy", "total_pnl", "mean_pnl", "median_pnl", "std_pnl",
            "win_rate", "n_wins", "n_trades", "profit_factor", "reward_risk",
            "sharpe", "breakeven_wr", "avg_hold_min", "worst_trade_pct",
        ])
        all_stats = []
        for strat in STRATEGIES:
            s = compute_stats(strat.name, strat.describe(), results[strat.name])
            all_stats.append(s)

        ranked = sorted(all_stats, key=lambda s: s.total_pnl, reverse=True)
        for i, s in enumerate(ranked):
            w.writerow([
                i + 1, s.name, f"{s.total_pnl:.2f}", f"{s.mean_pnl:.2f}",
                f"{s.median_pnl:.2f}", f"{s.std_pnl:.2f}",
                f"{s.win_rate:.4f}", s.n_wins, s.n_trades,
                f"{s.profit_factor:.4f}", f"{s.reward_risk_ratio:.4f}",
                f"{s.sharpe:.4f}", f"{s.breakeven_wr:.4f}",
                f"{s.avg_hold_minutes:.1f}", f"{s.max_drawdown_pct:.2f}",
            ])

    print(f"\nResults saved to {output_dir}/")
    print(f"  - strategy_summary.csv (ranked comparison)")
    print(f"  - <STRATEGY>_trades.csv (per-trade detail for each strategy)")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Exit Strategy Backtester")
    parser.add_argument("--port", type=int, default=4002, help="IBKR gateway port (default: 4002 paper)")
    parser.add_argument("--trades", type=str, default="detailed_trades.csv", help="Path to detailed_trades.csv")
    parser.add_argument("--output", type=str, default="backtest_results", help="Output directory")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip IBKR fetch, use cached bars only")
    parser.add_argument("--days", type=int, default=None, help="Only backtest the most recent N trading days")
    args = parser.parse_args()

    # Load trades
    trades = load_trades(args.trades)

    # Filter to most recent N days if requested
    if args.days:
        all_dates = sorted(set(t["entry_time"][:10] for t in trades))
        keep_dates = set(all_dates[-args.days:])
        trades = [t for t in trades if t["entry_time"][:10] in keep_dates]
        print(f"--days {args.days}: filtered to {len(trades)} trades on {sorted(keep_dates)}")
    groups = group_trades_by_ticker_date(trades)

    # Fetch bars
    if args.skip_fetch:
        print("\n--skip-fetch: loading from cache only")
        os.makedirs(BARS_CACHE_DIR, exist_ok=True)
        bars_map = {}
        for (ticker, date) in groups:
            cp = cache_path(ticker, date)
            if os.path.exists(cp):
                try:
                    df = pd.read_csv(cp, parse_dates=["date"], index_col="date")
                except (ValueError, pd.errors.EmptyDataError):
                    df = pd.DataFrame()
                if len(df) > 0:
                    bars_map[(ticker, date)] = df
        print(f"Loaded {len(bars_map)} ticker-days from cache")
    else:
        print(f"\nConnecting to IBKR on port {args.port}...")
        ib = IB()
        try:
            ib.connect("127.0.0.1", args.port, clientId=99)
            print("Connected to IBKR")
        except Exception as e:
            print(f"Failed to connect: {e}")
            print("Run with --skip-fetch to use cached bars, or start TWS/Gateway.")
            return

        print(f"\nFetching 1-min bars for {len(groups)} ticker-days...")
        bars_map = fetch_all_bars(ib, groups)
        ib.disconnect()

    if not bars_map:
        print("No bars available. Cannot run backtest.")
        return

    # Run backtest
    print(f"\nRunning {len(STRATEGIES)} strategies across {len(trades)} trades...")
    results = run_backtest(trades, bars_map, STRATEGIES)

    # Compute stats & print report
    all_stats = []
    for strat in STRATEGIES:
        s = compute_stats(strat.name, strat.describe(), results[strat.name])
        all_stats.append(s)

    print_report(all_stats)
    save_detailed_results(results, args.output)


if __name__ == "__main__":
    main()