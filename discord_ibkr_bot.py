#!/usr/bin/env python3
# ── Last change: 2026-04-09 ──────────────────────────────────────────────
# - Bracket orders: after every buy fill, submit take-profit (+20%) and
#   stop-loss (-8%) as OCA-linked orders. IBKR manages them server-side.
#   check_bracket_fills() in the main loop detects fills and logs P&L.
#   EOD close cancels remaining bracket legs before submitting market sell.
# - FIX: reconciliation now only runs 09:30–16:00 ET (was 04:00–16:00).
#   Market sells submitted pre-market go PreSubmitted and wait 5+ hours.
#   During that gap, old sells can settle and the position goes to 0.
#   When the orphan sell finally fills at 09:30, it creates a short.
#   (AIXI -97, SILO -188 on April 9.)
# - Re-implemented strategy matching with 5 data-driven strategies from
#   283-trade backtest (Apr 1–9).  All alert #1s still traded; strategies
#   only add labels for tracking.  Strategies: HIGH_VOLUME_RTH (+2.25%),
#   ZERO_BORROW (+1.14%), MID_PRICE_VOLUME (+0.63%), MID_PRICE_RTH (+0.51%),
#   LIQUID_MOVER (+0.43%).
# ── Previous: 2026-04-08 ────────────────────────────────────────────────
# - FIX: check_eod_close now guards on ib.isConnected() before calling
#   ib.portfolio().  When disconnected, portfolio returns empty/stale data
#   and the bot would falsely conclude all positions were "already sold",
#   removing them from tracking permanently.  (April 7 bug.)
# - EOD status log throttled to every ~10s (was every 1s, spammed hundreds
#   of identical lines while waiting for slow fills).
# ── Previous: 2026-04-02 ────────────────────────────────────────────────
# - EOD close revamp: single fire-and-forget market sell per position at
#   15:45 ET.  No retries, no fallbacks, no cancel-and-resubmit loops.
#   Subsequent loop iterations check fill status and log P&L.
#   Unfilled sells expire at market close; reconciliation handles next day.
# - Reconciliation simplified: same fire-and-forget pattern, no 15s wait.
# - Removed: _eod_sell_attempts, MAX_EOD_SELL_RETRIES, EOD_QUOTE_WAIT_SEC,
#   market-to-limit fallback, Inactive cancel logic, outsideRth on sells.
# - EOD cutoff moved from 15:55 → 15:45 ET to match new sell time.
# ─────────────────────────────────────────────────────────────────────────
"""
Discord Alert Trading Bot
=========================================================
Monitors a Discord channel for TrendVision volume alerts and
automatically trades them through Interactive Brokers.

Design principles:
  - Alerts accepted 4:00–15:45 ET (pre-market + RTH, EOD cutoff)
  - DAY limit buy (ask × ASK_LIMIT_MULTIPLIER, 12s timeout)
  - EOD-only exit: single market sell at 15:45 ET (fire-and-forget)
  - Time sourced from IBKR server clock, not the local machine
  - Position state persisted to JSON for recovery after restarts

Order flow:
  1. Discord alert received → parse ticker + price
  2. RTH check (IBKR server time)
  3. Duplicate check (active_positions + pending_tickers)
  4. DAY limit BUY queued to IBKR thread
  5. On fill → position tracked (no stop placed)
  6. Single market sell at 15:45 ET, tracked until filled
=========================================================
"""

import discord
from ib_insync import *
import pandas as pd
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import re
import os
import threading
import queue
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import json
import signal
import time
import asyncio
import math
import traceback

# ============ CONFIGURATION ============
import env  # loads .env
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")  # personal user token, not a bot token
CHANNEL_ID    = 1467323184905850921

# IBKR connection
IBKR_HOST      = "127.0.0.1"
IBKR_PORT      = 4002
IBKR_CLIENT_ID = 1

# Trading
TRADE_DIRECTION       = 'SHORT'  # 'LONG' or 'SHORT' — flips entry/exit order actions
TRADE_AMOUNT          = 100    # dollars per trade (shares = floor(TRADE_AMOUNT / price))
IOC_LIMIT_PCT         = 3.0    # max % below alert price for limit sell (fallback when no quote)
MIN_PRICE             = 0.01   # alerts below this price are ignored
MAX_PRICE             = 500.0  # alerts above this price are ignored

# Real-time quote settings
QUOTE_WAIT_SEC        = 1.0    # max seconds to wait for streaming bid after reqMktData
QUOTE_POLL_INTERVAL   = 0.1    # seconds between polls while waiting for quote
BID_LIMIT_MULTIPLIER  = 0.99   # SHORT: DAY limit = current bid × this (1% below bid)
ASK_LIMIT_MULTIPLIER  = 1.01   # LONG:  DAY limit = current ask × this (1% above ask)
SELL_LIMIT_TIMEOUT_SEC = 12    # seconds to wait for DAY limit sell (short) to fill
BUY_LIMIT_TIMEOUT_SEC = 12     # seconds to wait for DAY limit buy to fill before cancelling

# Bracket order exit
# LONG: take-profit + stop-loss submitted on every fill (OCA group).
# SHORT: stop-loss only (buy stop above entry to cap squeeze risk).
TAKE_PROFIT_PCT     = 20.0   # LONG: sell limit at entry × (1 + TP/100)
STOP_LOSS_PCT       = 8.0    # LONG: sell stop  at entry × (1 - SL/100)

# SHORT mode exit configuration.
#
# As of 2026-05-09: SHORT_STOP_LOSS_PCT is None (no stop) and trades exit
# either at the configured time-after-entry or at EOD.  Empirically (n=606
# over 8 sessions Apr 27–May 7) the synthetic 20% stop was costing ~$525
# vs holding-and-exiting; the +20-min window rule made it slightly worse
# than always-on.  A 30-min time exit captured 97% of the no-stop edge
# with materially better tail control (worst trade −57% vs −491%).
#
# To revert: set SHORT_STOP_LOSS_PCT back to 20.0 and SHORT_TIME_EXIT_MIN
# to None (and re-set SHORT_STOP_WINDOW_MIN if you want the window rule).
SHORT_STOP_LOSS_PCT = None   # disabled — set to 20.0 to re-enable the synthetic stop

# When the synthetic stop fires (only used if SHORT_STOP_LOSS_PCT is set),
# the BUY LIMIT cover goes out at market × (1 + this/100).
SHORT_STOP_LIMIT_BUFFER_PCT = 5.0

# Time-window for the synthetic stop (only used if SHORT_STOP_LOSS_PCT is set).
# None = always-on; numeric = SL active only for this many minutes after entry.
SHORT_STOP_WINDOW_MIN = None   # disabled along with the stop itself

# Time-based exit (SHORT only).  N minutes after entry, submit a marketable
# BUY LIMIT cover regardless of price.  None disables.  Backtest indicated
# 30 min was the sweet spot — captures most of the strategy edge before
# late-day squeeze risk dominates.
SHORT_TIME_EXIT_MIN = 30.0

# Buffer on the time-exit BUY LIMIT cover (market × (1 + buffer/100)).
# Wider than the stop buffer because price has had time to drift, so we
# care less about precision and more about guaranteed fill.
SHORT_TIME_EXIT_LIMIT_BUFFER_PCT = 2.0

# End-of-day forced close – single market sell submitted at this time (ET).
# tif='DAY' so unfilled sells auto-cancel at market close.
# Reconciliation picks up any survivors the next trading day.
EOD_CLOSE_HOUR   = 15
EOD_CLOSE_MINUTE = 45

# Maximum age (seconds) for a queued order before it's considered stale.
# If the IBKR thread is blocked, orders can sit in the queue for hours.
# This prevents executing old alerts when the thread finally unblocks.
MAX_ORDER_AGE_SEC = 120

# Order statuses that indicate a sell is still in-flight or rejected-but-alive.
# Used by EOD close and reconciliation to avoid stacking duplicate sells → short positions.
# Inactive = IBKR rejected but the order object is still "open" in TWS.
PENDING_SELL_STATUSES = {'Submitted', 'PreSubmitted', 'PendingSubmit', 'PendingCancel', 'Inactive'}

# Commissions (IBKR Fixed pricing: $0.005/share, minimum $1.00 per leg)
COMMISSION_PER_SHARE = 0.005
MIN_COMMISSION       = 1.00

# Logging
LOG_LEVEL         = logging.INFO
LOG_DIR           = "logs"
TRADE_LOG_FILE       = "trade_log.csv"
DETAILED_LOG_FILE    = "detailed_trades.csv"
FAILED_TRADES_FILE   = "failed_trades.csv"
STRATEGY_LOG_FILE    = "strategy_trades.csv"
POSITIONS_STATE_FILE = "positions_state.json"

ET = ZoneInfo("America/New_York")

# ============ IBKR SERVER TIME ============
# All time checks use ib.reqCurrentTime() instead of the local system clock.
# The IBKR thread refreshes the cache every ~30 s. Any thread can call get_et()
# and get an accurate ET timestamp via monotonic offset from the last sync.

_ibkr_time_cache      = None   # (utc_datetime_aware, monotonic_float)
_ibkr_time_cache_lock = threading.Lock()

def _refresh_server_time():
    """Fetch current time from IBKR and cache it. IBKR thread only."""
    global _ibkr_time_cache
    try:
        utc_dt = ib.reqCurrentTime()
        with _ibkr_time_cache_lock:
            _ibkr_time_cache = (utc_dt, time.monotonic())
        logger.debug(f"Server time: {utc_dt.astimezone(ET).strftime('%H:%M:%S ET')}")
    except Exception as e:
        logger.debug(f"Server time refresh failed: {e}")

def get_et():
    """Current time in ET from the IBKR server clock. Thread-safe.
    Falls back to system clock only before the first connection."""
    with _ibkr_time_cache_lock:
        cache = _ibkr_time_cache
    if cache is not None:
        utc_dt, mono_at_fetch = cache
        elapsed = time.monotonic() - mono_at_fetch
        return (utc_dt + timedelta(seconds=elapsed)).astimezone(ET)
    return datetime.now(ET)  # pre-connect fallback only

def is_rth():
    """True during the trading window (4:00–16:00 ET, Mon–Fri).
    Starts at 4:00 ET (IBKR pre-market open). IOC limit orders
    protect against bad fills in thin pre-market liquidity."""
    now_et = get_et()
    open_  = now_et.replace(hour=4,  minute=0,  second=0, microsecond=0)
    close_ = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return now_et.weekday() < 5 and open_ <= now_et <= close_

def is_past_eod_cutoff():
    """True if current IBKR server time is at or past the EOD close cutoff.
    Used to reject new entries that would never be caught by EOD close."""
    now_et = get_et()
    cutoff = now_et.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE,
                            second=0, microsecond=0)
    return now_et >= cutoff


# ============ LOGGING SETUP ============
Path(LOG_DIR).mkdir(exist_ok=True)

# File handler rotates at midnight local time, creating a new file each day.
# Current day's log: logs/bot.log
# Previous days:     logs/bot.log.2026-03-02, logs/bot.log.2026-03-03, etc.
_file_handler = TimedRotatingFileHandler(
    f"{LOG_DIR}/bot.log",
    when='midnight',
    backupCount=30,        # keep 30 days of logs
)
_file_handler.suffix = "%Y-%m-%d"

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        _file_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============ DISCORD CLIENT ============
client = discord.Client()

# ============ IBKR CLIENT ============
# Track IBKR order rejections by orderId so place_order and EOD close
# can detect them and take appropriate action.
#   202 = price cap (limit too aggressive for paper sim)
#   201 = trade restricted (closing-only, compliance, no permission)
_order_rejections = {}  # orderId → {'code': int, 'message': str}

def _on_ib_error(reqId, errorCode, errorString, contract):
    """Capture Error 201 and Warning 202 for order rejection handling."""
    if errorCode in (201, 202):
        _order_rejections[reqId] = {'code': errorCode, 'message': errorString}

ib = IB()
ib.errorEvent += _on_ib_error  # capture Warning 202 (price cap) for retry logic

# ============ SHARED STATE ============
# All reads/writes to active_positions and pending_tickers must hold positions_lock.
positions_lock   = threading.Lock()
active_positions = {}   # ticker → position dict
pending_tickers  = set()  # tickers queued but not yet in active_positions

# stats is written from both the Discord thread and the IBKR thread.
stats_lock = threading.Lock()

ibkr_connected      = False
ibkr_thread_started = False
_disconnected_at    = None         # set by monitor_connection on loss
reconnect_event     = threading.Event()
order_queue         = queue.Queue()

# Bug 5 fix: thread-safe connectivity flag written only by the IBKR thread,
# read by monitor_connection.  Avoids calling ib.isConnected() cross-thread.
_ibkr_connected_flag = threading.Event()

stats = {
    'session_start'   : datetime.now(),
    'alerts_received' : 0,
    'alerts_skipped'  : 0,
    'orders_placed'   : 0,
    'orders_filled'   : 0,
    'orders_failed'   : 0,
    'positions_closed': 0,
    'total_pnl'       : 0.0,
}

# ============ HELPERS ============
def log_separator(char="="):
    logger.info(char * 60)

def fmt(amount):
    return f"${amount:,.4f}"

def pct(value):
    return f"{value:+.2f}%"

def calc_commission(shares, price):
    """IBKR Fixed commission for one order leg: $0.005/sh, $1 min, capped at 1% of trade value."""
    return min(max(MIN_COMMISSION, shares * COMMISSION_PER_SHARE), 0.01 * shares * price)

def round_tick(price, up):
    """Round to IBKR's tick ($0.0001 under $1, $0.01 otherwise), toward the
    aggressive side: up for buys, down for sells.  Plain round(x, 2) wiped out
    the whole limit buffer on sub-$1 stocks (round(0.17 * 1.02, 2) == 0.17)."""
    step = 0.0001 if price < 1 else 0.01
    n = price / step
    n = math.ceil(n - 1e-6) if up else math.floor(n + 1e-6)
    return round(n * step, 4)

def calc_pnl(entry_price, exit_price, shares):
    """Net P&L and % after round-trip commissions."""
    if TRADE_DIRECTION == 'SHORT':
        gross = (entry_price - exit_price) * shares
    else:
        gross = (exit_price - entry_price) * shares
    total_commission = calc_commission(shares, entry_price) + calc_commission(shares, exit_price)
    net              = gross - total_commission
    net_pct          = (net / (entry_price * shares)) * 100
    return net, net_pct, total_commission

def _hold_seconds(entry_time, exit_time):
    """Seconds between entry and exit, handling naive/aware mismatch.
    Old positions restored from JSON may have naive entry_time while
    new code uses timezone-aware get_et()."""
    if entry_time.tzinfo is None and exit_time.tzinfo is not None:
        entry_time = entry_time.astimezone(ET)
    elif entry_time.tzinfo is not None and exit_time.tzinfo is None:
        exit_time = exit_time.astimezone(ET)
    return (exit_time - entry_time).total_seconds()

_csv_lock = threading.Lock()

def _append_csv(filepath, row):
    df = pd.DataFrame([row])
    with _csv_lock:
        if not os.path.exists(filepath):
            df.to_csv(filepath, index=False)
        else:
            df.to_csv(filepath, mode='a', header=False, index=False)

def log_failed_trade(ticker, reason, details, order_id=None,
                     snap_bid=None, snap_ask=None, snap_last=None):
    _append_csv(FAILED_TRADES_FILE, {
        'timestamp'     : get_et().strftime('%Y-%m-%d %H:%M:%S'),
        'ticker'        : ticker,
        'failure_reason': reason,
        'details'       : details,
        'order_id'      : order_id or '',
        'session_id'    : stats['session_start'].strftime('%Y%m%d_%H%M%S'),
        'snap_bid'      : round(snap_bid, 4)  if snap_bid  is not None and not math.isnan(snap_bid)  else '',
        'snap_ask'      : round(snap_ask, 4)  if snap_ask  is not None and not math.isnan(snap_ask)  else '',
        'snap_last'     : round(snap_last, 4) if snap_last is not None and not math.isnan(snap_last) else '',
    })

# ============ STRATEGY MATCHING ============
# Strategies derived from analysis of 283 actual bot trades (Apr 1–9).
# Each strategy identifies a combination of alert characteristics that
# produced a positive expected return on an EOD-hold basis.
# Baseline: 27.6% win rate, -3.61% mean return.
#
# Alerts are STILL TRADED regardless of strategy match.  Strategies only
# add labels so performance can be tracked separately.

def _parse_money_val(s):
    """Convert '276.86K' / '7.39M' / '1.03B' to float."""
    if not s or pd.isna(s) or str(s) in ('', 'N/A'):
        return None
    s = str(s).strip().replace('$', '').replace(',', '')
    try:
        if 'B' in s: return float(s.replace('B', '')) * 1e9
        if 'M' in s: return float(s.replace('M', '')) * 1e6
        if 'K' in s: return float(s.replace('K', '')) * 1e3
        return float(s)
    except (ValueError, TypeError):
        return None

def match_strategies(alert_price, mon_vol='', zero_borrow='', entry_time=None):
    """Return a list of strategy names that this alert matches.

    Strategies (from 283-trade backtest, sorted by expected return):
      HIGH_VOLUME_RTH   +2.25%  WR 41%  – MonVol >= $100K + hour >= 9 ET
      ZERO_BORROW       +1.14%  WR 31%  – Zero borrow flag present
      MID_PRICE_VOLUME  +0.63%  WR 34%  – Price $1–10 + MonVol >= $50K
      MID_PRICE_RTH     +0.51%  WR 42%  – Price >= $3 + hour >= 9 ET
      LIQUID_MOVER      +0.43%  WR 37%  – Price >= $1 + MonVol >= $50K
    """
    strategies = []
    pr = alert_price
    mv = _parse_money_val(mon_vol)
    zb = bool(zero_borrow and str(zero_borrow).strip())

    # Determine ET hour of entry
    hour_et = None
    if entry_time is not None:
        try:
            hour_et = entry_time.astimezone(ET).hour
        except Exception:
            pass

    # S1: HIGH_VOLUME_RTH (+2.25%, WR 41%, n=46)
    if mv is not None and mv >= 100e3 and hour_et is not None and hour_et >= 9:
        strategies.append('HIGH_VOLUME_RTH')

    # S2: ZERO_BORROW (+1.14%, WR 31%, n=13)
    if zb:
        strategies.append('ZERO_BORROW')

    # S3: MID_PRICE_VOLUME (+0.63%, WR 34%, n=59)
    if pr is not None and 1 <= pr <= 10 and mv is not None and mv >= 50e3:
        strategies.append('MID_PRICE_VOLUME')

    # S4: MID_PRICE_RTH (+0.51%, WR 42%, n=43)
    if pr is not None and pr >= 3 and hour_et is not None and hour_et >= 9:
        strategies.append('MID_PRICE_RTH')

    # S5: LIQUID_MOVER (+0.43%, WR 37%, n=83)
    if pr is not None and pr >= 1 and mv is not None and mv >= 50e3:
        strategies.append('LIQUID_MOVER')

    return strategies

def log_strategy_trade(ticker, action, strategies, entry_price=None,
                       exit_price=None, entry_time=None, exit_time=None,
                       alert_price=None, pnl=None, pnl_pct=None,
                       hold_time_sec=None, slippage_pct=None,
                       pct_change='', mcap='', float_shares='',
                       rel_vol='', mon_vol='', zero_borrow='',
                       n_strategies=0):
    """Log a strategy-tagged trade to STRATEGY_LOG_FILE."""
    _append_csv(STRATEGY_LOG_FILE, {
        'timestamp'      : get_et().strftime('%Y-%m-%d %H:%M:%S'),
        'ticker'         : ticker,
        'action'         : action,
        'strategies'     : '|'.join(strategies) if strategies else '',
        'n_strategies'   : n_strategies,
        'entry_price'    : round(entry_price, 4) if entry_price is not None else '',
        'exit_price'     : round(exit_price, 4) if exit_price is not None else '',
        'entry_time'     : entry_time.strftime('%Y-%m-%d %H:%M:%S') if entry_time else '',
        'exit_time'      : exit_time.strftime('%Y-%m-%d %H:%M:%S') if exit_time else '',
        'alert_price'    : round(alert_price, 4) if alert_price is not None else '',
        'pnl'            : round(pnl, 2) if pnl is not None else '',
        'pnl_pct'        : round(pnl_pct, 2) if pnl_pct is not None else '',
        'hold_time_min'  : int(hold_time_sec / 60) if hold_time_sec is not None else '',
        'slippage_pct'   : round(slippage_pct, 2) if slippage_pct is not None else '',
        'pct_change'     : pct_change,
        'mcap'           : mcap,
        'float_shares'   : float_shares,
        'rel_vol'        : rel_vol,
        'mon_vol'        : mon_vol,
        'zero_borrow'    : zero_borrow,
    })

# ============ POSITION PERSISTENCE ============
def save_positions_state():
    """Write open positions to JSON. Caller must hold positions_lock."""
    try:
        state = {'timestamp': datetime.now().isoformat(), 'positions': {}}
        for ticker, pos in active_positions.items():
            state['positions'][ticker] = {
                'ticker'       : ticker,
                'shares'       : pos['shares'],
                'entry_price'  : pos['entry_price'],
                'alert_price'  : pos['alert_price'],
                'entry_time'   : pos['entry_time'].isoformat(),
                'order_id'     : pos.get('order_id'),
                'eod_sell_order_id': pos.get('eod_sell_order_id'),
                'symbol'       : pos['contract'].symbol,
                'strategies'   : pos.get('strategies', []),
                'slippage_pct' : pos.get('slippage_pct'),
                'pct_change'   : pos.get('pct_change', ''),
                'mcap'         : pos.get('mcap', ''),
                'float_shares' : pos.get('float_shares', ''),
                'rel_vol'      : pos.get('rel_vol', ''),
                'tags'         : pos.get('tags', ''),
                'zero_borrow'  : pos.get('zero_borrow', ''),
                'mon_vol'      : pos.get('mon_vol', ''),
                'tp_order_id'  : pos.get('tp_order_id'),
                'sl_order_id'  : pos.get('sl_order_id'),
                'stop_price'   : pos.get('stop_price'),
                'stop_triggered': pos.get('stop_triggered', False),
                'stop_window_expired': pos.get('stop_window_expired', False),
                'time_exit_fired': pos.get('time_exit_fired', False),
            }
        with open(POSITIONS_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving positions state: {e}")

def load_positions_state():
    """Restore positions from disk and verify via IBKR portfolio.
    Must only be called from the IBKR thread."""
    if not os.path.exists(POSITIONS_STATE_FILE):
        logger.info("No saved positions – starting fresh")
        return

    try:
        with open(POSITIONS_STATE_FILE, 'r') as f:
            state = json.load(f)

        if not state.get('positions'):
            logger.info("No active positions in saved state")
            return

        logger.info(f"Found {len(state['positions'])} saved position(s) – restoring...")

        # Get actual IBKR portfolio to verify shares are still held
        portfolio = {p.contract.symbol: p for p in ib.portfolio()}

        with positions_lock:
            for ticker, pos_data in state['positions'].items():
                try:
                    contract = Stock(pos_data['symbol'], 'SMART', 'USD')
                    ib.qualifyContracts(contract)

                    ibkr_pos = portfolio.get(ticker)
                    # LONG positions held positive, SHORT positions held negative.
                    # Previously this check was `ibkr_pos.position > 0` which
                    # silently skipped all shorts on restart, leaving them
                    # unmanaged until reconciliation ran at 09:30 ET.
                    if ibkr_pos is None:
                        position_held = False
                    elif TRADE_DIRECTION == 'SHORT':
                        position_held = ibkr_pos.position < 0
                    else:
                        position_held = ibkr_pos.position > 0

                    if position_held:
                        active_positions[ticker] = {
                            'shares'       : pos_data['shares'],
                            'entry_price'  : pos_data['entry_price'],
                            'alert_price'  : pos_data['alert_price'],
                            'entry_time'   : datetime.fromisoformat(pos_data['entry_time']),
                            'order_id'     : pos_data.get('order_id'),
                            'contract'     : contract,
                            'strategies'   : pos_data.get('strategies', []),
                            'slippage_pct' : pos_data.get('slippage_pct'),
                            'pct_change'   : pos_data.get('pct_change', ''),
                            'mcap'         : pos_data.get('mcap', ''),
                            'float_shares' : pos_data.get('float_shares', ''),
                            'rel_vol'      : pos_data.get('rel_vol', ''),
                            'tags'         : pos_data.get('tags', ''),
                            'zero_borrow'  : pos_data.get('zero_borrow', ''),
                            'mon_vol'      : pos_data.get('mon_vol', ''),
                            # Restore order-tracking fields so a restart mid-session
                            # doesn't lose the synthetic stop or bracket state.
                            'tp_order_id'     : pos_data.get('tp_order_id'),
                            'sl_order_id'     : pos_data.get('sl_order_id'),
                            'eod_sell_order_id': pos_data.get('eod_sell_order_id'),
                            'stop_price'      : pos_data.get('stop_price'),
                            'stop_triggered'  : pos_data.get('stop_triggered', False),
                            'stop_window_expired': pos_data.get('stop_window_expired', False),
                            'time_exit_fired' : pos_data.get('time_exit_fired', False),
                            'direction'       : TRADE_DIRECTION,
                        }
                        logger.info(f"  ✅ {ticker}: {pos_data['shares']} shares @ {fmt(pos_data['entry_price'])}")
                    else:
                        logger.warning(f"  ⚠️  {ticker}: not found in IBKR portfolio – may have been closed manually")

                except Exception as e:
                    logger.error(f"  Error restoring {ticker}: {e}")

        if active_positions:
            logger.info(f"Restored {len(active_positions)} position(s)")

    except Exception as e:
        logger.error(f"Error loading positions state: {e}")

def cleanup_positions_state():
    """Delete state file when there are no open positions on exit."""
    try:
        if os.path.exists(POSITIONS_STATE_FILE):
            os.remove(POSITIONS_STATE_FILE)
            logger.info("Positions state file removed")
    except Exception as e:
        logger.error(f"Error removing state file: {e}")

# ============ PORTFOLIO RECONCILIATION ============
_last_reconcile = None

def reconcile_portfolio():
    """Check IBKR portfolio for orphaned positions and sell them.

    An orphaned position is one that exists in the IBKR account but is NOT
    tracked in active_positions. This happens when:
      - EOD sell didn't fill before market close and IBKR cancelled it
      - The bot restarts and doesn't restore the position
      - Manual intervention leaves a position behind

    Fire-and-forget: submits ONE market sell per orphan with tif='DAY'.
    Does NOT wait for fill — the order either fills on its own or the
    next reconciliation pass picks it up.  Never submits a sell if one
    is already in flight for that ticker.

    Runs on startup and every ~5 minutes during regular trading hours.
    Only submits sells after 09:30 ET so market orders fill immediately
    instead of sitting PreSubmitted for hours (which can create shorts
    if the position settles/changes before the sell executes).
    Must only be called from the IBKR thread."""
    global _last_reconcile

    if not ib.isConnected():
        return

    # Only reconcile during regular trading hours (09:30–16:00 ET).
    # Pre-market (04:00–09:30) is excluded because market sells go
    # PreSubmitted and don't execute until 09:30.  During that wait,
    # stale positions can settle to 0 → sell creates a short.
    now_et = get_et()
    if now_et.weekday() >= 5:
        return
    rth_open  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    rth_close = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    if not (rth_open <= now_et <= rth_close):
        return

    # Throttle: run at most every 5 minutes
    now = datetime.now()
    if _last_reconcile is not None and (now - _last_reconcile).total_seconds() < 300:
        return
    _last_reconcile = now

    try:
        portfolio = ib.portfolio()
    except Exception as e:
        logger.debug(f"Portfolio fetch failed during reconciliation: {e}")
        return

    # Tickers to never auto-sell (e.g. legacy positions from prior bugs)
    RECONCILE_IGNORE = {'APBO'}

    with positions_lock:
        tracked = set(active_positions.keys())
        pending = set(pending_tickers)

    orphans = []
    for pos in portfolio:
        if TRADE_DIRECTION == 'SHORT':
            if pos.position >= 0:
                continue  # skip long positions and zero positions in short mode
        else:
            if pos.position <= 0:
                continue  # skip short positions and zero positions in long mode
        ticker = pos.contract.symbol
        if ticker in RECONCILE_IGNORE:
            continue
        if ticker not in tracked and ticker not in pending:
            orphans.append(pos)

    if not orphans:
        return

    log_separator("=")
    logger.warning(
        f"🧹 RECONCILIATION: found {len(orphans)} orphaned position(s)"
    )
    log_separator("=")

    close_action = 'BUY' if TRADE_DIRECTION == 'SHORT' else 'SELL'

    for pos in orphans:
        ticker = pos.contract.symbol
        shares = abs(int(pos.position))
        try:
            # ── Check for already-pending close orders ───────────────────
            existing_closes = [
                t for t in ib.openTrades()
                if t.contract.symbol == ticker
                and t.order.action == close_action
                and t.orderStatus.status in PENDING_SELL_STATUSES
            ]
            if existing_closes:
                logger.info(
                    f"   ⏳ {ticker}: {close_action} already pending "
                    f"({existing_closes[0].orderStatus.status}) "
                    f"– skipping, will check next reconciliation"
                )
                continue

            contract = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(contract)

            close_order     = MarketOrder(close_action, shares)
            close_order.tif = 'DAY'
            close_trade     = ib.placeOrder(contract, close_order)
            logger.info(
                f"   📤 {ticker}: MKT {close_action} {shares} orphaned shares "
                f"(ID: {close_trade.order.orderId}) – fire and forget"
            )

        except Exception as e:
            logger.error(f"   ❌ {ticker}: orphan sell error: {e}")

# ============ CONNECTION ============
def connect_with_retry(max_retries=10, initial_delay=5):
    """Connect to IBKR Gateway with exponential backoff."""
    global ibkr_connected
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            if ib.isConnected():
                ib.RequestTimeout = 20  # ensure timeout is set even on reconnect
                ibkr_connected = True
                _ibkr_connected_flag.set()
                return True

            logger.info(f"Connection attempt {attempt}/{max_retries}...")
            ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=20)

            if ib.isConnected():
                ib.RequestTimeout = 20  # seconds – prevents zombie hangs
                account = ib.managedAccounts()[0]
                net_liq = next(
                    (float(v.value) for v in ib.accountValues()
                     if v.tag == 'NetLiquidation'),
                    None
                )
                ibkr_connected = True
                _ibkr_connected_flag.set()
                log_separator("=")
                logger.info("✅ CONNECTED TO IBKR GATEWAY")
                logger.info(f"   Account : {account}")
                if net_liq:
                    logger.info(f"   Balance : {fmt(net_liq)}")
                log_separator("=")
                load_positions_state()
                reconcile_portfolio()
                return True

        except Exception as e:
            ibkr_connected = False
            _ibkr_connected_flag.clear()
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                logger.info(f"Retrying in {delay}s...")
                time.sleep(delay)
                delay = min(delay * 1.5, 60)

    logger.error("All connection attempts failed")
    return False

def monitor_connection():
    """Background thread: detects connection loss and signals the IBKR thread.
    Bug 5 fix: reads a thread-safe Event flag instead of calling
    ib.isConnected() directly, avoiding cross-thread ib_insync access."""
    global ibkr_connected, _disconnected_at
    while True:
        time.sleep(10)
        if ibkr_connected and not _ibkr_connected_flag.is_set():
            logger.warning("⚠️  Gateway connection lost")
            ibkr_connected   = False
            _disconnected_at = datetime.now()
            with positions_lock:
                save_positions_state()
            reconnect_event.set()

# ============ BRACKET FILL MONITORING ============
def _log_position_close(ticker, pos, exit_price, sell_qty, action='EOD_CLOSE'):
    """Log a successful position close. Used by both bracket fills and EOD."""
    exit_time  = get_et()
    net_pnl, net_pnl_pct, commission = calc_pnl(
        pos['entry_price'], exit_price, sell_qty)
    hold_sec = _hold_seconds(pos['entry_time'], exit_time)

    with stats_lock:
        stats['total_pnl']        += net_pnl
        stats['positions_closed'] += 1

    logger.info(
        f"   ✅ {ticker} @ {fmt(exit_price)} ({action})  "
        f"P&L: ${net_pnl:+.2f} ({pct(net_pnl_pct)}, net of commissions)"
    )
    close_action = 'BUY' if TRADE_DIRECTION == 'SHORT' else 'SELL'
    log_trade(ticker, close_action, sell_qty, exit_price, exit_time)
    log_detailed_trade(
        ticker=ticker, action=action, shares=sell_qty,
        entry_price=pos['entry_price'], exit_price=exit_price,
        entry_time=pos['entry_time'],   exit_time=exit_time,
        alert_price=pos['alert_price'],
        pnl=net_pnl, pnl_pct=net_pnl_pct, hold_time_sec=hold_sec,
    )
    strats = pos.get('strategies', [])
    if strats:
        log_strategy_trade(
            ticker=ticker, action=action, strategies=strats,
            entry_price=pos['entry_price'], exit_price=exit_price,
            entry_time=pos['entry_time'], exit_time=exit_time,
            alert_price=pos['alert_price'],
            pnl=net_pnl, pnl_pct=net_pnl_pct,
            hold_time_sec=hold_sec,
            slippage_pct=pos.get('slippage_pct'),
            pct_change=pos.get('pct_change', ''),
            mcap=pos.get('mcap', ''),
            float_shares=pos.get('float_shares', ''),
            rel_vol=pos.get('rel_vol', ''),
            mon_vol=pos.get('mon_vol', ''),
            zero_borrow=pos.get('zero_borrow', ''),
            n_strategies=len(strats),
        )

def check_bracket_fills():
    """Check if any bracket legs (take-profit or stop-loss) have filled.
    LONG: checks both TP and SL. SHORT: checks SL only (buy stop).
    If filled, log P&L and remove the position from tracking.
    Must only be called from the IBKR thread."""
    if not ib.isConnected():
        return

    with positions_lock:
        snapshot = [(t, p) for t, p in active_positions.items()
                    if p.get('tp_order_id') or p.get('sl_order_id')]

    if not snapshot:
        return

    for ticker, pos in snapshot:
        tp_id = pos.get('tp_order_id')
        sl_id = pos.get('sl_order_id')

        try:
            for t in ib.trades():
                oid = t.order.orderId
                if oid not in (tp_id, sl_id):
                    continue
                if t.orderStatus.status != 'Filled':
                    continue

                # A bracket leg filled
                exit_price = t.orderStatus.avgFillPrice
                action = 'BRACKET_TP' if oid == tp_id else 'BRACKET_SL'
                sell_qty = int(t.orderStatus.filled)

                _log_position_close(ticker, pos, exit_price, sell_qty, action)

                # OCA should cancel the other leg, but clean up tracking
                with positions_lock:
                    active_positions.pop(ticker, None)
                    pending_tickers.discard(ticker)
                break  # done with this ticker

        except Exception as e:
            logger.debug(f"Bracket check error for {ticker}: {e}")

# ============ SYNTHETIC STOP (SHORT) ============
def check_synthetic_stops():
    """Monitor open SHORT positions; fire a marketable BUY LIMIT cover when
    the position's market price crosses its stored stop_price trigger.

    Why synthetic instead of a real StopOrder:
      IBKR stop orders only *trigger* during regular trading hours
      (09:30–16:00 ET), regardless of the outsideRth=True flag.  On 2026-04-17
      EFOI entered short at $2.66 with a $3.19 hardware stop; the stock ran
      to $5.40 during the final 30 min of pre-market while the stop sat
      dormant, then the cover filled at $5.41 at 09:30 RTH open — a −105%
      loss on a 20% stop.  A plain LimitOrder with outsideRth=True DOES
      execute outside RTH, so we poll price ourselves and submit a limit
      cover the instant the trigger is crossed.

    Time-window rule (SHORT_STOP_WINDOW_MIN):
      The stop is only enforced during the first SHORT_STOP_WINDOW_MIN
      minutes after entry.  After that, stop_window_expired flips True and
      the position rides to EOD regardless of price.  This is empirically
      derived: fast-gap squeezes (the EFOI/SCNI/NTIP class) trigger within
      seconds-to-minutes of entry, but stops that fire 30+ min after entry
      tend to be slow grinds that mean-revert by EOD — holding through is
      better.  Set SHORT_STOP_WINDOW_MIN=None to disable and revert to the
      previous always-on behavior.

    Design:
      - Uses ib.portfolio() marketPrice for trigger detection.  No per-ticker
        snapshot calls; portfolio data updates continuously via ib_insync.
      - On trigger, submits BUY LIMIT at marketPrice × (1 + buffer/100),
        outsideRth=True, so it fills immediately during any session while
        capping slippage on fast moves.
      - Sets stop_triggered=True and stores sl_order_id; check_bracket_fills()
        picks up the fill on the next loop and logs BRACKET_SL via the
        normal path.
      - Fire-and-forget: no cancel, no retry.  If the limit doesn't fill
        (price spiked past buffer), EOD close mops it up.
    Must only be called from the IBKR thread."""
    if not ib.isConnected():
        return
    if TRADE_DIRECTION != 'SHORT':
        return

    now = get_et()

    with positions_lock:
        # Phase 1: identify positions whose stop window has expired and mark
        # them so the next sweep skips them entirely.  We log once per
        # position (gated by stop_window_expired) so the audit trail is clean.
        if SHORT_STOP_WINDOW_MIN is not None:
            for ticker, p in active_positions.items():
                if (p.get('direction') != 'SHORT'
                        or p.get('stop_price') is None
                        or p.get('stop_triggered')
                        or p.get('stop_window_expired')):
                    continue
                entry_time = p.get('entry_time')
                if entry_time is None:
                    continue
                age_min = _hold_seconds(entry_time, now) / 60
                if age_min > SHORT_STOP_WINDOW_MIN:
                    p['stop_window_expired'] = True
                    logger.info(
                        f"   ⏰ {ticker}: synthetic stop window expired "
                        f"({age_min:.1f}min > {SHORT_STOP_WINDOW_MIN:g}min) "
                        f"— holding to EOD"
                    )

        # Phase 2: collect candidates that are still inside the window.
        candidates = [
            (t, p) for t, p in active_positions.items()
            if p.get('direction') == 'SHORT'
               and p.get('stop_price') is not None
               and not p.get('stop_triggered')
               and not p.get('stop_window_expired')
        ]

    if not candidates:
        return

    # Fetch portfolio once — all positions with recent marketPrice in one call.
    try:
        portfolio = {p.contract.symbol: p for p in ib.portfolio()}
    except Exception as e:
        logger.debug(f"Synthetic stop: portfolio fetch failed: {e}")
        return

    for ticker, pos in candidates:
        ibkr_pos = portfolio.get(ticker)
        if ibkr_pos is None:
            continue
        market_price = ibkr_pos.marketPrice
        if market_price is None or math.isnan(market_price) or market_price <= 0:
            continue

        stop_price = pos['stop_price']
        if market_price < stop_price:
            continue

        # ── Trigger! Submit marketable BUY LIMIT cover. ─────────────────────
        limit_price = round_tick(market_price * (1 + SHORT_STOP_LIMIT_BUFFER_PCT / 100), up=True)
        try:
            cover_order            = LimitOrder('BUY', pos['shares'], limit_price)
            cover_order.tif        = 'DAY'
            cover_order.outsideRth = True  # critical: this IS honored for limits
            cover_trade            = ib.placeOrder(pos['contract'], cover_order)
            cover_order_id         = cover_trade.order.orderId

            logger.warning(
                f"🛡️  {ticker}: synthetic stop TRIGGERED  "
                f"(market {fmt(market_price)} ≥ stop {fmt(stop_price)})  "
                f"→ BUY LIMIT {pos['shares']} @ {fmt(limit_price)} "
                f"(ID: {cover_order_id})"
            )

            # Mark triggered BEFORE saving so we don't double-fire on the next
            # loop pass if save_positions_state() is slow.
            with positions_lock:
                if ticker in active_positions:
                    active_positions[ticker]['stop_triggered'] = True
                    active_positions[ticker]['sl_order_id']    = cover_order_id
                    save_positions_state()
        except Exception as e:
            logger.error(f"   ❌ {ticker}: synthetic stop cover failed: {e}")

# ============ TIME-BASED EXIT (SHORT) ============
def check_time_exits():
    """Submit a marketable BUY LIMIT cover for any open SHORT position whose
    age exceeds SHORT_TIME_EXIT_MIN.  Fire-and-forget, gated by
    time_exit_fired so each position only fires once.

    Why this exists:
      Empirically (n=606 over 8 sessions Apr 27–May 7) holding SHORTs to
      EOD captured the most absolute profit but exposed us to late-day
      squeeze disasters (e.g. SKK 5x'd between minute 60 and EOD on May 4).
      Cutting positions at minute 30 sacrifices very little of the strategy
      edge (~$32 of $1400 = 2%) while reducing worst-case single-trade move
      from −491% to −57%.  Sharpe ~3x better.

    Design:
      - Reads ib.portfolio() marketPrice for the ask side; submits a BUY
        LIMIT at marketPrice × (1 + SHORT_TIME_EXIT_LIMIT_BUFFER_PCT/100).
      - 2% default buffer: wider than the synthetic-stop buffer because by
        minute 30 the position is no longer in fast-tape territory; we want
        a guaranteed fill, not a precise level.
      - Fires only once per position; sl_order_id is reused for tracking
        so the existing check_bracket_fills path picks up the cover and
        logs it as BRACKET_SL (the action label is "BRACKET_SL" but this
        is mechanically a time-driven exit; downstream analysis can
        distinguish via the stop_percent field which now reads "TIME:30min").
    Must only be called from the IBKR thread."""
    if not ib.isConnected():
        return
    if TRADE_DIRECTION != 'SHORT':
        return
    if SHORT_TIME_EXIT_MIN is None:
        return

    now = get_et()

    with positions_lock:
        candidates = [
            (t, p) for t, p in active_positions.items()
            if p.get('direction') == 'SHORT'
               and not p.get('time_exit_fired')
               and not p.get('stop_triggered')   # don't double-fire if SL also fired
               and p.get('entry_time') is not None
               and _hold_seconds(p['entry_time'], now) / 60 >= SHORT_TIME_EXIT_MIN
        ]

    if not candidates:
        return

    # Fetch portfolio once for ask-side marketPrice
    try:
        portfolio = {p.contract.symbol: p for p in ib.portfolio()}
    except Exception as e:
        logger.debug(f"Time exit: portfolio fetch failed: {e}")
        return

    for ticker, pos in candidates:
        ibkr_pos = portfolio.get(ticker)
        if ibkr_pos is None:
            continue
        market_price = ibkr_pos.marketPrice
        if market_price is None or math.isnan(market_price) or market_price <= 0:
            # Fall back to entry price if portfolio data is stale; the cover
            # at entry × buffer is still better than letting the position drift.
            market_price = pos['entry_price']

        limit_price = round_tick(market_price * (1 + SHORT_TIME_EXIT_LIMIT_BUFFER_PCT / 100), up=True)
        age_min = _hold_seconds(pos['entry_time'], now) / 60
        try:
            cover_order            = LimitOrder('BUY', pos['shares'], limit_price)
            cover_order.tif        = 'DAY'
            cover_order.outsideRth = True
            cover_trade            = ib.placeOrder(pos['contract'], cover_order)
            cover_order_id         = cover_trade.order.orderId

            logger.warning(
                f"⏱️  {ticker}: time exit fired  "
                f"(age {age_min:.1f}min ≥ {SHORT_TIME_EXIT_MIN:g}min)  "
                f"→ BUY LIMIT {pos['shares']} @ {fmt(limit_price)} "
                f"(market {fmt(market_price)}, ID: {cover_order_id})"
            )

            with positions_lock:
                if ticker in active_positions:
                    active_positions[ticker]['time_exit_fired'] = True
                    active_positions[ticker]['sl_order_id']     = cover_order_id
                    save_positions_state()
        except Exception as e:
            logger.error(f"   ❌ {ticker}: time exit cover failed: {e}")

# ============ END-OF-DAY CLOSE ============
_eod_closed_today = None   # date when EOD sells were submitted (prevents re-submission)
_eod_last_log_time = 0.0   # monotonic timestamp of last EOD status log (throttle to ~10s)

def check_eod_close():
    """Fire-and-forget EOD close at EOD_CLOSE_HOUR:EOD_CLOSE_MINUTE ET.

    Design:
      - ONE market sell per position, tif='DAY' (no outsideRth).
      - Submit once, never cancel, never retry, never fallback.
      - On subsequent calls (every ~1s), check if submitted sells filled
        and log P&L.  Positions whose sell didn't fill by market close
        auto-cancel (tif='DAY') and reconciliation handles them next day.
      - Duplicate sells are impossible: we tag each position with
        eod_sell_order_id and never submit a second.

    Must only be called from the IBKR thread."""
    global _eod_closed_today

    now_et = get_et()
    today  = now_et.date()

    if now_et.weekday() >= 5:
        return

    eod = now_et.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE,
                          second=0, microsecond=0)
    if now_et < eod:
        return

    # ── Drain the order queue on first pass ───────────────────────────────
    if _eod_closed_today != today:
        drained = 0
        while True:
            try:
                stale = order_queue.get_nowait()
                stale_ticker = stale.get('ticker', '?')
                logger.warning(f"⚠️  Draining post-EOD queued order: {stale_ticker}")
                log_failed_trade(stale_ticker, 'PAST_EOD_CUTOFF',
                                 'Drained from queue at EOD close')
                with positions_lock:
                    pending_tickers.discard(stale_ticker)
                drained += 1
            except queue.Empty:
                break
        if drained:
            logger.info(f"   Drained {drained} queued order(s)")

    with positions_lock:
        snapshot = list(active_positions.items())

    if not snapshot:
        _eod_closed_today = today
        return

    # Only log the banner once (first pass)
    if _eod_closed_today != today:
        log_separator("=")
        logger.warning(
            f"🔔 EOD CLOSE @ {now_et.strftime('%H:%M ET')} – "
            f"processing {len(snapshot)} position(s)"
        )
        log_separator("=")

    # ── Connection guard ─────────────────────────────────────────────────
    # When disconnected, ib.portfolio() returns stale/empty cached data.
    # If we trust that, we'd conclude all positions are "already sold" and
    # remove them from tracking — permanently losing them.  (April 7 bug.)
    if not ib.isConnected():
        return

    # Fetch actual IBKR portfolio ONCE
    try:
        ibkr_portfolio = {p.contract.symbol: int(p.position)
                          for p in ib.portfolio()}
    except Exception as e:
        logger.error(f"   ❌ Could not fetch portfolio for EOD close: {e}")
        return  # retry next loop iteration

    submitted_new   = 0
    filled_now      = 0
    still_working   = 0

    for ticker, pos in snapshot:
        try:
            actual_shares = ibkr_portfolio.get(ticker, 0)

            # ── Position already gone ─────────────────────────────────────
            # LONG: position is 0 or negative means gone
            # SHORT: position is 0 or positive means gone (covered)
            position_gone = (actual_shares >= 0) if TRADE_DIRECTION == 'SHORT' else (actual_shares <= 0)

            if position_gone:
                # Try to find fill price from a completed cover/sell
                exit_price = None
                cover_action = 'BUY' if TRADE_DIRECTION == 'SHORT' else 'SELL'
                try:
                    completed = [
                        t for t in ib.trades()
                        if t.contract.symbol == ticker
                        and t.order.action == cover_action
                        and t.orderStatus.status == 'Filled'
                    ]
                    if completed:
                        exit_price = completed[-1].orderStatus.avgFillPrice
                except Exception:
                    pass

                if exit_price and exit_price > 0:
                    _log_position_close(ticker, pos, exit_price, pos['shares'])
                    filled_now += 1
                else:
                    logger.warning(
                        f"   ⚠️  {ticker}: no IBKR position – already closed, "
                        f"removing from tracking"
                    )
                with positions_lock:
                    active_positions.pop(ticker, None)
                    pending_tickers.discard(ticker)
                continue

            sell_qty = abs(actual_shares)

            # ── Already submitted an EOD sell for this position? ──────────
            eod_order_id = pos.get('eod_sell_order_id')
            if eod_order_id is not None:
                # Check the status of the existing sell
                eod_trade = None
                try:
                    for t in ib.trades():
                        if t.order.orderId == eod_order_id:
                            eod_trade = t
                            break
                except Exception:
                    pass

                if eod_trade and eod_trade.orderStatus.status == 'Filled':
                    exit_price = eod_trade.orderStatus.avgFillPrice
                    _log_position_close(ticker, pos, exit_price, sell_qty)
                    filled_now += 1
                    with positions_lock:
                        active_positions.pop(ticker, None)
                        pending_tickers.discard(ticker)
                elif eod_trade and eod_trade.orderStatus.status in (
                        'Submitted', 'PreSubmitted', 'PendingSubmit'):
                    still_working += 1
                else:
                    # Cancelled/Inactive/unknown — remove from tracking so
                    # reconciliation picks it up (next pass or next trading day).
                    status = (eod_trade.orderStatus.status
                              if eod_trade else 'not found')
                    logger.warning(
                        f"   ⚠️  {ticker}: EOD sell {status} "
                        f"(ID: {eod_order_id}) – removing from tracking, "
                        f"reconciliation will handle"
                    )
                    with positions_lock:
                        active_positions.pop(ticker, None)
                        pending_tickers.discard(ticker)
                continue

            # ── Cancel bracket/stop orders before EOD close ─────────────────
            # Must cancel outstanding orders before submitting EOD market order
            # to avoid conflicting orders.
            for bracket_id in (pos.get('tp_order_id'), pos.get('sl_order_id')):
                if bracket_id is None:
                    continue
                try:
                    for t in ib.openTrades():
                        if t.order.orderId == bracket_id:
                            ib.cancelOrder(t.order)
                            logger.info(
                                f"   🗑️  {ticker}: cancelled stop/bracket "
                                f"(ID: {bracket_id})"
                            )
                            break
                except Exception:
                    pass
            # Brief pause for cancellations to process
            if pos.get('tp_order_id') or pos.get('sl_order_id'):
                ib.sleep(0.5)

            # ── Check for any existing close order from another source ────
            close_action = 'BUY' if TRADE_DIRECTION == 'SHORT' else 'SELL'
            existing_closes = [
                t for t in ib.openTrades()
                if t.contract.symbol == ticker
                and t.order.action == close_action
                and t.orderStatus.status in PENDING_SELL_STATUSES
            ]
            if existing_closes:
                logger.info(
                    f"   ⏳ {ticker}: {close_action} already in flight – skipping"
                )
                still_working += 1
                continue

            # ── Submit ONE market order to close — fire and forget ────────
            close_order     = MarketOrder(close_action, sell_qty)
            close_order.tif = 'DAY'
            close_trade     = ib.placeOrder(pos['contract'], close_order)
            order_id        = close_trade.order.orderId

            # Tag the position so we never submit a second sell
            with positions_lock:
                if ticker in active_positions:
                    active_positions[ticker]['eod_sell_order_id'] = order_id

            logger.info(
                f"   📤 {ticker}: MKT {close_action} {sell_qty} shares "
                f"(ID: {order_id})"
            )
            submitted_new += 1

        except Exception as e:
            logger.error(f"   ❌ EOD close error for {ticker}: {e}")

    # ── Persist state and log summary ─────────────────────────────────────
    with positions_lock:
        save_positions_state()
        remaining = len(active_positions)

    # Throttle status log to every ~10s (or always log when something changed)
    global _eod_last_log_time
    now_mono = time.monotonic()
    something_changed = submitted_new > 0 or filled_now > 0 or remaining == 0
    if something_changed or (now_mono - _eod_last_log_time) >= 10:
        _eod_last_log_time = now_mono
        log_separator()
        logger.info(
            f"📊 EOD: {submitted_new} new sells, {filled_now} filled, "
            f"{still_working} working, {remaining} remaining  |  "
            f"Session P&L: ${stats['total_pnl']:+.2f}"
        )
        log_separator()

    # Mark sells as submitted — prevents re-submission on subsequent passes.
    # The function still runs each loop to check fill status above.
    _eod_closed_today = today

# ============ ORDER PLACEMENT ============
def place_order(ticker, alert_price, alert_time, tags='', alert_reason='',
                sec_filing='', news='', recent_ipo='', recent_rs='', zero_borrow='',
                pct_change='', mcap='', float_shares='', min_vol='', rel_vol='', mon_vol=''):
    """Execute a DAY limit order. SHORT: sell at bid, cover at EOD.
    LONG: buy at ask, exit via brackets + EOD. IBKR thread only."""
    is_short = (TRADE_DIRECTION == 'SHORT')
    if not ibkr_connected:
        log_failed_trade(ticker, 'NO_CONNECTION', 'Gateway not connected')
        with positions_lock:
            pending_tickers.discard(ticker)
        return

    # Final EOD cutoff check – catches the case where the order was queued
    # before cutoff but place_order runs after (e.g. slow queue processing).
    if is_past_eod_cutoff():
        logger.info(f"⏰ {ticker}: past EOD cutoff at execution – skipped")
        log_failed_trade(ticker, 'PAST_EOD_CUTOFF',
                         'Past EOD cutoff when place_order ran')
        with positions_lock:
            pending_tickers.discard(ticker)
        return

    try:
        contract = Stock(ticker, 'SMART', 'USD')

        # Wrap qualifyContracts in a timeout so a zombie connection can't
        # block the IBKR thread for hours.
        qualify_start = time.monotonic()
        try:
            qualified = ib.qualifyContracts(contract)
        except (ConnectionError, OSError, asyncio.CancelledError,
                asyncio.TimeoutError) as e:
            logger.error(f"   ❌ {ticker}: qualifyContracts failed: {e}")
            log_failed_trade(ticker, 'QUALIFY_ERROR', str(e))
            with positions_lock:
                pending_tickers.discard(ticker)
            return
        qualify_elapsed = time.monotonic() - qualify_start
        if qualify_elapsed > 10:
            logger.warning(f"   ⚠️  qualifyContracts took {qualify_elapsed:.1f}s for {ticker}")

        if not qualified:
            log_failed_trade(ticker, 'INVALID_TICKER', 'Could not qualify contract')
            with positions_lock:
                pending_tickers.discard(ticker)
            return

        # ── Request streaming quote ────────────────────────────────────────
        # SHORT uses bid for pricing; LONG uses ask.
        snap_bid = snap_ask = snap_last = float('nan')
        ticker_data = ib.reqMktData(contract, '', snapshot=False, regulatorySnapshot=False)

        try:
            # Poll for quote to populate
            quote_deadline = time.monotonic() + QUOTE_WAIT_SEC
            while math.isnan(ticker_data.bid if is_short else ticker_data.ask) and time.monotonic() < quote_deadline:
                ib.sleep(QUOTE_POLL_INTERVAL)

            snap_bid  = ticker_data.bid
            snap_ask  = ticker_data.ask
            snap_last = ticker_data.last

            if is_short:
                # SHORT: limit sell at bid × BID_LIMIT_MULTIPLIER (slightly below bid)
                usable_bid = (
                    not math.isnan(snap_bid)
                    and snap_bid > 0
                    and snap_bid < alert_price * 10
                )
                if usable_bid:
                    price_for_sizing = snap_bid
                    limit_price = round_tick(snap_bid * BID_LIMIT_MULTIPLIER, up=False)
                    price_source = f"bid: {fmt(snap_bid)}"
                else:
                    price_for_sizing = alert_price
                    limit_price = round_tick(alert_price * (1 - IOC_LIMIT_PCT / 100), up=False)
                    price_source = "bid: N/A"
            else:
                # LONG: limit buy at ask × ASK_LIMIT_MULTIPLIER
                usable_ask = (
                    not math.isnan(snap_ask)
                    and snap_ask > 0
                    and snap_ask < alert_price * 10
                )
                if usable_ask:
                    price_for_sizing = snap_ask
                    limit_price = round_tick(snap_ask * ASK_LIMIT_MULTIPLIER, up=True)
                    price_source = f"ask: {fmt(snap_ask)}"
                else:
                    price_for_sizing = alert_price
                    limit_price = round_tick(alert_price * (1 + IOC_LIMIT_PCT / 100), up=True)
                    price_source = "ask: N/A"

            shares = max(1, int(TRADE_AMOUNT / price_for_sizing))

            entry_action = 'SHORT' if is_short else 'BUY'
            entry_emoji = '📉' if is_short else '📈'
            log_separator("-")
            logger.info(f"{entry_emoji} {entry_action}: {shares} × {ticker}  @ DAY limit {fmt(limit_price)}  ({price_source}, alert: {fmt(alert_price)}"
                        + (f"  {pct_change}" if pct_change else '') + ")")
            if not math.isnan(snap_bid) and snap_bid > 0:
                logger.info(f"   Quote: bid={fmt(snap_bid)} ask={fmt(snap_ask)} last={fmt(snap_last) if not math.isnan(snap_last) else 'N/A'}")
            if mcap:         logger.info(f"   MCap  : {mcap}")
            if float_shares: logger.info(f"   Float : {float_shares}")
            if min_vol:      logger.info(f"   MinVol: {min_vol}  RelVol: {rel_vol}  MonVol: {mon_vol}")
            if tags:         logger.info(f"   Tags  : {tags}")
            if alert_reason: logger.info(f"   Reason: {alert_reason}")
            if sec_filing:   logger.info(f"   SEC   : {sec_filing}")
            if news:         logger.info(f"   News  : {news[:80]}{'…' if len(news) > 80 else ''}")
            if recent_ipo:   logger.info(f"   IPO   : {recent_ipo}")
            if recent_rs:    logger.info(f"   R/S   : {recent_rs}")
            if zero_borrow:  logger.info(f"   Borrow: 0 – {zero_borrow}")

            # ── DAY limit order ────────────────────────────────────────────
            order_action = 'SELL' if is_short else 'BUY'
            timeout_sec = SELL_LIMIT_TIMEOUT_SEC if is_short else BUY_LIMIT_TIMEOUT_SEC
            buy_order              = LimitOrder(order_action, shares, limit_price)
            buy_order.tif          = 'DAY'
            buy_order.outsideRth   = True  # required for pre-market (4:00-9:30 ET)
            buy_trade     = ib.placeOrder(contract, buy_order)
            order_id      = buy_trade.order.orderId
            start         = get_et()  # used for execution_time in log_detailed_trade

            with stats_lock:
                stats['orders_placed'] += 1

            logger.info(f"   Submitted (ID: {order_id}) – waiting up to {timeout_sec}s for fill...")

            deadline = time.monotonic() + timeout_sec
            while not buy_trade.isDone() and time.monotonic() < deadline:
                ib.sleep(0.3)

                if reconnect_event.is_set() or not ib.isConnected():
                    logger.error(f"   ❌ Connection lost while waiting for {ticker} buy – aborting")
                    log_failed_trade(ticker, 'CONNECTION_LOST',
                                     'Disconnected during buy wait', order_id,
                                     snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    return

            # Cancel if not filled after timeout
            if not buy_trade.isDone():
                logger.info(f"   ⏱️  DAY limit not filled within {timeout_sec}s – cancelling")
                try:
                    ib.cancelOrder(buy_trade.order)
                    cancel_deadline = time.monotonic() + 5
                    while not buy_trade.isDone() and time.monotonic() < cancel_deadline:
                        ib.sleep(0.3)
                except Exception as ce:
                    logger.warning(f"   ⚠️  Cancel request failed: {ce}")

            # DAY limit outcome: fully filled, partially filled, or cancelled (no fill).
            # Re-read after cancel block in case a fill arrived during the cancel wait.
            fill_qty = int(buy_trade.orderStatus.filled)

            if fill_qty == 0:
                # Drain pending events so error handler fires before we check
                ib.sleep(0.1)

                rejection = _order_rejections.pop(order_id, None)

                if rejection and rejection['code'] == 201:
                    # Error 201: IBKR won't let us open this position.
                    # Reasons: closing-only status, compliance restriction,
                    # small cap restriction, no trading permission.
                    reason_short = rejection['message']
                    if 'closing-only' in reason_short.lower():
                        reason_short = 'Closing-only status'
                    elif 'Compliance' in reason_short:
                        reason_short = 'Compliance restriction'
                    elif 'Small Cap' in reason_short:
                        reason_short = 'Small cap restriction'
                    else:
                        reason_short = reason_short[:100]
                    logger.info(f"   🚫 Trade restricted: {reason_short}")
                    log_failed_trade(ticker, 'TRADE_RESTRICTED', reason_short,
                                     order_id,
                                     snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                    with stats_lock:
                        stats['orders_failed'] += 1
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    return

                elif rejection and rejection['code'] == 202:
                    # Warning 202: price cap — no retry, just log and move on.
                    logger.info(f"   ⏭️  Price cap (Warning 202) – skipped (limit was {fmt(limit_price)})")
                    log_failed_trade(ticker, 'PRICE_CAP_NO_FILL',
                                     f'Price cap (limit was {limit_price})',
                                     order_id,
                                     snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                    with stats_lock:
                        stats['orders_failed'] += 1
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    return

                else:
                    # Genuine no-fill: price moved past our limit
                    status = buy_trade.orderStatus.status
                    direction = "below" if is_short else "above"
                    logger.info(f"   ⏭️  No fill – price {direction} DAY limit {fmt(limit_price)} (status: {status})")
                    log_failed_trade(ticker, 'DAY_LIMIT_NO_FILL',
                                     f'Price {direction} limit {limit_price}', order_id,
                                     snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                    with stats_lock:
                        stats['orders_failed'] += 1
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    return

            if fill_qty < shares:
                logger.info(f"   ⚠️  Partial DAY limit fill: {fill_qty}/{shares} shares")

            fill_price   = buy_trade.orderStatus.avgFillPrice
            fill_time    = get_et()
            if is_short:
                # SHORT: we want to sell high, slippage = alert - fill (negative = got less)
                slippage     = alert_price - fill_price
            else:
                slippage     = fill_price - alert_price
            slippage_pct = (slippage / alert_price) * 100

            with stats_lock:
                stats['orders_filled'] += 1

            logger.info(f"   ✅ Filled: {fill_qty} × {fmt(fill_price)}")
            logger.info(f"   Slippage: {fmt(slippage)}  ({pct(slippage_pct)})")

            # ── Submit exit orders ─────────────────────────────────────────
            tp_order_id = None
            sl_order_id = None
            stop_price  = None   # SHORT synthetic stop trigger (None for LONG)

            if is_short:
                # SHORT: synthetic stop (optional) + time exit (optional).
                # Both checked from the main loop; both fire BUY LIMIT covers
                # at market × (1 + buffer/100) with outsideRth=True.
                #
                # Why not a real StopOrder?  IBKR stop orders only *trigger* during
                # RTH (09:30–16:00 ET), regardless of outsideRth=True.  On 2026-04-17
                # EFOI entered short at $2.66 with a $3.19 StopOrder; the stock ran
                # to $5.40 during the final 30 min of pre-market while the stop sat
                # dormant, then filled at $5.41 at 09:30 RTH open (−105% loss).  A
                # plain LimitOrder with outsideRth=True DOES execute outside RTH,
                # so we monitor price ourselves and fire a limit cover the instant
                # the trigger is crossed (or, for time exit, the timer fires).
                if SHORT_STOP_LOSS_PCT is not None:
                    stop_price = round(fill_price * (1 + SHORT_STOP_LOSS_PCT / 100), 4)
                    _window_str = (
                        f", active for {SHORT_STOP_WINDOW_MIN:g}min from entry"
                        if SHORT_STOP_WINDOW_MIN is not None else ""
                    )
                    logger.info(
                        f"   🛡️  Synthetic SL armed: cover when market ≥ {fmt(stop_price)}  "
                        f"(+{SHORT_STOP_LOSS_PCT}%{_window_str})"
                    )
                    # sl_order_id stays None until check_synthetic_stops() fires.

                if SHORT_TIME_EXIT_MIN is not None:
                    logger.info(
                        f"   ⏱️  Time exit armed: cover at +{SHORT_TIME_EXIT_MIN:g}min from entry "
                        f"(market × {1 + SHORT_TIME_EXIT_LIMIT_BUFFER_PCT/100:.2f} BUY LIMIT)"
                    )
                    # time_exit_fired stays False until check_time_exits() fires.

            else:
                # LONG: take-profit + stop-loss bracket (OCA)
                tp_price = round(fill_price * (1 + TAKE_PROFIT_PCT / 100), 2)
                sl_price = round(fill_price * (1 - STOP_LOSS_PCT / 100), 2)
                sl_price = max(sl_price, 0.01)  # IBKR minimum

                oca_group = f"bracket_{ticker}_{order_id}"

                try:
                    tp_order          = LimitOrder('SELL', fill_qty, tp_price)
                    tp_order.tif      = 'DAY'
                    tp_order.ocaGroup = oca_group
                    tp_order.ocaType  = 1  # cancel remaining on fill
                    tp_trade          = ib.placeOrder(contract, tp_order)
                    tp_order_id       = tp_trade.order.orderId

                    sl_order          = StopOrder('SELL', fill_qty, sl_price)
                    sl_order.tif      = 'DAY'
                    sl_order.ocaGroup = oca_group
                    sl_order.ocaType  = 1
                    sl_trade          = ib.placeOrder(contract, sl_order)
                    sl_order_id       = sl_trade.order.orderId

                    logger.info(
                        f"   🎯 Bracket: TP {fmt(tp_price)} (+{TAKE_PROFIT_PCT}%)  "
                        f"SL {fmt(sl_price)} (-{STOP_LOSS_PCT}%)  "
                        f"(IDs: {tp_order_id}/{sl_order_id})"
                    )
                except Exception as e:
                    logger.error(f"   ⚠️  Bracket order failed: {e} – will hold to EOD")

            # ── Match strategies ──────────────────────────────────────────────
            matched = match_strategies(
                alert_price=alert_price, mon_vol=mon_vol,
                zero_borrow=zero_borrow, entry_time=fill_time,
            )
            if matched:
                logger.info(f"   🎯 Strategies: {', '.join(matched)}")

            # ── Track position ────────────────────────────────────────────────
            with positions_lock:
                active_positions[ticker] = {
                    'shares'      : fill_qty,
                    'entry_price' : fill_price,
                    'alert_price' : alert_price,
                    'entry_time'  : fill_time,
                    'order_id'    : order_id,
                    'contract'    : contract,
                    'strategies'  : matched,
                    'slippage_pct': slippage_pct,
                    'pct_change'  : pct_change,
                    'mcap'        : mcap,
                    'float_shares': float_shares,
                    'rel_vol'     : rel_vol,
                    'tags'        : tags,
                    'zero_borrow' : zero_borrow,
                    'mon_vol'     : mon_vol,
                    'tp_order_id' : tp_order_id,
                    'sl_order_id' : sl_order_id,
                    # Synthetic-stop state (SHORT only; None/False for LONG).
                    # check_synthetic_stops() reads stop_price + stop_triggered
                    # and writes sl_order_id on fire.
                    'stop_price'    : stop_price,
                    'stop_triggered': False,
                    # Time-window rule: set to True by check_synthetic_stops()
                    # once `now - entry_time > SHORT_STOP_WINDOW_MIN`.  Once
                    # set, the synthetic stop stops monitoring this position
                    # and EOD close handles it.
                    'stop_window_expired': False,
                    # Time-exit state (SHORT only).  check_time_exits() flips
                    # this once it submits a BUY LIMIT cover at the configured
                    # number of minutes after entry.  Used to prevent re-fire.
                    'time_exit_fired': False,
                    'direction'   : TRADE_DIRECTION,
                }
                save_positions_state()

            entry_log_action = 'SELL' if is_short else 'BUY'
            log_trade(ticker, entry_log_action, fill_qty, fill_price, fill_time)
            log_detailed_trade(
                ticker=ticker, action='OPEN', shares=fill_qty,
                entry_price=fill_price, entry_time=fill_time,
                alert_price=alert_price,
                slippage=slippage, slippage_pct=slippage_pct,
                execution_time=(fill_time - start).total_seconds(),
                order_id=order_id,
                pct_change=pct_change, mcap=mcap, float_shares=float_shares,
                min_vol=min_vol, rel_vol=rel_vol, mon_vol=mon_vol,
                tags=tags, alert_reason=alert_reason, sec_filing=sec_filing,
                news=news, recent_ipo=recent_ipo, recent_rs=recent_rs,
                zero_borrow=zero_borrow,
                snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last,
            )
            if matched:
                log_strategy_trade(
                    ticker=ticker, action='OPEN', strategies=matched,
                    entry_price=fill_price, entry_time=fill_time,
                    alert_price=alert_price,
                    slippage_pct=slippage_pct,
                    pct_change=pct_change, mcap=mcap, float_shares=float_shares,
                    rel_vol=rel_vol, mon_vol=mon_vol, zero_borrow=zero_borrow,
                    n_strategies=len(matched),
                )

            log_separator()
            logger.info(
                f"📊 Positions: {len(active_positions)}  |  "
                f"Session P&L: ${stats['total_pnl']:+.2f}"
            )
            log_separator()

        finally:
            # Always cancel streaming subscription to free the market data line.
            # Without this, we'd exhaust all 100 lines within ~100 alerts.
            ib.cancelMktData(contract)

    except Exception as e:
        logger.error(f"❌ {ticker}: {e}")
        logger.error(traceback.format_exc())
        log_failed_trade(ticker, 'EXCEPTION', str(e))
        with stats_lock:
            stats['orders_failed'] += 1
        with positions_lock:
            pending_tickers.discard(ticker)

# ============ CSV LOGGING ============
def log_trade(ticker, action, shares, price, timestamp):
    _append_csv(TRADE_LOG_FILE, {
        'timestamp'  : timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'ticker'     : ticker,
        'action'     : action,
        'shares'     : shares,
        'price'      : round(price, 4),
        'total_value': round(shares * price, 2),
    })

def _exit_config_tag():
    """Build a compact string describing the SHORT exit configuration in
    effect right now.  Used as the `stop_percent` CSV column value so
    historical analysis can distinguish trades made under different rules.

    Examples:
        SL=None,      WIN=None,  TIME=30  → "TIME:30min"
        SL=20,        WIN=None,  TIME=None → "SL:20%"
        SL=20,        WIN=20,    TIME=None → "SL:20%@≤20min"
        SL=20,        WIN=20,    TIME=30   → "SL:20%@≤20min/TIME:30min"
        SL=None,      WIN=None,  TIME=None → "EOD_ONLY"
    """
    parts = []
    if SHORT_STOP_LOSS_PCT is not None:
        tag = f'SL:{SHORT_STOP_LOSS_PCT:g}%'
        if SHORT_STOP_WINDOW_MIN is not None:
            tag += f'@≤{SHORT_STOP_WINDOW_MIN:g}min'
        parts.append(tag)
    if SHORT_TIME_EXIT_MIN is not None:
        parts.append(f'TIME:{SHORT_TIME_EXIT_MIN:g}min')
    return '/'.join(parts) if parts else 'EOD_ONLY'


def log_detailed_trade(ticker, action, shares,
                        entry_price=None, exit_price=None,
                        entry_time=None,  exit_time=None,
                        alert_price=None,
                        slippage=None, slippage_pct=None, execution_time=None,
                        pnl=None, pnl_pct=None, hold_time_sec=None,
                        order_id=None, stop_order_id=None,
                        pct_change='', mcap='', float_shares='',
                        min_vol='', rel_vol='', mon_vol='',
                        tags='', alert_reason='', sec_filing='',
                        news='', recent_ipo='', recent_rs='', zero_borrow='',
                        snap_bid=None, snap_ask=None, snap_last=None):
    _append_csv(DETAILED_LOG_FILE, {
        'timestamp'         : get_et().strftime('%Y-%m-%d %H:%M:%S'),
        'ticker'            : ticker,
        'action'            : action,
        'shares'            : shares,
        'entry_price'       : round(entry_price,    4) if entry_price    is not None else '',
        'exit_price'        : round(exit_price,     4) if exit_price     is not None else '',
        'entry_time'        : entry_time.strftime('%Y-%m-%d %H:%M:%S') if entry_time else '',
        'exit_time'         : exit_time.strftime('%Y-%m-%d %H:%M:%S')  if exit_time  else '',
        'alert_price'       : round(alert_price,    4) if alert_price    is not None else '',
        'slippage'          : round(slippage,       4) if slippage       is not None else '',
        'slippage_pct'      : round(slippage_pct,   2) if slippage_pct   is not None else '',
        'execution_time_sec': round(execution_time, 2) if execution_time is not None else '',
        'pnl'               : round(pnl,            2) if pnl            is not None else '',
        'pnl_pct'           : round(pnl_pct,        2) if pnl_pct        is not None else '',
        'hold_time_sec'     : int(hold_time_sec)        if hold_time_sec  is not None else '',
        'hold_time_min'     : int(hold_time_sec / 60)   if hold_time_sec  is not None else '',
        # Reflect the actual exit configuration in effect for this trade.
        # SHORT mode: any of {synthetic SL, SL window, time exit, EOD-only}
        # depending on which config knobs are set.  Format is a slash-joined
        # tag like "TIME:30min" or "SL:20%@≤20min/TIME:30min" so downstream
        # analysis can identify the regime.
        # LONG mode: OCA bracket with TP/SL pair.
        'stop_percent'      : _exit_config_tag() if TRADE_DIRECTION == 'SHORT'
                              else f'TP:{TAKE_PROFIT_PCT:g}%/SL:{STOP_LOSS_PCT:g}%',
        'order_id'          : order_id      or '',
        'stop_order_id'     : stop_order_id or '',
        'pct_change'        : pct_change,
        'mcap'              : mcap,
        'float_shares'      : float_shares,
        'min_vol'           : min_vol,
        'rel_vol'           : rel_vol,
        'mon_vol'           : mon_vol,
        'tags'              : tags,
        'alert_reason'      : alert_reason,
        'sec_filing'        : sec_filing,
        'news'              : news,
        'recent_ipo'        : recent_ipo,
        'recent_rs'         : recent_rs,
        'zero_borrow'       : zero_borrow,
        'snap_bid'          : round(snap_bid, 4)  if snap_bid  is not None and not math.isnan(snap_bid)  else '',
        'snap_ask'          : round(snap_ask, 4)  if snap_ask  is not None and not math.isnan(snap_ask)  else '',
        'snap_last'         : round(snap_last, 4) if snap_last is not None and not math.isnan(snap_last) else '',
        'direction'         : TRADE_DIRECTION,
    })

# ============ GRACEFUL SHUTDOWN ============
shutdown_requested = False

def graceful_shutdown(signum=None, frame=None):
    global shutdown_requested
    if shutdown_requested:
        os._exit(0)
    shutdown_requested = True

    logger.info("\n🛑 Shutdown requested – saving state...")
    with positions_lock:
        save_positions_state()
        open_count = len(active_positions)

    logger.info(f"💾 {open_count} position(s) saved")
    log_separator()
    logger.info("📊 Final Session Stats:")
    logger.info(f"   Runtime   : {datetime.now() - stats['session_start']}")
    logger.info(f"   Alerts    : {stats['alerts_received']} received  /  {stats['alerts_skipped']} skipped")
    logger.info(f"   Orders    : {stats['orders_placed']} placed  /  {stats['orders_filled']} filled  /  {stats['orders_failed']} failed")
    logger.info(f"   Positions : {stats['positions_closed']} closed  /  {open_count} still open")
    logger.info(f"   P&L       : ${stats['total_pnl']:+.2f}")
    log_separator()

    if open_count == 0:
        cleanup_positions_state()

    if ib.isConnected():
        ib.disconnect()
        logger.info("Disconnected from Gateway")

    os._exit(0)

signal.signal(signal.SIGINT,  graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

# ============ DISCORD EVENTS ============
@client.event
async def on_ready():
    global ibkr_thread_started
    if ibkr_thread_started:
        logger.info("Discord reconnected – IBKR thread already running")
        return
    ibkr_thread_started = True

    log_separator("=")
    logger.info("🤖 DISCORD ALERT TRADING BOT – LIVE")
    logger.info(f"   Account    : {client.user}")
    logger.info(f"   Session    : {stats['session_start'].strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   Trade size : ${TRADE_AMOUNT}")
    logger.info(f"   Entry      : DAY limit (ask × {ASK_LIMIT_MULTIPLIER}, fallback: alert + {IOC_LIMIT_PCT}%)")
    logger.info(f"   Exit       : Bracket TP +{TAKE_PROFIT_PCT}% / SL -{STOP_LOSS_PCT}%, EOD safety net @ {EOD_CLOSE_HOUR:02d}:{EOD_CLOSE_MINUTE:02d} ET")
    logger.info(f"   Price range: {fmt(MIN_PRICE)} – {fmt(MAX_PRICE)}")
    logger.info(f"   Hours      : 04:00–16:00 ET (pre-market + RTH)")
    log_separator("=")

    threading.Thread(target=connect_ibkr_thread, daemon=True).start()

@client.event
async def on_message(message):
    if message.channel.id != CHANNEL_ID or message.author == client.user:
        return

    # Assemble full text from message body and all embed fields
    parts = [message.content]
    for embed in message.embeds:
        if embed.author and embed.author.name:
            parts.append(f"Author: {embed.author.name}")
        if embed.title:
            parts.append(f"Title: {embed.title}")
        if embed.description:
            parts.append(f"Desc: {embed.description}")
        for field in embed.fields:
            parts.append(f"{field.name}: {field.value}")

    alert = parse_alert("\n".join(filter(None, parts)))

    with stats_lock:
        stats['alerts_received'] += 1

    if not alert:
        with stats_lock:
            stats['alerts_skipped'] += 1
        return

    ticker = alert['ticker']

    # Ignore alerts outside regular trading hours
    if not is_rth():
        now_et = get_et()
        logger.info(
            f"⏰ {ticker}: outside market hours "
            f"({now_et.strftime('%H:%M ET')}) – skipped"
        )
        log_failed_trade(
            ticker, 'OUTSIDE_RTH',
            f"Alert at {now_et.strftime('%H:%M:%S ET')}"
        )
        with stats_lock:
            stats['alerts_skipped'] += 1
        return

    # Bug 2 fix: reject alerts that arrive after the EOD cutoff time.
    # is_rth() passes until 16:00, but EOD close fires at 15:55 and is
    # one-shot, so positions opened between 15:55–16:00 would be stranded.
    if is_past_eod_cutoff():
        now_et = get_et()
        logger.info(
            f"⏰ {ticker}: past EOD cutoff "
            f"({now_et.strftime('%H:%M ET')}) – skipped"
        )
        log_failed_trade(
            ticker, 'PAST_EOD_CUTOFF',
            f"Alert at {now_et.strftime('%H:%M:%S ET')}"
        )
        with stats_lock:
            stats['alerts_skipped'] += 1
        return

    # Skip if we already hold or are buying this ticker
    with positions_lock:
        already_active  = ticker in active_positions
        already_pending = ticker in pending_tickers

    if already_active or already_pending:
        status = 'active' if already_active else 'queued'
        logger.warning(f"⚠️  {ticker} already {status} – skipping")
        log_failed_trade(ticker, 'DUPLICATE', f'Already {status}')
        with stats_lock:
            stats['alerts_skipped'] += 1
        return

    with positions_lock:
        pending_tickers.add(ticker)

    log_separator()
    logger.info(f"🚨 ALERT: {ticker} @ {fmt(alert['price'])}")
    alert['queued_at'] = time.monotonic()
    order_queue.put(alert)

# ============ IBKR THREAD ============
def connect_ibkr_thread():
    """Owns the ib_insync event loop. All IB API calls happen here."""
    global ibkr_connected, _disconnected_at

    util.startLoop()

    log_separator("=")
    logger.info("🔌 CONNECTING TO IB GATEWAY")
    log_separator("=")

    if not connect_with_retry(max_retries=3):
        logger.error("❌ Could not connect to Gateway – exiting IBKR thread")
        return

    threading.Thread(target=monitor_connection, daemon=True).start()
    logger.info("Connection monitor started")

    # Sync server time immediately so is_rth() and check_eod_close()
    # are accurate from the very first loop iteration
    _refresh_server_time()
    _loop_counter = [0]

    while True:
        try:
            # Bug 5 fix: update the thread-safe connectivity flag from the IBKR
            # thread (the only thread that should touch ib_insync internals).
            if ib.isConnected():
                _ibkr_connected_flag.set()
            else:
                _ibkr_connected_flag.clear()

            # Reconnect if monitor_connection raised the flag
            if reconnect_event.is_set():
                reconnect_event.clear()
                logger.warning("🔄 Reconnecting...")
                if connect_with_retry(max_retries=5):
                    _disconnected_at = None
                    _refresh_server_time()
                    logger.info("✅ Reconnected")
                else:
                    logger.error("❌ Reconnection failed – will retry on next signal")

            # ── Check bracket fills (take-profit / stop-loss) ─────────────────
            check_bracket_fills()

            # ── Check synthetic stops (SHORT only) ─────────────────────────────
            # Fires marketable BUY LIMIT covers when price crosses stop_price.
            # Handles the pre-market gap-through that a hardware StopOrder can't
            # catch (see Apr 17 EFOI incident).
            check_synthetic_stops()

            # ── Check time-based exits (SHORT only) ────────────────────────────
            # Fires marketable BUY LIMIT covers for positions older than
            # SHORT_TIME_EXIT_MIN minutes.  Exits before late-day squeeze risk
            # dominates.  See May 9 backtest analysis.
            check_time_exits()

            # ── EOD close runs FIRST ─────────────────────────────────────────
            # This must happen before order processing so that a blocked
            # place_order call can never prevent EOD close from firing.
            check_eod_close()

            # ── Process one queued order ─────────────────────────────────────
            try:
                order_data = order_queue.get_nowait()
            except queue.Empty:
                order_data = None

            if order_data is not None:
                ticker = order_data['ticker']
                should_process = True

                # Staleness gate: drop orders that have been sitting in the
                # queue too long (e.g. IBKR thread was stuck/disconnected).
                queued_at = order_data.get('queued_at')
                if queued_at is not None:
                    age = time.monotonic() - queued_at
                    if age > MAX_ORDER_AGE_SEC:
                        logger.warning(
                            f"⚠️  {ticker}: stale order ({int(age)}s old) – dropping"
                        )
                        log_failed_trade(ticker, 'STALE_ORDER',
                                         f'Queued {int(age)}s ago, max {MAX_ORDER_AGE_SEC}s')
                        with positions_lock:
                            pending_tickers.discard(ticker)
                        should_process = False

                # EOD cutoff gate: drop orders if we've passed the EOD cutoff
                # since the order was queued.
                if should_process and is_past_eod_cutoff():
                    logger.warning(
                        f"⚠️  {ticker}: past EOD cutoff at dequeue – dropping"
                    )
                    log_failed_trade(ticker, 'PAST_EOD_CUTOFF',
                                     'Past EOD cutoff when dequeued')
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    should_process = False

                if should_process:
                    if ib.isConnected():
                        place_order(
                            order_data['ticker'],    order_data['price'],
                            order_data['timestamp'], order_data.get('tags', ''),
                            order_data.get('alert_reason', ''),
                            order_data.get('sec_filing', ''),
                            order_data.get('news', ''),
                            order_data.get('recent_ipo', ''),
                            order_data.get('recent_rs', ''),
                            order_data.get('zero_borrow', ''),
                            order_data.get('pct_change', ''),
                            order_data.get('mcap', ''),
                            order_data.get('float_shares', ''),
                            order_data.get('min_vol', ''),
                            order_data.get('rel_vol', ''),
                            order_data.get('mon_vol', ''),
                        )
                    else:
                        # Drop orders queued during an outage longer than 60 s
                        outage = (
                            (datetime.now() - _disconnected_at).total_seconds()
                            if _disconnected_at else 0
                        )
                        if outage > 60:
                            logger.warning(
                                f"⚠️  Dropping stale alert for {order_data['ticker']} "
                                f"({int(outage)}s outage)"
                            )
                            log_failed_trade(order_data['ticker'], 'STALE_ALERT',
                                             f'Dropped after {int(outage)}s outage')
                            with positions_lock:
                                pending_tickers.discard(order_data['ticker'])
                        else:
                            order_queue.put(order_data)
                            time.sleep(2)  # use time.sleep, not ib.sleep — ib may be dead

            ib.sleep(1)

            # Refresh IBKR server time every ~30 s
            _loop_counter[0] = (_loop_counter[0] + 1) % 30
            if _loop_counter[0] == 0:
                _refresh_server_time()
                reconcile_portfolio()  # throttled internally to every 5 min

        except (ConnectionError, OSError, asyncio.CancelledError,
                asyncio.TimeoutError) as e:
            # Connection died mid-loop. Log it and trigger reconnect.
            # The loop continues — reconnect logic at the top will handle it.
            logger.error(f"❌ IBKR loop connection error: {e}")
            ibkr_connected = False
            _ibkr_connected_flag.clear()
            _disconnected_at = datetime.now()
            reconnect_event.set()
            time.sleep(2)  # use time.sleep, not ib.sleep — ib may be dead

        except Exception as e:
            # Catch-all so the thread never dies silently.
            logger.error(f"❌ Unexpected error in IBKR loop: {e}")
            logger.error(traceback.format_exc())
            time.sleep(5)  # avoid tight spin on repeated errors

# ============ ALERT PARSING ============
def parse_alert(message_content):
    """Extract ticker, price, and metadata from a TrendVision embed.
    Returns None for any message that should not be traded."""

    # Ticker – tolerates Unicode flag emojis between the symbol and "- Alert"
    ticker_match = re.search(r'\b([A-Z]{1,6})\b[^\n\-]*-\s*Alert', message_content)
    if not ticker_match:
        return None

    ticker = ticker_match.group(1)

    # Only trade the first alert for a ticker each day (Alert #1)
    if not re.search(r'Alert\s*#1\b', message_content):
        m = re.search(r'Alert\s*#(\d+)', message_content)
        num = m.group(1) if m else '?'
        logger.info(f"ℹ️  {ticker}: Alert #{num} – skipping (only trade #1)")
        return None

    # Price
    price_match = re.search(
        r'Price\s*[:\-\s]?\s*\$?([\d.]+)',
        message_content, re.IGNORECASE | re.MULTILINE
    )
    if not price_match:
        log_failed_trade(ticker, 'PARSE_ERROR', 'No price found')
        return None
    try:
        price = float(price_match.group(1))
    except ValueError:
        log_failed_trade(ticker, 'PARSE_ERROR', 'Invalid price format')
        return None

    if not (MIN_PRICE <= price <= MAX_PRICE):
        log_failed_trade(ticker, 'PRICE_FILTER',
                         f'{price} outside [{MIN_PRICE}, {MAX_PRICE}]')
        return None

    # ── Optional metadata ──────────────────────────────────────────────────

    # Tags: from embed.author.name ("Known Runner", "Reg SHO・Known Runner", …)
    # Non-ASCII chars (emoji, ・) are replaced with a space to keep logs clean.
    tags = ''
    for prefix in ('Author:', 'Title:', 'Desc:'):
        m = re.search(rf'^{prefix}\s*(.+)$', message_content, re.MULTILINE)
        if m:
            candidate = m.group(1).strip()
            if '- Alert' not in candidate and 'Price' not in candidate:
                tags = re.sub(r'[^\x00-\x7F]+', ' ', candidate).strip()
                tags = re.sub(r'\s{2,}', ' ', tags)
                break

    # Alert Reason ("BREAKOUT", "MOMENTUM", "BTT V", "HUGE S", …)
    alert_reason = ''
    m = re.search(r'Alert\s*Reason\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        alert_reason = m.group(1).strip()

    # SEC filings – anchored to "FORM"; they arrive as markdown links
    # like "[FORM 8-K](https://www.sec.gov/...)", so keep only the form name.
    sec_filing = ''
    m = re.search(
        r"Today'?s?\s*SEC\s*Fil(?:ing)?\s*[:\n]\s*((?:\[?FORM[^\n]+\n?)+)",
        message_content, re.IGNORECASE
    )
    if m:
        sec_filing = ', '.join(re.findall(r'FORM [^\]\n]+', m.group(1)))

    # News headlines (bullet-separated)
    news = ''
    m = re.search(
        r"Today'?s?\s*News\s*[:\n]\s*(.+?)"
        r"(?=\n(?:Today'?s?|Alert\s*Reason|Recent|0\s*Borrow|TrendVision)|$)",
        message_content, re.IGNORECASE | re.DOTALL
    )
    if m:
        headlines = [h.strip() for h in re.split(r'\s*•\s*', m.group(1).strip()) if h.strip()]
        news = ' | '.join(headlines)

    # Recent IPO
    recent_ipo = ''
    m = re.search(r'Recent\s*IPO\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        recent_ipo = m.group(1).strip()

    # Recent reverse split
    recent_rs = ''
    m = re.search(r'Recent\s*R/S\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        recent_rs = m.group(1).strip()

    # Zero borrow
    zero_borrow = ''
    m = re.search(r'0\s*Borrow\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        zero_borrow = m.group(1).strip()

    # ── Numeric scanner metrics ────────────────────────────────────────────
    # These appear as Discord embed fields, assembled as "FieldName: Value".
    # The regex also handles the raw "FieldName\nValue" form (description text).

    # % change from the alert title line e.g. "LGHL - Alert #1 | ↑ 14.62%"
    # Stored as a signed string: "+14.62%" or "-4.62%"
    pct_change = ''
    m = re.search(r'Alert\s*#\d+[^\n↑↓]*([↑↓])\s*([\d.]+)%', message_content)
    if m:
        sign = '+' if m.group(1) == '↑' else '-'
        pct_change = f"{sign}{m.group(2)}%"

    # Market cap e.g. "276.86K", "7.39M"
    mcap = ''
    m = re.search(r'MCap\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        mcap = m.group(1).strip()

    # Float e.g. "210K", "12.70M", "N/A"
    float_shares = ''
    m = re.search(r'Float\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        float_shares = m.group(1).strip()

    # Minute volume e.g. "4.41K", "26.70K"
    min_vol = ''
    m = re.search(r'Min\s*Vol\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        min_vol = m.group(1).strip()

    # Relative volume e.g. "3.49x", "981.23x"
    rel_vol = ''
    m = re.search(r'Rel\s*Vol\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        rel_vol = m.group(1).strip()

    # Money volume e.g. "$6.56K", "$92.54K"
    mon_vol = ''
    m = re.search(r'Mon\s*Vol\s*[:\n]\s*([^\n]+)', message_content, re.IGNORECASE)
    if m:
        mon_vol = m.group(1).strip()

    logger.info(
        f"✅ {ticker} @ {fmt(price)}"
        + (f"  {pct_change}"          if pct_change   else '')
        + (f"  mcap={mcap}"           if mcap         else '')
        + (f"  float={float_shares}"  if float_shares else '')
        + (f"  minvol={min_vol}"      if min_vol      else '')
        + (f"  relvol={rel_vol}"      if rel_vol      else '')
        + (f"  monvol={mon_vol}"      if mon_vol      else '')
        + (f"  tags={tags}"           if tags         else '')
        + (f"  reason={alert_reason}" if alert_reason else '')
        + (f"  sec={sec_filing}"      if sec_filing   else '')
        + (f"  ipo={recent_ipo}"      if recent_ipo   else '')
        + (f"  rs={recent_rs}"        if recent_rs    else '')
        + (f"  0borrow"               if zero_borrow  else '')
    )

    return {
        'ticker'      : ticker,
        'price'       : price,
        'timestamp'   : get_et(),
        'pct_change'  : pct_change,
        'mcap'        : mcap,
        'float_shares': float_shares,
        'min_vol'     : min_vol,
        'rel_vol'     : rel_vol,
        'mon_vol'     : mon_vol,
        'tags'        : tags,
        'alert_reason': alert_reason,
        'sec_filing'  : sec_filing,
        'news'        : news,
        'recent_ipo'  : recent_ipo,
        'recent_rs'   : recent_rs,
        'zero_borrow' : zero_borrow,
    }

# ============ MAIN ============
if __name__ == "__main__":
    log_separator("=")
    logger.info("Starting bot...")
    log_separator("=")

    if not DISCORD_TOKEN:
        logger.error("❌ DISCORD_TOKEN is not set – add it to .env (see .env.example)")
        exit(1)

    # Create or validate CSV files with correct headers.
    # If a file exists with stale headers (e.g. missing snap_bid/ask/last),
    # rename it as a backup and create a fresh file.  Data in the old file
    # is preserved — it just won't be auto-appended to.
    for filepath, cols in [
        (TRADE_LOG_FILE, [
            'timestamp', 'ticker', 'action', 'shares', 'price', 'total_value',
        ]),
        (DETAILED_LOG_FILE, [
            'timestamp', 'ticker', 'action', 'shares',
            'entry_price', 'exit_price', 'entry_time', 'exit_time',
            'alert_price', 'slippage', 'slippage_pct', 'execution_time_sec',
            'pnl', 'pnl_pct', 'hold_time_sec', 'hold_time_min',
            'stop_percent', 'order_id', 'stop_order_id',
            'pct_change', 'mcap', 'float_shares', 'min_vol', 'rel_vol', 'mon_vol',
            'tags', 'alert_reason', 'sec_filing',
            'news', 'recent_ipo', 'recent_rs', 'zero_borrow',
            'snap_bid', 'snap_ask', 'snap_last', 'direction',
        ]),
        (FAILED_TRADES_FILE, [
            'timestamp', 'ticker', 'failure_reason', 'details',
            'order_id', 'session_id',
            'snap_bid', 'snap_ask', 'snap_last',
        ]),
        (STRATEGY_LOG_FILE, [
            'timestamp', 'ticker', 'action', 'strategies', 'n_strategies',
            'entry_price', 'exit_price', 'entry_time', 'exit_time',
            'alert_price', 'pnl', 'pnl_pct', 'hold_time_min',
            'slippage_pct', 'pct_change', 'mcap', 'float_shares',
            'rel_vol', 'mon_vol', 'zero_borrow',
        ]),
    ]:
        if not os.path.exists(filepath):
            pd.DataFrame(columns=cols).to_csv(filepath, index=False)
            logger.info(f"Created {filepath}")
        else:
            # Validate existing header matches expected columns
            try:
                with open(filepath) as f:
                    existing_header = f.readline().strip().split(',')
                if existing_header != cols:
                    backup = f"{filepath}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                    os.rename(filepath, backup)
                    pd.DataFrame(columns=cols).to_csv(filepath, index=False)
                    logger.warning(
                        f"⚠️  {filepath}: stale header ({len(existing_header)} cols, "
                        f"expected {len(cols)}) – backed up to {backup}, created fresh"
                    )
            except Exception as e:
                logger.warning(f"⚠️  Could not validate {filepath} header: {e}")

    try:
        client.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Discord error: {e}")
        graceful_shutdown()