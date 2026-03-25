#!/usr/bin/env python3
"""
Discord Alert Trading Bot
=========================================================
Monitors a Discord channel for TrendVision volume alerts and
automatically trades them through Interactive Brokers.

Design principles:
  - Alerts accepted 4:00–15:55 ET (pre-market + RTH, EOD cutoff)
  - IOC limit buy to prevent slippage (alert price + IOC_LIMIT_PCT%)
  - EOD-only exit: all positions closed at 15:55 ET (no trailing stop)
  - Time sourced from IBKR server clock, not the local machine
  - Position state persisted to JSON for recovery after restarts

Order flow:
  1. Discord alert received → parse ticker + price
  2. RTH check (IBKR server time)
  3. Duplicate check (active_positions + pending_tickers)
  4. IOC limit BUY queued to IBKR thread
  5. On fill → position tracked (no stop placed)
  6. All positions market-sold at 15:55 ET
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
from pathlib import Path
import json
import signal
import time
import asyncio
import math

# ============ CONFIGURATION ============
DISCORD_TOKEN = "REDACTED_DISCORD_TOKEN"          # ← paste your bot token here
CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID", "0"))
IBKR_HOST      = "127.0.0.1"
IBKR_PORT      = 4002
IBKR_CLIENT_ID = 1

# Trading
TRADE_AMOUNT          = 100    # dollars per trade (shares = floor(TRADE_AMOUNT / price))
IOC_LIMIT_PCT         = 3.0    # max % above alert price for IOC limit buy (fallback when no quote)
MIN_PRICE             = 0.10   # alerts below this price are ignored
MAX_PRICE             = 500.0  # alerts above this price are ignored
USE_MARKET_ORDERS     = True   # True = market orders (fill everything), False = IOC limit orders

# Real-time quote settings
QUOTE_WAIT_SEC        = 1.0    # max seconds to wait for streaming ask after reqMktData
QUOTE_POLL_INTERVAL   = 0.1    # seconds between polls while waiting for quote
EOD_QUOTE_WAIT_SEC    = 2.0    # max seconds to wait for bid before EOD sell
ASK_LIMIT_MULTIPLIER  = 1.05   # IOC limit = current ask × this (e.g. 1.05 = 5% above ask)

# End-of-day forced close – all positions market-sold at this time (ET)
EOD_CLOSE_HOUR   = 15
EOD_CLOSE_MINUTE = 55

# Maximum age (seconds) for a queued order before it's considered stale.
# If the IBKR thread is blocked, orders can sit in the queue for hours.
# This prevents executing old alerts when the thread finally unblocks.
MAX_ORDER_AGE_SEC = 120

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

from logging.handlers import TimedRotatingFileHandler

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
ib = IB()

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

def calc_commission(shares):
    """IBKR commission for one order leg."""
    return max(MIN_COMMISSION, shares * COMMISSION_PER_SHARE)

def calc_pnl(entry_price, exit_price, shares):
    """Net P&L and % after round-trip commissions."""
    gross            = (exit_price - entry_price) * shares
    total_commission = calc_commission(shares) * 2
    net              = gross - total_commission
    net_pct          = (net / (entry_price * shares)) * 100
    return net, net_pct, total_commission

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
        'timestamp'     : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
# Strategies derived from analysis of 2,801 alert #1s (Feb 2 – Mar 7).
# Each strategy identifies a statistically significant pattern where
# stocks continued to run >5% after the alert at an above-baseline rate.
# Baseline: 28.6% of alert #1s gain >5%.  Every strategy below is 40%+.
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

def _parse_pct_val(s):
    """Convert '+15.33%' to 15.33."""
    if not s or pd.isna(s) or str(s) == '':
        return None
    try:
        return float(str(s).replace('%', '').replace('+', ''))
    except (ValueError, TypeError):
        return None

def _parse_relvol_val(s):
    """Convert '47.52x' to 47.52."""
    if not s or pd.isna(s) or str(s) == '':
        return None
    try:
        return float(str(s).replace('x', ''))
    except (ValueError, TypeError):
        return None

def match_strategies(alert_price, pct_change='', mcap='', float_shares='',
                     rel_vol='', tags='', zero_borrow='', entry_time=None):
    """Return a list of strategy names that this alert matches.

    Strategies (sorted by historical >5% continuation rate):
      ZERO_BORROW_LOW_FLOAT  68.1%  – Zero borrow + float <= 20M
      ZERO_BORROW_MOMENTUM   62.8%  – Zero borrow + pct change >= 10%
      RUNNER_MOMENTUM        50.8%  – Known Runner tag + pct >= 15% + float <= 20M
      PENNY_RUNNER           50.2%  – Price < $1 + pct >= 15%
      PREMARKET_MOMENTUM     49.0%  – Before 8 AM ET + pct >= 10%
      MICRO_BREAKOUT         49.2%  – MCap < $25M + pct >= 20%
      VOLUME_SPIKE           46.6%  – RelVol >= 40x + float <= 20M
      MICRO_SQUEEZE          46.1%  – pct >= 15% + float <= 20M + mcap < $100M
    """
    strategies = []

    p   = _parse_pct_val(pct_change)
    mc  = _parse_money_val(mcap)
    fl  = _parse_money_val(float_shares)
    rv  = _parse_relvol_val(rel_vol)
    pr  = alert_price
    tgs = str(tags) if tags else ''
    zb  = bool(zero_borrow and str(zero_borrow).strip())

    # Determine ET hour of entry
    hour_et = None
    if entry_time is not None:
        hour_et = entry_time.astimezone(ET).hour if hasattr(entry_time, 'astimezone') else None
        if hour_et is None:
            # entry_time is naive (local) – convert via get_et() offset
            try:
                hour_et = get_et().hour
            except Exception:
                pass

    # S1: ZERO_BORROW_LOW_FLOAT (68.1%)
    if zb and fl is not None and fl <= 20e6:
        strategies.append('ZERO_BORROW_LOW_FLOAT')

    # S2: ZERO_BORROW_MOMENTUM (62.8%)
    if zb and p is not None and p >= 10:
        strategies.append('ZERO_BORROW_MOMENTUM')

    # S3: RUNNER_MOMENTUM (50.8%)
    if 'Known Runner' in tgs and p is not None and p >= 15 and fl is not None and fl <= 20e6:
        strategies.append('RUNNER_MOMENTUM')

    # S4: PENNY_RUNNER (50.2%)
    if pr is not None and pr < 1 and p is not None and p >= 15:
        strategies.append('PENNY_RUNNER')

    # S5: PREMARKET_MOMENTUM (49.0%)
    if hour_et is not None and hour_et < 8 and p is not None and p >= 10:
        strategies.append('PREMARKET_MOMENTUM')

    # S6: MICRO_BREAKOUT (49.2%)
    if mc is not None and mc < 25e6 and p is not None and p >= 20:
        strategies.append('MICRO_BREAKOUT')

    # S7: VOLUME_SPIKE (46.6%)
    if rv is not None and rv >= 40 and fl is not None and fl <= 20e6:
        strategies.append('VOLUME_SPIKE')

    # S8: MICRO_SQUEEZE (46.1%)
    if p is not None and p >= 15 and fl is not None and fl <= 20e6 and mc is not None and mc < 100e6:
        strategies.append('MICRO_SQUEEZE')

    return strategies

def log_strategy_trade(ticker, action, strategies, entry_price=None,
                       exit_price=None, entry_time=None, exit_time=None,
                       alert_price=None, pnl=None, pnl_pct=None,
                       hold_time_sec=None, slippage_pct=None,
                       pct_change='', mcap='', float_shares='',
                       rel_vol='', tags='', zero_borrow='',
                       n_strategies=0):
    """Log a strategy-tagged trade to STRATEGY_LOG_FILE."""
    _append_csv(STRATEGY_LOG_FILE, {
        'timestamp'      : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
        'tags'           : tags,
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
                'symbol'       : pos['contract'].symbol,
                'strategies'   : pos.get('strategies', []),
                'slippage_pct' : pos.get('slippage_pct'),
                'pct_change'   : pos.get('pct_change', ''),
                'mcap'         : pos.get('mcap', ''),
                'float_shares' : pos.get('float_shares', ''),
                'rel_vol'      : pos.get('rel_vol', ''),
                'tags'         : pos.get('tags', ''),
                'zero_borrow'  : pos.get('zero_borrow', ''),
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
                    if ibkr_pos and ibkr_pos.position > 0:
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
      - EOD sell times out (EOD_SELL_TIMEOUT) and the position is removed
        from tracking but remains open in IBKR
      - The bot restarts and doesn't restore the position
      - Manual intervention leaves a position behind

    Runs on startup and every ~5 minutes during trading hours.
    Must only be called from the IBKR thread."""
    global _last_reconcile

    if not ib.isConnected():
        return

    # Only reconcile during market hours — avoids selling orphans when
    # there may be no liquidity (pre-market before 4 AM, after 4 PM).
    if not is_rth():
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
        if pos.position <= 0:
            continue  # skip short positions and zero positions
        ticker = pos.contract.symbol
        if ticker in RECONCILE_IGNORE:
            continue
        if ticker not in tracked and ticker not in pending:
            orphans.append(pos)

    if not orphans:
        return

    log_separator("=")
    logger.warning(
        f"🧹 RECONCILIATION: found {len(orphans)} orphaned position(s) – selling"
    )
    log_separator("=")

    for pos in orphans:
        ticker = pos.contract.symbol
        shares = int(pos.position)
        try:
            contract = Stock(ticker, 'SMART', 'USD')
            ib.qualifyContracts(contract)

            sell_order     = MarketOrder('SELL', shares)
            sell_order.tif = 'DAY'
            sell_trade     = ib.placeOrder(contract, sell_order)
            logger.info(f"   📤 {ticker}: selling {shares} orphaned shares")

            deadline = time.monotonic() + 15
            while not sell_trade.isDone() and time.monotonic() < deadline:
                ib.sleep(0.5)

            if sell_trade.orderStatus.status == 'Filled':
                exit_price = sell_trade.orderStatus.avgFillPrice
                logger.info(f"   ✅ {ticker}: sold {shares} @ {fmt(exit_price)}")
                log_trade(ticker, 'SELL', shares, exit_price, datetime.now())
                log_detailed_trade(
                    ticker=ticker, action='ORPHAN_CLOSE', shares=shares,
                    exit_price=exit_price, exit_time=datetime.now(),
                )
            else:
                logger.error(
                    f"   ❌ {ticker}: orphan sell status {sell_trade.orderStatus.status} "
                    f"– will retry next reconciliation"
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

# ============ END-OF-DAY CLOSE ============
_eod_closed_today = None

def check_eod_close():
    """Market-sell all open positions at EOD_CLOSE_HOUR:EOD_CLOSE_MINUTE ET.
    Fires once per trading day using IBKR server time.
    Must only be called from the IBKR thread."""
    global _eod_closed_today

    now_et = get_et()
    today  = now_et.date()

    if now_et.weekday() >= 5 or _eod_closed_today == today:
        return

    eod = now_et.replace(hour=EOD_CLOSE_HOUR, minute=EOD_CLOSE_MINUTE,
                          second=0, microsecond=0)
    if now_et < eod:
        return

    # Bug 4 fix: drain the order queue so no new orders can be placed after
    # EOD fires.  Any alerts that arrived while place_order was blocking will
    # be discarded instead of opening positions that would never get closed.
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

    log_separator("=")
    logger.warning(
        f"🔔 EOD CLOSE @ {now_et.strftime('%H:%M ET')} – "
        f"closing {len(snapshot)} position(s)"
    )
    log_separator("=")

    failed_tickers = []

    # Fetch actual IBKR portfolio ONCE before the sell loop so we never
    # sell shares we don't actually hold (prevents creating short positions
    # when a previous sell filled but wasn't detected as 'Filled' in time).
    try:
        ibkr_portfolio = {p.contract.symbol: int(p.position) for p in ib.portfolio()}
    except Exception as e:
        logger.error(f"   ❌ Could not fetch portfolio for EOD close: {e}")
        return  # retry next loop — don't sell blind

    for ticker, pos in snapshot:
        try:
            # ── Verify actual IBKR position before selling ────────────────
            actual_shares = ibkr_portfolio.get(ticker, 0)
            if actual_shares <= 0:
                # Position is gone — a previously-pending sell order filled.
                # Try to find the fill details so we can log the EOD_CLOSE.
                exit_price = None
                try:
                    completed_sells = [
                        t for t in ib.trades()
                        if t.contract.symbol == ticker
                        and t.order.action == 'SELL'
                        and t.orderStatus.status == 'Filled'
                    ]
                    if completed_sells:
                        exit_price = completed_sells[-1].orderStatus.avgFillPrice
                except Exception:
                    pass

                if exit_price and exit_price > 0:
                    exit_time  = datetime.now()
                    sell_qty_logged = pos['shares']
                    net_pnl, net_pnl_pct, commission = calc_pnl(
                        pos['entry_price'], exit_price, sell_qty_logged)
                    hold_sec = (exit_time - pos['entry_time']).total_seconds()

                    with stats_lock:
                        stats['total_pnl']        += net_pnl
                        stats['positions_closed'] += 1

                    logger.info(
                        f"   ✅ {ticker} @ {fmt(exit_price)} (delayed fill)  "
                        f"P&L: ${net_pnl:+.2f} ({pct(net_pnl_pct)}, net of commissions)"
                    )
                    log_trade(ticker, 'SELL', sell_qty_logged, exit_price, exit_time)
                    log_detailed_trade(
                        ticker=ticker, action='EOD_CLOSE', shares=sell_qty_logged,
                        entry_price=pos['entry_price'], exit_price=exit_price,
                        entry_time=pos['entry_time'],   exit_time=exit_time,
                        alert_price=pos['alert_price'],
                        pnl=net_pnl, pnl_pct=net_pnl_pct, hold_time_sec=hold_sec,
                    )
                else:
                    logger.warning(
                        f"   ⚠️  {ticker}: no IBKR position (actual={actual_shares}) "
                        f"– already sold, removing from tracking"
                    )

                with positions_lock:
                    active_positions.pop(ticker, None)
                    pending_tickers.discard(ticker)
                continue

            # Use actual IBKR shares, not tracked shares, in case of partial
            # fills or other discrepancies.
            sell_qty = actual_shares

            # ── Check for already-pending sell orders ─────────────────────
            # If a previous EOD sell is still "Submitted" (working but slow
            # to fill), don't submit another — that would create a short.
            # Add to failed_tickers so _eod_closed_today stays unset and
            # check_eod_close fires again in ~1s. On the next pass, either:
            #   - Portfolio shows 0 shares → the actual_shares <= 0 block
            #     catches it, finds the fill price, and logs EOD_CLOSE.
            #   - Still pending → we skip again and retry next pass.
            existing_sells = [
                t for t in ib.openTrades()
                if t.contract.symbol == ticker
                and t.order.action == 'SELL'
                and t.orderStatus.status in ('Submitted', 'PreSubmitted')
            ]
            if existing_sells:
                pending_qty = sum(int(t.order.totalQuantity) for t in existing_sells)
                logger.info(
                    f"   ⏳ {ticker}: sell already pending "
                    f"({pending_qty} shares, {existing_sells[0].orderStatus.status}) "
                    f"– skipping to avoid double-sell, will check again"
                )
                failed_tickers.append(ticker)
                continue

            # ── Request streaming quote before selling ────────────────────
            eod_snap_bid = eod_snap_ask = eod_snap_last = float('nan')
            try:
                eod_ticker = ib.reqMktData(pos['contract'], '', snapshot=False, regulatorySnapshot=False)
                eod_quote_deadline = time.monotonic() + EOD_QUOTE_WAIT_SEC
                while math.isnan(eod_ticker.bid) and time.monotonic() < eod_quote_deadline:
                    ib.sleep(QUOTE_POLL_INTERVAL)
                eod_snap_bid  = eod_ticker.bid
                eod_snap_ask  = eod_ticker.ask
                eod_snap_last = eod_ticker.last
            except Exception as qe:
                logger.debug(f"   {ticker}: EOD quote request failed: {qe}")
            finally:
                try:
                    ib.cancelMktData(pos['contract'])
                except Exception:
                    pass  # best-effort cancel; don't block EOD close

            has_bid = not math.isnan(eod_snap_bid) and eod_snap_bid > 0

            # ── Choose order type based on bid availability ───────────────
            if has_bid:
                sell_order     = MarketOrder('SELL', sell_qty)
                sell_order.tif = 'DAY'
                order_desc     = f"MKT (bid={fmt(eod_snap_bid)})"
            else:
                # No bid — use limit sell to avoid hanging market order.
                # Use last * 0.90 if available, otherwise entry * 0.50 as floor.
                has_last = not math.isnan(eod_snap_last) and eod_snap_last > 0
                if has_last:
                    limit_sell_price = round(eod_snap_last * 0.90, 4)
                else:
                    limit_sell_price = round(pos['entry_price'] * 0.50, 4)
                limit_sell_price = max(limit_sell_price, 0.0001)  # IBKR minimum
                sell_order     = LimitOrder('SELL', sell_qty, limit_sell_price)
                sell_order.tif = 'DAY'
                order_desc     = f"LMT {fmt(limit_sell_price)} (no bid, last={'N/A' if not has_last else fmt(eod_snap_last)})"
                logger.warning(f"   ⚠️  {ticker}: no bid at EOD — using limit sell: {order_desc}")

            sell_trade     = ib.placeOrder(pos['contract'], sell_order)
            logger.info(f"   📤 {ticker}: {sell_qty} shares {order_desc} (tracked: {pos['shares']})")

            deadline = time.monotonic() + 15
            while not sell_trade.isDone() and time.monotonic() < deadline:
                ib.sleep(0.5)

            if sell_trade.orderStatus.status == 'Filled':
                exit_price = sell_trade.orderStatus.avgFillPrice
                exit_time  = datetime.now()
                net_pnl, net_pnl_pct, commission = calc_pnl(
                    pos['entry_price'], exit_price, sell_qty)
                hold_sec = (exit_time - pos['entry_time']).total_seconds()

                with stats_lock:
                    stats['total_pnl']        += net_pnl
                    stats['positions_closed'] += 1

                logger.info(
                    f"   ✅ {ticker} @ {fmt(exit_price)}  "
                    f"P&L: ${net_pnl:+.2f} ({pct(net_pnl_pct)}, net of commissions)"
                )
                log_trade(ticker, 'SELL', sell_qty, exit_price, exit_time)
                log_detailed_trade(
                    ticker=ticker, action='EOD_CLOSE', shares=sell_qty,
                    entry_price=pos['entry_price'], exit_price=exit_price,
                    entry_time=pos['entry_time'],   exit_time=exit_time,
                    alert_price=pos['alert_price'],
                    pnl=net_pnl, pnl_pct=net_pnl_pct, hold_time_sec=hold_sec,
                    snap_bid=eod_snap_bid, snap_ask=eod_snap_ask, snap_last=eod_snap_last,
                )
                # Log strategy performance for this trade
                strats = pos.get('strategies', [])
                if strats:
                    log_strategy_trade(
                        ticker=ticker, action='CLOSE', strategies=strats,
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
                        tags=pos.get('tags', ''),
                        zero_borrow=pos.get('zero_borrow', ''),
                        n_strategies=len(strats),
                    )
                # Only remove from tracking on successful sell
                with positions_lock:
                    active_positions.pop(ticker, None)
                    pending_tickers.discard(ticker)

            else:
                logger.error(
                    f"   ❌ {ticker} did not fill within 15s "
                    f"(status: {sell_trade.orderStatus.status}) – will retry"
                )
                log_failed_trade(ticker, 'EOD_SELL_TIMEOUT',
                                 sell_trade.orderStatus.status,
                                 snap_bid=eod_snap_bid, snap_ask=eod_snap_ask, snap_last=eod_snap_last)
                failed_tickers.append(ticker)
                # Position stays in active_positions for retry

        except (ConnectionError, OSError, asyncio.CancelledError,
                asyncio.TimeoutError) as e:
            logger.error(
                f"   ❌ EOD close connection error for {ticker}: {e} "
                f"– will retry"
            )
            log_failed_trade(ticker, 'EOD_SELL_DISCONNECT', str(e))
            failed_tickers.append(ticker)
            # Position stays in active_positions for retry

        except Exception as e:
            logger.error(f"   ❌ EOD close error for {ticker}: {e}")
            failed_tickers.append(ticker)
            # Position stays in active_positions for retry

    with positions_lock:
        save_positions_state()

    log_separator()
    logger.info(f"📊 Session P&L after EOD close: ${stats['total_pnl']:+.2f}")

    if failed_tickers:
        logger.warning(
            f"   ⚠️  {len(failed_tickers)} position(s) failed to close: "
            f"{', '.join(failed_tickers)} – will retry next loop"
        )
        # Do NOT set _eod_closed_today — let check_eod_close fire again
    else:
        _eod_closed_today = today

    log_separator()

# ============ ORDER PLACEMENT ============
def place_order(ticker, alert_price, alert_time, tags='', alert_reason='',
                sec_filing='', news='', recent_ipo='', recent_rs='', zero_borrow='',
                pct_change='', mcap='', float_shares='', min_vol='', rel_vol='', mon_vol=''):
    """Execute an IOC limit buy. No trailing stop – positions are held until
    EOD close at 15:55 ET. IBKR thread only."""
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

        # ── Request streaming quote for ask-based pricing ─────────────────
        # Uses the Streaming Bundle subscription — no per-request cost.
        # snapshot=False, regulatorySnapshot=False → streaming, $0.
        snap_bid = snap_ask = snap_last = float('nan')
        ticker_data = ib.reqMktData(contract, '', snapshot=False, regulatorySnapshot=False)

        try:
            # Poll for ask to populate (IB samples ~250ms; first tick arrives ~300-500ms)
            quote_deadline = time.monotonic() + QUOTE_WAIT_SEC
            while math.isnan(ticker_data.ask) and time.monotonic() < quote_deadline:
                ib.sleep(QUOTE_POLL_INTERVAL)

            snap_bid  = ticker_data.bid
            snap_ask  = ticker_data.ask
            snap_last = ticker_data.last

            # Determine if ask is usable for pricing
            usable_ask = (
                not math.isnan(snap_ask)
                and snap_ask > 0
                and snap_ask < alert_price * 10  # sanity: reject clearly erroneous data
            )

            if usable_ask:
                price_for_sizing = snap_ask
                limit_price = round(snap_ask * ASK_LIMIT_MULTIPLIER, 2)        # ASK_LIMIT_MULTIPLIER above current ask
                price_source = f"ask: {fmt(snap_ask)}"
            else:
                price_for_sizing = alert_price
                limit_price = round(alert_price * (1 + IOC_LIMIT_PCT / 100), 2)  # fallback 3%
                price_source = "ask: N/A"

            shares = max(1, int(TRADE_AMOUNT / price_for_sizing))

            log_separator("-")
            logger.info(f"📈 BUY: {shares} × {ticker}  @ IOC limit {fmt(limit_price)}  ({price_source}, alert: {fmt(alert_price)}"
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

            # ── Place buy order ────────────────────────────────────────────
            if USE_MARKET_ORDERS:
                buy_order = MarketOrder('BUY', shares)
                order_type_desc = "MKT"
            else:
                # IOC limit: fills at or below limit_price, cancels remainder.
                buy_order     = LimitOrder('BUY', shares, limit_price)
                buy_order.tif = 'IOC'
                order_type_desc = f"IOC limit {fmt(limit_price)}"
            buy_trade     = ib.placeOrder(contract, buy_order)
            order_id      = buy_trade.order.orderId

            with stats_lock:
                stats['orders_placed'] += 1

            logger.info(f"   Submitted (ID: {order_id}) – waiting for IOC result...")

            # IOC orders resolve very quickly (typically <2s). We give a generous
            # 10 s window to account for gateway latency.
            start = datetime.now()
            deadline = time.monotonic() + 10
            while not buy_trade.isDone() and time.monotonic() < deadline:
                ib.sleep(0.3)

                if reconnect_event.is_set() or not ib.isConnected():
                    logger.error(f"   ❌ Connection lost while waiting for {ticker} IOC – aborting")
                    log_failed_trade(ticker, 'CONNECTION_LOST',
                                     'Disconnected during IOC wait', order_id,
                                     snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    return

            # IOC outcome: fully filled, partially filled, or cancelled (no fill)
            fill_qty = int(buy_trade.orderStatus.filled)

            if fill_qty == 0:
                # No fill at all – price was above our limit.  This is normal IOC behavior.
                status = buy_trade.orderStatus.status
                logger.info(f"   ⏭️  No fill – price above IOC limit {fmt(limit_price)} (status: {status})")
                log_failed_trade(ticker, 'IOC_NO_FILL',
                                 f'Price exceeded limit {limit_price}', order_id,
                                 snap_bid=snap_bid, snap_ask=snap_ask, snap_last=snap_last)
                with stats_lock:
                    stats['orders_failed'] += 1
                with positions_lock:
                    pending_tickers.discard(ticker)
                return

            if fill_qty < shares:
                logger.info(f"   ⚠️  Partial IOC fill: {fill_qty}/{shares} shares")

            fill_price   = buy_trade.orderStatus.avgFillPrice
            fill_time    = datetime.now()
            slippage     = fill_price - alert_price
            slippage_pct = (slippage / alert_price) * 100

            with stats_lock:
                stats['orders_filled'] += 1

            logger.info(f"   ✅ Filled: {fill_qty} × {fmt(fill_price)}")
            logger.info(f"   Slippage: {fmt(slippage)}  ({pct(slippage_pct)})")
            logger.info(f"   📋 Holding until EOD close ({EOD_CLOSE_HOUR:02d}:{EOD_CLOSE_MINUTE:02d} ET)")

            # ── Match strategies ──────────────────────────────────────────────
            matched = match_strategies(
                alert_price=alert_price, pct_change=pct_change, mcap=mcap,
                float_shares=float_shares, rel_vol=rel_vol, tags=tags,
                zero_borrow=zero_borrow, entry_time=fill_time,
            )
            if matched:
                logger.info(f"   🎯 Strategies: {', '.join(matched)}")

            # ── Track position (no trailing stop) ────────────────────────────
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
                }
                save_positions_state()

            log_trade(ticker, 'BUY', fill_qty, fill_price, fill_time)
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
                    rel_vol=rel_vol, tags=tags, zero_borrow=zero_borrow,
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
        import traceback
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
        'timestamp'         : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
        'stop_percent'      : 'EOD_ONLY',
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
    logger.info(f"   Entry      : IOC limit (alert + {IOC_LIMIT_PCT}%)")
    logger.info(f"   Exit       : EOD only @ {EOD_CLOSE_HOUR:02d}:{EOD_CLOSE_MINUTE:02d} ET (no trailing stop)")
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

            # ── EOD close runs FIRST ─────────────────────────────────────────
            # This must happen before order processing so that a blocked
            # place_order call can never prevent EOD close from firing.
            check_eod_close()

            # ── Process one queued order ─────────────────────────────────────
            try:
                order_data = order_queue.get_nowait()
                ticker = order_data['ticker']

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
                        # Don't process – fall through to sleep
                        raise queue.Empty  # skip to next iteration

                # EOD cutoff gate: drop orders if we've passed the EOD cutoff
                # since the order was queued.
                if is_past_eod_cutoff():
                    logger.warning(
                        f"⚠️  {ticker}: past EOD cutoff at dequeue – dropping"
                    )
                    log_failed_trade(ticker, 'PAST_EOD_CUTOFF',
                                     'Past EOD cutoff when dequeued')
                    with positions_lock:
                        pending_tickers.discard(ticker)
                    raise queue.Empty  # skip to next iteration

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
                        ib.sleep(2)
            except queue.Empty:
                pass

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
            import traceback
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

    # SEC filings – anchored to "FORM" to prevent over-capturing
    sec_filing = ''
    m = re.search(
        r"Today'?s?\s*SEC\s*Fil(?:ing)?\s*[:\n]\s*((?:FORM[^\n]+\n?)+)",
        message_content, re.IGNORECASE
    )
    if m:
        sec_filing = ', '.join(
            f.strip() for f in m.group(1).strip().splitlines() if f.strip()
        )

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
        'timestamp'   : datetime.now(),
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
        logger.error("❌ DISCORD_TOKEN is not set – edit the config at the top of this file")
        exit(1)

    # Create CSV files with headers on first run
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
        ]),
    ]:
        if not os.path.exists(filepath):
            pd.DataFrame(columns=cols).to_csv(filepath, index=False)
            logger.info(f"Created {filepath}")

    try:
        client.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Discord error: {e}")
        graceful_shutdown()