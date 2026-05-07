#!/usr/bin/env python3
"""
Trade Inspector
===============
Pulls IBKR historical bars for trades in detailed_trades.csv and answers
analytical questions about execution: did the synthetic stop fire at the
right moment, was the stop fill better or worse than expected, would a
different exit have been better, etc.

Usage examples
--------------
Single-task (legacy):
    python trade_inspector.py --date 2026-04-21
    python trade_inspector.py --date 2026-04-21 --stops-only
    python trade_inspector.py --date 2026-04-21 --ticker EDBL

Multi-task batch (new) — all tasks run in one IBKR session, one report file:
    python trade_inspector.py \\
        --task 2026-04-22:stops \\
        --task 2026-04-23:stops \\
        --task 2026-04-24:stops \\
        --task 2026-04-21:EDBL \\
        --task 2026-04-22:LHSW

    SCOPE values:
      all     — every trade on the date (default if SCOPE omitted)
      stops   — BRACKET_SL only
      eods    — EOD_CLOSE only
      <SYM>   — specific ticker (e.g. EDBL, LHSW)

Output
------
Reports go to inspector_reports/inspector_YYYYMMDD-HHMMSS.txt by default.
Override with --output-dir or disable with --no-file.  Useful columns:
    MFE/MAE  = max favorable / adverse excursion in dollars at any point
               during the hold (what the P&L *could* have been)
    trigger  = first bar-time where price crossed entry × (1 + stop%/100)
    verdict  = one-line judgment on the trade:
                 TOO_EARLY  — stop fired then price reverted, lost on noise
                 SAVED      — stop fired and price kept running past fill
                 OK         — stop fired cleanly near trigger
                 MISSED_TP  — EOD exit but a big profit was available earlier
                 HELD_OK    — EOD exit and there wasn't a much better moment

Dependencies: ib_insync, pandas.  IBKR Gateway/TWS listening on port 4002.
Bars are cached under bars_cache_15s/ (shared with chart_trades.py).
"""

import argparse
import os
import sys
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd

try:
    from ib_insync import IB, Stock, util
    HAS_IB = True
except ImportError:
    HAS_IB = False


# ─── Config ──────────────────────────────────────────────────────────────
BARS_CACHE_DIR  = Path("bars_cache_15s")
TRADES_CSV      = "detailed_trades.csv"
REPORTS_DIR     = Path("inspector_reports")     # default output folder
IBKR_HOST       = "127.0.0.1"
IBKR_PORT       = 4002
IBKR_CLIENT_ID  = 97   # different from chart_trades.py so they can coexist

# Stop config mirrors the bot's config — kept here so the script is
# self-contained.  Override via --what-if-stop / --what-if-buffer.
SHORT_STOP_LOSS_PCT       = 20.0
SHORT_STOP_LIMIT_BUFFER   = 5.0     # limit = market × (1 + buffer/100)

# Window in minutes from entry within which a trigger crossing must occur
# for the stop to fire.  None = no time window (current bot behavior).
# Setting e.g. 15.0 means: only enforce the stop if price crosses trigger
# within 15 min of entry (catches fast-gap squeezes); otherwise hold to EOD.
SHORT_STOP_WINDOW_MIN: Optional[float] = None

# Reversion window for the TOO_EARLY check: if, after a stop fills, price
# falls back below entry within this many minutes, the stop probably fired
# on a wick and we'd have been better off holding.
REVERSION_WINDOW_MIN = 15

# CSV column layout (matches detailed_trades.csv header, with 'direction'
# as the implicit 36th column appended by the bot).
_CSV_COLS = [
    'timestamp', 'ticker', 'action', 'shares', 'entry_price', 'exit_price',
    'entry_time', 'exit_time', 'alert_price', 'slippage', 'slippage_pct',
    'execution_time_sec', 'pnl', 'pnl_pct', 'hold_time_sec', 'hold_time_min',
    'stop_percent', 'order_id', 'stop_order_id', 'pct_change', 'mcap',
    'float_shares', 'min_vol', 'rel_vol', 'mon_vol', 'tags', 'alert_reason',
    'sec_filing', 'news', 'recent_ipo', 'recent_rs', 'zero_borrow',
    'snap_bid', 'snap_ask', 'snap_last', 'direction',
]

# Global IB handle (lazy-connected).
_ib: Optional["IB"] = None
_ib_unavailable_logged = False  # set after first failed connect to suppress repeats


# ─── IBKR connection ─────────────────────────────────────────────────────
def get_ib() -> Optional["IB"]:
    """Lazy-connect to IBKR gateway on first use."""
    global _ib, _ib_unavailable_logged
    if _ib is not None and _ib.isConnected():
        return _ib
    if _ib_unavailable_logged:
        # Already tried and failed; don't spam the log
        return None
    if not HAS_IB:
        print("  ⚠️  ib_insync not installed – cached bars only", file=sys.stderr)
        _ib_unavailable_logged = True
        return None
    _ib = IB()
    try:
        _ib.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID, timeout=15)
        print(f"  ✓ Connected to IBKR on port {IBKR_PORT}")
        return _ib
    except Exception as e:
        print(f"  ⚠️  IBKR connect failed: {e} – cached bars only", file=sys.stderr)
        _ib = None
        _ib_unavailable_logged = True
        return None


# ─── Bar fetching (with cache) ───────────────────────────────────────────
def _fetch_from_ibkr(ticker: str, date: str) -> Optional[pd.DataFrame]:
    """Fetch 15-sec bars from IBKR and cache them.  Two 8-hour chunks
    to stay under IBKR's ~2000-bar-per-request limit."""
    ib = get_ib()
    if ib is None:
        return None
    contract = Stock(ticker, "SMART", "USD")
    try:
        if not ib.qualifyContracts(contract):
            print(f"  ✗ {ticker}: could not qualify")
            return None
    except Exception as e:
        print(f"  ✗ {ticker}: qualify error: {e}")
        return None

    end_1 = datetime.strptime(date, "%Y-%m-%d").replace(hour=12, minute=0)
    end_2 = datetime.strptime(date, "%Y-%m-%d").replace(hour=20, minute=0)

    dfs = []
    for end_dt in (end_1, end_2):
        try:
            bars = ib.reqHistoricalData(
                contract,
                endDateTime   = end_dt.strftime("%Y%m%d-%H:%M:%S"),
                durationStr   = "28800 S",
                barSizeSetting= "15 secs",
                whatToShow    = "TRADES",
                useRTH        = False,
                formatDate    = 1,
                timeout       = 60,
            )
        except Exception as e:
            print(f"  ✗ {ticker}: histdata error: {e}")
            bars = []
        if bars:
            ch = util.df(bars)
            ch["date"] = pd.to_datetime(ch["date"])
            ch = ch.set_index("date")[["open", "high", "low", "close", "volume"]]
            dfs.append(ch)
        _time.sleep(0.5)

    if not dfs:
        return None

    df = pd.concat(dfs)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    df = df[df.index.strftime("%Y-%m-%d") == date]

    BARS_CACHE_DIR.mkdir(exist_ok=True)
    df.to_csv(BARS_CACHE_DIR / f"{ticker}_{date}.csv")
    print(f"  ✓ {ticker} {date}: {len(df)} bars fetched")
    return df


def load_bars(ticker: str, date: str) -> Optional[pd.DataFrame]:
    """Load cached 15-sec bars, fetching from IBKR on miss."""
    cp = BARS_CACHE_DIR / f"{ticker}_{date}.csv"
    if cp.exists():
        try:
            df = pd.read_csv(cp, parse_dates=["date"], index_col="date")
            if len(df) > 0:
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                return df
        except (ValueError, pd.errors.EmptyDataError):
            pass  # fall through to refetch
    return _fetch_from_ibkr(ticker, date)


# ─── Trade model ─────────────────────────────────────────────────────────
@dataclass
class Trade:
    """Single round-trip: one OPEN row matched with one close row."""
    ticker       : str
    direction    : str       # 'SHORT' or 'LONG'
    entry_time   : datetime
    entry_price  : float
    exit_time    : datetime
    exit_price   : float
    shares       : int
    pnl          : float
    pnl_pct      : float
    action       : str       # 'BRACKET_SL', 'EOD_CLOSE', etc.
    stop_percent : str       # 'SL:20%' etc. — what the bot had configured
    alert_price  : Optional[float] = None
    snap_bid     : Optional[float] = None
    snap_ask     : Optional[float] = None

    @property
    def hold_min(self) -> float:
        return (self.exit_time - self.entry_time).total_seconds() / 60

    @property
    def stop_trigger(self) -> Optional[float]:
        """Configured stop trigger price, or None if no stop configured."""
        if self.direction == 'SHORT':
            return self.entry_price * (1 + SHORT_STOP_LOSS_PCT / 100)
        return None

    @property
    def move_pct(self) -> float:
        """Signed % move entry→exit.  Positive for SHORT = against us."""
        if self.direction == 'SHORT':
            return (self.exit_price - self.entry_price) / self.entry_price * 100
        return (self.exit_price - self.entry_price) / self.entry_price * 100


def _to_float(x) -> Optional[float]:
    try:
        v = float(x)
        if pd.isna(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def load_trades(date: str, ticker_filter: Optional[str] = None) -> List[Trade]:
    """Read detailed_trades.csv and return one Trade per round-trip on
    the given date.  Pairs OPEN rows with their close rows in ticker+time
    order — same logic the bot uses when matching."""
    df = pd.read_csv(TRADES_CSV, names=_CSV_COLS, skiprows=1)
    df['log_date'] = pd.to_datetime(df['timestamp']).dt.date
    target_date = pd.to_datetime(date).date()
    day = df[df['log_date'] == target_date].copy()
    if ticker_filter:
        day = day[day['ticker'] == ticker_filter]

    opens  = day[day['action'] == 'OPEN'].copy()
    closes = day[day['action'] != 'OPEN'].copy()

    trades: List[Trade] = []
    for ticker, og in opens.groupby('ticker'):
        og = og.sort_values('entry_time').reset_index(drop=True)
        cg = closes[closes.ticker == ticker].sort_values('exit_time').reset_index(drop=True)
        for i in range(min(len(og), len(cg))):
            o, c = og.iloc[i], cg.iloc[i]
            entry_price = _to_float(o['entry_price'])
            exit_price  = _to_float(c['exit_price'])
            if entry_price is None or exit_price is None:
                continue
            trades.append(Trade(
                ticker       = ticker,
                direction    = o['direction'],
                entry_time   = pd.to_datetime(o['entry_time']).to_pydatetime(),
                entry_price  = entry_price,
                exit_time    = pd.to_datetime(c['exit_time']).to_pydatetime(),
                exit_price   = exit_price,
                shares       = int(c['shares']),
                pnl          = _to_float(c['pnl']) or 0.0,
                pnl_pct      = _to_float(c['pnl_pct']) or 0.0,
                action       = c['action'],
                stop_percent = str(c.get('stop_percent', '')),
                alert_price  = _to_float(o.get('alert_price')),
                snap_bid     = _to_float(o.get('snap_bid')),
                snap_ask     = _to_float(o.get('snap_ask')),
            ))
    return trades


# ─── Analysis primitives ─────────────────────────────────────────────────
def bars_during_hold(bars: pd.DataFrame, t: Trade,
                     extend_after_min: int = 0) -> pd.DataFrame:
    """Return the slice of bars covering the hold window, optionally
    extended past the exit by `extend_after_min` minutes (used by the
    reversion check to see what would have happened if we'd held)."""
    end = t.exit_time + timedelta(minutes=extend_after_min)
    mask = (bars.index >= t.entry_time) & (bars.index <= end)
    return bars[mask]


def mfe_mae(bars: pd.DataFrame, t: Trade) -> dict:
    """Max favorable and max adverse excursion during the hold, in $.
    For SHORT: favorable = price drops, adverse = price rises.
    For LONG: reversed."""
    w = bars_during_hold(bars, t)
    if w.empty:
        return {'mfe_price': None, 'mae_price': None,
                'mfe_time': None, 'mae_time': None,
                'mfe_dollars': None, 'mae_dollars': None}
    if t.direction == 'SHORT':
        # Favorable = lowest low (we buy to cover cheap)
        mfe_idx = w['low'].idxmin()
        mae_idx = w['high'].idxmax()
        mfe_price = w.loc[mfe_idx, 'low']
        mae_price = w.loc[mae_idx, 'high']
        mfe_dollars = (t.entry_price - mfe_price) * t.shares
        mae_dollars = (t.entry_price - mae_price) * t.shares
    else:
        mfe_idx = w['high'].idxmax()
        mae_idx = w['low'].idxmin()
        mfe_price = w.loc[mfe_idx, 'high']
        mae_price = w.loc[mae_idx, 'low']
        mfe_dollars = (mfe_price - t.entry_price) * t.shares
        mae_dollars = (mae_price - t.entry_price) * t.shares
    return {
        'mfe_price'  : float(mfe_price),   'mae_price': float(mae_price),
        'mfe_time'   : mfe_idx,            'mae_time' : mae_idx,
        'mfe_dollars': float(mfe_dollars), 'mae_dollars': float(mae_dollars),
    }


def find_trigger_crossing(bars: pd.DataFrame, t: Trade) -> Optional[pd.Timestamp]:
    """First bar timestamp (entry_time onwards) where the high
    reached/crossed the stop trigger for SHORT trades.  None for LONG
    or if never crossed."""
    if t.direction != 'SHORT' or t.stop_trigger is None:
        return None
    w = bars_during_hold(bars, t)
    if w.empty:
        return None
    hit = w[w['high'] >= t.stop_trigger]
    if hit.empty:
        return None
    return hit.index[0]


def reversion_after_stop(bars: pd.DataFrame, t: Trade,
                          window_min: int = REVERSION_WINDOW_MIN) -> Optional[float]:
    """After a stop fill, how low did price get (for SHORT) within
    `window_min` minutes?  Returns that low price or None.  Used to
    detect stops that fired on a wick and immediately reverted — the
    TOO_EARLY case.  Compares against entry_price to decide if the
    revert was "significant"."""
    if t.direction != 'SHORT':
        return None
    end = t.exit_time + timedelta(minutes=window_min)
    w = bars[(bars.index >= t.exit_time) & (bars.index <= end)]
    if w.empty:
        return None
    return float(w['low'].min())


def counterfactual_pnl(bars: pd.DataFrame, t: Trade,
                       exit_at: str = 'eod') -> Optional[float]:
    """What would P&L be if we'd exited at some other moment?
    exit_at='eod'  → close at 15:45 ET
    exit_at='min'  → exit at MFE (best case for this trade)
    exit_at='max'  → exit at MAE (worst case)"""
    if exit_at == 'eod':
        eod_time = t.exit_time.replace(hour=15, minute=45, second=0, microsecond=0)
        w = bars[bars.index <= eod_time]
        if w.empty:
            return None
        exit_px = float(w.iloc[-1]['close'])
    else:
        w = bars_during_hold(bars, t)
        if w.empty:
            return None
        if exit_at == 'min':
            exit_px = float(w['low'].min() if t.direction == 'SHORT' else w['high'].max())
        elif exit_at == 'max':
            exit_px = float(w['high'].max() if t.direction == 'SHORT' else w['low'].min())
        else:
            raise ValueError(f"bad exit_at: {exit_at}")
    if t.direction == 'SHORT':
        return (t.entry_price - exit_px) * t.shares
    return (exit_px - t.entry_price) * t.shares


# ─── Per-trade reports ───────────────────────────────────────────────────
def _fmt_price(p) -> str:
    if p is None: return 'n/a'
    return f'${p:.4f}' if p < 10 else f'${p:.2f}'


def _fmt_time(t) -> str:
    if t is None: return 'n/a'
    return pd.Timestamp(t).strftime('%H:%M:%S')


def inspect_stop(t: Trade, bars: pd.DataFrame) -> str:
    """Detailed analysis of a BRACKET_SL trade.  Answers: did the
    stop fire at the right moment?  Was the fill price reasonable
    given the trigger?  Would holding have done better?"""
    lines = [
        f"\n{'='*68}",
        f"{t.ticker} @ {t.entry_time.date()} — {t.direction} (BRACKET_SL)",
        f"{'='*68}",
        f"  Entry       : {_fmt_time(t.entry_time)} @ {_fmt_price(t.entry_price)} ({t.shares} sh)",
        f"  Exit (stop) : {_fmt_time(t.exit_time)} @ {_fmt_price(t.exit_price)}  ({t.move_pct:+.1f}%)",
        f"  Hold        : {t.hold_min:.1f} min",
        f"  P&L         : ${t.pnl:+.2f}",
    ]
    if t.stop_trigger:
        lines.append(f"  Stop config : trigger={_fmt_price(t.stop_trigger)}  (+{SHORT_STOP_LOSS_PCT}%)")

    if bars is None or bars.empty:
        lines.append("  ⚠️  No bars available — skipping intraday analysis.")
        return "\n".join(lines)

    # Trigger crossing
    trig = find_trigger_crossing(bars, t)
    if trig is None:
        lines.append("  ⚠️  Price never crossed configured trigger in bars "
                     "(bot may have used stale marketPrice).")
    else:
        lag = (t.exit_time - trig).total_seconds()
        # Bar at trigger crossing
        trig_bar = bars.loc[trig]
        lines.append(f"  Trigger hit : {_fmt_time(trig)} @ high {_fmt_price(trig_bar['high'])}  "
                     f"({lag:+.0f}s before fill)")

    # MFE/MAE
    mm = mfe_mae(bars, t)
    if mm['mfe_price'] is not None:
        lines.append(f"  MFE (low)   : {_fmt_price(mm['mfe_price'])} @ {_fmt_time(mm['mfe_time'])}  "
                     f"(${mm['mfe_dollars']:+.2f} if exited there)")
        lines.append(f"  MAE (high)  : {_fmt_price(mm['mae_price'])} @ {_fmt_time(mm['mae_time'])}  "
                     f"(${mm['mae_dollars']:+.2f} if exited there)")

    # Reversion check — did price fall back to/below entry shortly after stop?
    rev = reversion_after_stop(bars, t)
    if rev is not None:
        rev_pct = (rev - t.entry_price) / t.entry_price * 100
        lines.append(f"  Post-stop   : low {_fmt_price(rev)} within {REVERSION_WINDOW_MIN}min  "
                     f"({rev_pct:+.1f}% vs entry)")

    # Counterfactual: what if held to EOD?
    eod = counterfactual_pnl(bars, t, 'eod')
    if eod is not None:
        lines.append(f"  If held EOD : P&L ${eod:+.2f}  (actual stopped at ${t.pnl:+.2f})")

    # Time-window simulation: would a window-N rule have suppressed this stop?
    suppressed = False
    if SHORT_STOP_WINDOW_MIN is not None and t.direction == 'SHORT':
        # The window rule fires the stop only if trigger is crossed within
        # SHORT_STOP_WINDOW_MIN of entry.  If trig is None (price never
        # crossed in the bars), the stop wouldn't have fired either, so
        # it's also suppressed.
        if trig is None:
            suppressed = True
            window_lag_min = None
        else:
            window_lag_min = (trig - pd.Timestamp(t.entry_time)).total_seconds() / 60
            suppressed = window_lag_min > SHORT_STOP_WINDOW_MIN

        rule_label = f"≤{SHORT_STOP_WINDOW_MIN:g}min window"
        if suppressed:
            tag = ('trigger never crossed' if window_lag_min is None
                   else f'trigger hit at +{window_lag_min:.1f}min')
            sim_pnl = eod if eod is not None else t.pnl
            lines.append(
                f"  Window sim  : {rule_label} → SUPPRESSED  ({tag})  "
                f"→ would hold to EOD, P&L ${sim_pnl:+.2f}  "
                f"(Δ vs actual stop ${sim_pnl - t.pnl:+.2f})"
            )
        else:
            lines.append(
                f"  Window sim  : {rule_label} → STOP FIRES  "
                f"(trigger hit at +{window_lag_min:.1f}min)  "
                f"→ same outcome ${t.pnl:+.2f}"
            )

    # Verdict
    verdict = _stop_verdict(t, bars, mm, rev, eod, suppressed=suppressed)
    lines.append(f"  VERDICT     : {verdict}")
    return "\n".join(lines)


def _stop_verdict(t: Trade, bars: pd.DataFrame, mm: dict,
                  rev: Optional[float], eod: Optional[float],
                  suppressed: bool = False) -> str:
    """One-line judgment on a stop fire.  Heuristics:
      SUPPRESSED  — time-window rule would have skipped this stop.  The
                    sub-tag tells you whether that was the right call.
      OVERSHOOT   — stop filled significantly past trigger (≥ 2×buffer),
                    usually pre-market gap.  Verdict on execution, not signal.
      SAVED       — EOD counterfactual is materially WORSE than the stop fill
                    (>$10 worse).  Stop prevented a larger loss.  This is the
                    case the stop is meant to catch.
      TOO_EARLY   — EOD counterfactual is materially BETTER than the stop fill
                    (>$10 better).  Stop fired on what turned out to be noise.
      OK          — Stop fill within ±$10 of EOD outcome.

    EOD-based SAVED/TOO_EARLY take precedence over the reversion-window
    heuristic (rev <= entry_price): the EOD counterfactual is the actual
    decision we're trying to evaluate.  The reversion test is only used as a
    fallback when EOD data is unavailable.
    """
    if suppressed:
        # Sub-tag: was suppression the right call?
        if eod is None:
            return 'SUPPRESSED (EOD price unavailable)'
        delta = eod - t.pnl
        if delta > 10:
            return f'SUPPRESSED_GOOD (window rule saved ${delta:+.2f}: stop ${t.pnl:+.2f}, EOD ${eod:+.2f})'
        if delta < -10:
            return f'SUPPRESSED_BAD (window rule cost ${delta:+.2f}: stop ${t.pnl:+.2f}, EOD ${eod:+.2f})'
        return f'SUPPRESSED_NEUTRAL (Δ ${delta:+.2f} vs actual stop)'

    if t.stop_trigger is None:
        return 'OK'

    # Execution-quality flag: fast-gap fills past the limit buffer.  Surfaced
    # as a *prefix* on the verdict when material; otherwise just used as
    # context in the OK case.
    overshoot_pct = (t.exit_price - t.stop_trigger) / t.stop_trigger * 100
    is_overshoot = overshoot_pct > 2 * SHORT_STOP_LIMIT_BUFFER

    # EOD counterfactual decides SAVED vs TOO_EARLY vs OK.  This is the
    # primary signal — what would have happened if we'd held to 15:45?
    if eod is not None:
        delta = eod - t.pnl
        if delta < -10:
            tag = 'OVERSHOOT_SAVED' if is_overshoot else 'SAVED'
            return f'{tag} (stop ${t.pnl:+.2f} vs EOD ${eod:+.2f}, saved ${-delta:.2f})'
        if delta > 10:
            return f'TOO_EARLY (stop ${t.pnl:+.2f} vs EOD ${eod:+.2f}, cost ${delta:.2f})'
        # Neutral zone — small ±$10 difference, decide by fill quality
        if is_overshoot:
            return f'OVERSHOOT (+{overshoot_pct:.1f}% past trigger; EOD Δ ${delta:+.2f})'
        return f'OK (fill {overshoot_pct:+.1f}% past trigger; EOD Δ ${delta:+.2f})'

    # Fallback: no EOD data, use the original reversion-window heuristic
    if is_overshoot:
        return f'OVERSHOOT (+{overshoot_pct:.1f}% past trigger — likely fast gap)'
    if rev is not None and rev <= t.entry_price:
        return 'TOO_EARLY (reverted below entry; EOD price unavailable)'
    if rev is not None and rev > t.exit_price:
        return 'SAVED (price kept climbing after stop; EOD price unavailable)'
    return f'OK (fill {overshoot_pct:+.1f}% past trigger)'


def inspect_eod(t: Trade, bars: pd.DataFrame) -> str:
    """EOD_CLOSE analysis: was EOD the right moment, or would an earlier
    take-profit have captured more?"""
    lines = [
        f"\n{'='*68}",
        f"{t.ticker} @ {t.entry_time.date()} — {t.direction} (EOD_CLOSE)",
        f"{'='*68}",
        f"  Entry       : {_fmt_time(t.entry_time)} @ {_fmt_price(t.entry_price)} ({t.shares} sh)",
        f"  Exit (EOD)  : {_fmt_time(t.exit_time)} @ {_fmt_price(t.exit_price)}  ({t.move_pct:+.1f}%)",
        f"  P&L         : ${t.pnl:+.2f}",
    ]
    if bars is None or bars.empty:
        lines.append("  ⚠️  No bars available.")
        return "\n".join(lines)

    mm = mfe_mae(bars, t)
    if mm['mfe_price'] is None:
        return "\n".join(lines)

    best   = mm['mfe_dollars']   # best-case P&L if exited at MFE
    worst  = mm['mae_dollars']   # worst-case
    lines.append(f"  MFE         : {_fmt_price(mm['mfe_price'])} @ {_fmt_time(mm['mfe_time'])}  "
                 f"(${best:+.2f} if exited there)")
    lines.append(f"  MAE         : {_fmt_price(mm['mae_price'])} @ {_fmt_time(mm['mae_time'])}  "
                 f"(${worst:+.2f} if exited there)")

    # Verdict: was there an obviously better exit?
    missed = best - t.pnl
    if missed > 20:
        verdict = f'MISSED_TP (${missed:.2f} left on the table — MFE was {_fmt_time(mm["mfe_time"])})'
    elif worst < -30:
        verdict = f'SCARY_RIDE (MAE ${worst:+.2f} — held through drawdown, lucky recovery)'
    else:
        verdict = 'HELD_OK'
    lines.append(f"  VERDICT     : {verdict}")
    return "\n".join(lines)


# ─── Daily summary ───────────────────────────────────────────────────────
def _verdict_word(text: str) -> str:
    """Extract just the leading verdict word (TOO_EARLY, SAVED, etc.) from
    the full verdict string for aggregation."""
    if 'VERDICT' not in text:
        return 'OK'
    after = text.split('VERDICT')[-1].split(':', 1)[-1].strip()
    return after.split(' ')[0] if after else 'OK'


def summarize_task(label: str, trades: List[Trade], reports: List[dict]) -> str:
    """High-level totals for one task (one date+scope)."""
    n = len(trades)
    if n == 0:
        return f"\n(no trades to summarize for {label})\n"
    actual_pnl = sum(t.pnl for t in trades)
    best_case  = sum((r.get('best_case')  or t.pnl) for t, r in zip(trades, reports))
    worst_case = sum((r.get('worst_case') or t.pnl) for t, r in zip(trades, reports))

    verdict_counts: dict = {}
    for r in reports:
        v = r.get('verdict', 'OK')
        verdict_counts[v] = verdict_counts.get(v, 0) + 1

    lines = [
        "",
        "-"*68,
        f"TASK SUMMARY — {label}",
        "-"*68,
        f"  Trades analyzed : {n}",
        f"  Actual P&L      : ${actual_pnl:+.2f}",
    ]
    # If a window rule is active, surface the simulated P&L
    if SHORT_STOP_WINDOW_MIN is not None:
        sim_pnl = sum(r.get('sim_pnl', t.pnl) for t, r in zip(trades, reports))
        n_suppressed = sum(1 for r in reports if 'SUPPRESSED' in r.get('verdict', ''))
        lines.append(
            f"  Sim. P&L (≤{SHORT_STOP_WINDOW_MIN:g}min) : ${sim_pnl:+.2f}  "
            f"(Δ ${sim_pnl - actual_pnl:+.2f}, {n_suppressed}/{n} stops suppressed)"
        )
    lines += [
        f"  If-perfect MFE  : ${best_case:+.2f}  (upper bound — exit at each trade's MFE)",
        f"  If-cursed  MAE  : ${worst_case:+.2f}  (lower bound — exit at each trade's MAE)",
        f"  Verdicts        : " + ", ".join(f"{k}={v}" for k, v in sorted(verdict_counts.items())),
    ]
    return "\n".join(lines) + "\n"


# ─── Tasks ───────────────────────────────────────────────────────────────
@dataclass
class Task:
    """One analysis unit: a date plus what subset of trades to inspect."""
    date  : str
    scope : str = 'all'   # 'all' | 'stops' | 'eods' | <ticker symbol>

    @property
    def label(self) -> str:
        return f"{self.date} :: {self.scope}"

    def filter_trades(self, trades: List[Trade]) -> List[Trade]:
        if self.scope == 'all':
            return trades
        if self.scope == 'stops':
            return [t for t in trades if t.action == 'BRACKET_SL']
        if self.scope == 'eods':
            return [t for t in trades if t.action == 'EOD_CLOSE']
        # Otherwise: a ticker symbol
        return [t for t in trades if t.ticker == self.scope]


def parse_task(spec: str) -> Task:
    """Parse 'DATE' or 'DATE:SCOPE'.  SCOPE is 'all', 'stops', 'eods', or
    a ticker symbol.  Symbol detection: anything that's not the three known
    keywords is treated as a ticker."""
    if ':' in spec:
        date, scope = spec.split(':', 1)
    else:
        date, scope = spec, 'all'
    date = date.strip()
    scope = scope.strip()
    # Validate date (will raise if malformed)
    datetime.strptime(date, '%Y-%m-%d')
    if not scope:
        scope = 'all'
    # Normalize known keywords; otherwise leave as ticker (uppercased)
    if scope.lower() in ('all', 'stops', 'eods'):
        scope = scope.lower()
    else:
        scope = scope.upper()
    return Task(date=date, scope=scope)


def run_task(task: Task,
             bars_cache_local: dict) -> tuple[str, List[Trade], List[dict]]:
    """Execute one Task and return (report_text, trades, per-trade meta).
    bars_cache_local is shared across tasks so the same (ticker, date)
    isn't fetched twice."""
    # Trades for this date, then filtered by scope
    if task.scope not in ('all', 'stops', 'eods'):
        # Specific ticker — narrow CSV read at the source for speed
        trades = load_trades(task.date, ticker_filter=task.scope)
    else:
        trades = load_trades(task.date)
    trades = task.filter_trades(trades)

    if not trades:
        return (f"\n[{task.label}]  No trades matched.\n", [], [])

    out: List[str] = []
    out.append(f"\n{'#'*68}")
    out.append(f"#  TASK: {task.label}  ({len(trades)} trade{'s' if len(trades)!=1 else ''})")
    out.append(f"{'#'*68}")

    # Fetch bars (with shared cache across tasks)
    needed = sorted({(t.ticker, task.date) for t in trades})
    for ticker, date in needed:
        key = (ticker, date)
        if key not in bars_cache_local:
            bars_cache_local[key] = load_bars(ticker, date)

    reports: List[dict] = []
    for t in trades:
        bars = bars_cache_local.get((t.ticker, task.date))
        txt = inspect_stop(t, bars) if t.action == 'BRACKET_SL' else inspect_eod(t, bars)
        out.append(txt)

        meta: dict = {}
        if bars is not None and not bars.empty:
            mm = mfe_mae(bars, t)
            if mm['mfe_price'] is not None:
                if t.direction == 'SHORT':
                    meta['best_case']  = (t.entry_price - mm['mfe_price']) * t.shares
                    meta['worst_case'] = (t.entry_price - mm['mae_price']) * t.shares
                else:
                    meta['best_case']  = (mm['mfe_price'] - t.entry_price) * t.shares
                    meta['worst_case'] = (mm['mae_price'] - t.entry_price) * t.shares

            # Window-rule simulation: did this trade get suppressed?  If yes,
            # the simulated P&L is the EOD price; otherwise it's the actual P&L.
            if (SHORT_STOP_WINDOW_MIN is not None
                    and t.direction == 'SHORT'
                    and t.action == 'BRACKET_SL'):
                trig = find_trigger_crossing(bars, t)
                if trig is None:
                    suppressed = True
                    lag_min = None
                else:
                    lag_min = (trig - pd.Timestamp(t.entry_time)).total_seconds() / 60
                    suppressed = lag_min > SHORT_STOP_WINDOW_MIN
                if suppressed:
                    eod = counterfactual_pnl(bars, t, 'eod')
                    meta['sim_pnl'] = eod if eod is not None else t.pnl
                else:
                    meta['sim_pnl'] = t.pnl

        meta['verdict'] = _verdict_word(txt)
        reports.append(meta)

    out.append(summarize_task(task.label, trades, reports))
    return ("\n".join(out), trades, reports)


# ─── Orchestration ───────────────────────────────────────────────────────
def run_tasks(tasks: List[Task],
              what_if_stop: Optional[float] = None,
              what_if_buffer: Optional[float] = None,
              what_if_window_min: Optional[float] = None,
              output_dir: Optional[Path] = REPORTS_DIR,
              quiet: bool = False) -> Optional[Path]:
    """Run a batch of tasks.  Connects to IBKR once, dedupes bar fetches
    across tasks, writes a combined report to a single file, and returns
    the path written (or None if --no-file was set via output_dir=None)."""

    global SHORT_STOP_LOSS_PCT, SHORT_STOP_LIMIT_BUFFER, SHORT_STOP_WINDOW_MIN
    if what_if_stop is not None:
        SHORT_STOP_LOSS_PCT = what_if_stop
    if what_if_buffer is not None:
        SHORT_STOP_LIMIT_BUFFER = what_if_buffer
    if what_if_window_min is not None:
        SHORT_STOP_WINDOW_MIN = what_if_window_min

    # Header for the report file
    header_lines = [
        "="*68,
        f"Trade Inspector report — generated {datetime.now():%Y-%m-%d %H:%M:%S}",
        "="*68,
        f"Tasks ({len(tasks)}):",
    ]
    for i, task in enumerate(tasks, 1):
        header_lines.append(f"  {i}. {task.label}")
    if what_if_stop is not None:
        header_lines.append(f"What-if stop trigger : {what_if_stop}%")
    if what_if_buffer is not None:
        header_lines.append(f"What-if limit buffer : {what_if_buffer}%")
    if what_if_window_min is not None:
        header_lines.append(
            f"What-if stop window  : ≤{what_if_window_min:g} min from entry "
            f"(stops outside window are suppressed → hold to EOD)"
        )
    header_lines.append("")

    sections: List[str] = []
    all_trades: List[Trade] = []
    all_reports: List[dict] = []
    bars_cache_local: dict = {}   # shared across tasks

    for task in tasks:
        if not quiet:
            print(f"  → running {task.label} …")
        section, trades, reports = run_task(task, bars_cache_local)
        sections.append(section)
        all_trades.extend(trades)
        all_reports.extend(reports)

    # Cross-task summary at the top
    overall = ["", "="*68, "OVERALL SUMMARY (across all tasks)", "="*68]
    if all_trades:
        n = len(all_trades)
        actual_pnl = sum(t.pnl for t in all_trades)
        best  = sum((r.get('best_case')  or t.pnl) for t, r in zip(all_trades, all_reports))
        worst = sum((r.get('worst_case') or t.pnl) for t, r in zip(all_trades, all_reports))
        verdict_counts: dict = {}
        for r in all_reports:
            v = r.get('verdict', 'OK')
            verdict_counts[v] = verdict_counts.get(v, 0) + 1
        # Note: trades may overlap across tasks (e.g. stops-task on Apr 22
        # plus ticker-task on Apr 22:LHSW will count LHSW twice).  This is
        # fine for verdict counting but the P&L total double-counts in that
        # case — we flag it.
        overall += [
            f"  Trades counted   : {n}  (note: overlapping tasks may double-count)",
            f"  Sum actual P&L   : ${actual_pnl:+.2f}",
        ]
        if SHORT_STOP_WINDOW_MIN is not None:
            sim_total = sum(r.get('sim_pnl', t.pnl) for t, r in zip(all_trades, all_reports))
            n_suppressed = sum(1 for r in all_reports if 'SUPPRESSED' in r.get('verdict', ''))
            overall.append(
                f"  Sim. P&L (≤{SHORT_STOP_WINDOW_MIN:g}min) : ${sim_total:+.2f}  "
                f"(Δ ${sim_total - actual_pnl:+.2f}, {n_suppressed} stops suppressed)"
            )
        overall += [
            f"  Sum if-perfect   : ${best:+.2f}",
            f"  Sum if-cursed    : ${worst:+.2f}",
            f"  Verdict tally    : " + ", ".join(f"{k}={v}" for k, v in sorted(verdict_counts.items())),
        ]
    else:
        overall.append("  (no trades analyzed across any task)")

    full_report = "\n".join(header_lines + overall + [""] + sections)

    # Always print to stdout (the user can redirect if they want)
    if not quiet:
        print(full_report)

    # Write to file if output_dir is set
    out_path = None
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d-%H%M%S')
        out_path = output_dir / f"inspector_{ts}.txt"
        out_path.write_text(full_report)
        if not quiet:
            print(f"\n✓ Report written to {out_path}")

    if _ib is not None and _ib.isConnected():
        _ib.disconnect()

    return out_path


# ─── CLI ─────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    # Multi-task batch interface
    p.add_argument('--task', action='append', default=[], metavar='DATE[:SCOPE]',
                   help="Add an analysis task.  SCOPE = all|stops|eods|<ticker>.  "
                        "Repeatable.  Examples: --task 2026-04-22:stops, "
                        "--task 2026-04-21:EDBL")
    # Legacy single-task interface (kept for backwards compatibility)
    p.add_argument('--date', default=None,
                   help="(legacy) Trading date to analyze (YYYY-MM-DD).  "
                        "Equivalent to --task DATE[:scope]")
    p.add_argument('--ticker', default=None,
                   help="(legacy) Restrict to this ticker — combined with --date")
    grp = p.add_mutually_exclusive_group()
    grp.add_argument('--stops-only', action='store_true',
                     help="(legacy) Only BRACKET_SL trades — combined with --date")
    grp.add_argument('--eods-only', action='store_true',
                     help="(legacy) Only EOD_CLOSE trades — combined with --date")
    # What-ifs
    p.add_argument('--what-if-stop', type=float, default=None,
                   help="Override stop trigger %% for this run (e.g. 10 for tighter)")
    p.add_argument('--what-if-buffer', type=float, default=None,
                   help="Override BUY-limit buffer %% for this run")
    p.add_argument('--what-if-stop-window-min', type=float, default=None,
                   metavar='MIN',
                   help="Simulate a time-window stop rule: only enforce the stop "
                        "if trigger is breached within MIN minutes of entry; "
                        "otherwise suppress the stop and hold to EOD.  "
                        "Recommended: 15.  Adds SUPPRESSED verdicts and a "
                        "Sim. P&L line to each summary.")
    # Output
    p.add_argument('--output-dir', default=str(REPORTS_DIR),
                   help=f"Folder for the combined report file (default: {REPORTS_DIR})")
    p.add_argument('--no-file', action='store_true',
                   help="Skip writing the report file; stdout only")
    p.add_argument('--quiet', action='store_true',
                   help="Suppress stdout (only useful with file output)")
    args = p.parse_args()

    # Build the task list
    tasks: List[Task] = []
    for spec in args.task:
        try:
            tasks.append(parse_task(spec))
        except ValueError as e:
            p.error(f"bad --task {spec!r}: {e}")

    # Legacy path: convert --date/--ticker/--stops-only/--eods-only into a Task
    if args.date:
        if args.ticker:
            scope = args.ticker.upper()
        elif args.stops_only:
            scope = 'stops'
        elif args.eods_only:
            scope = 'eods'
        else:
            scope = 'all'
        try:
            tasks.append(Task(date=args.date, scope=scope))
            datetime.strptime(args.date, '%Y-%m-%d')  # validate
        except ValueError as e:
            p.error(f"bad --date {args.date!r}: {e}")

    if not tasks:
        p.error("no tasks specified — use --task DATE[:SCOPE] (repeatable) "
                "or the legacy --date/--ticker/--stops-only/--eods-only flags")

    output_dir = None if args.no_file else Path(args.output_dir)
    run_tasks(tasks,
              what_if_stop=args.what_if_stop,
              what_if_buffer=args.what_if_buffer,
              what_if_window_min=args.what_if_stop_window_min,
              output_dir=output_dir,
              quiet=args.quiet)


if __name__ == '__main__':
    main()