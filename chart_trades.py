#!/usr/bin/env python3
"""
Alert vs Entry Chart Generator
===============================
For a given ticker+date (or all trades), plots the intraday price chart with markers
showing when the Discord alert fired and when the bot actually entered.

Uses cached 1-min bars from bars_cache/ (run exit_strategy_backtest.py first),
or fetches from IBKR if not cached.

Usage:
    # Single ticker
    python chart_trades.py --ticker LUNR --date 2026-04-09

    # All trades on a date
    python chart_trades.py --date 2026-04-10

    # All trades (generates one HTML per ticker-date)
    python chart_trades.py --all

    # Limit to filtered trades only
    python chart_trades.py --date 2026-04-10 --min-price 5 --min-monvol 100000 --min-hour 9
"""

import argparse
import csv
import os
import json
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd

try:
    from ib_insync import IB, Stock, util
    HAS_IB = True
except ImportError:
    HAS_IB = False

# ─── Config ───
BARS_CACHE_DIR = "bars_cache"
BARS_15S_CACHE_DIR = "bars_cache_15s"
CHANNEL_EXPORT = "channel_export_1467323184905850921.csv"
DETAILED_TRADES = "detailed_trades.csv"
OUTPUT_DIR = "trade_charts"

# Global IB connection (lazy-initialized)
_ib = None


def parse_money(s):
    if not s or s == "N/A":
        return None
    s = s.replace("$", "").replace(",", "")
    if s.endswith("K"): return float(s[:-1]) * 1e3
    if s.endswith("M"): return float(s[:-1]) * 1e6
    if s.endswith("B"): return float(s[:-1]) * 1e9
    try: return float(s)
    except: return None


def load_alerts(path):
    """Load Discord alerts, keyed by (ticker, date_ET)."""
    alerts = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            # Timestamp is UTC, convert to ET (UTC-4 during EDT)
            try:
                utc_dt = datetime.strptime(row["timestamp"].strip(), "%Y-%m-%d %H:%M:%S UTC")
            except:
                continue
            et_dt = utc_dt - timedelta(hours=4)
            date_str = et_dt.strftime("%Y-%m-%d")
            alerts[(row["ticker"], date_str)].append({
                "time": et_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "price": float(row["price"]) if row.get("price") else None,
                "alert_num": row.get("alert_num", ""),
                "pct_change": row.get("pct_change", ""),
            })
    return alerts


def load_trades(path):
    """Load trades, keyed by (ticker, date)."""
    trades = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            date = row["entry_time"][:10] if row.get("entry_time") else row["timestamp"][:10]
            trades[(row["ticker"], date)].append(row)
    return trades


def get_ib(port):
    """Lazy-connect to IBKR."""
    global _ib
    if _ib is not None and _ib.isConnected():
        return _ib
    if not HAS_IB:
        return None
    _ib = IB()
    try:
        _ib.connect("127.0.0.1", port, clientId=98)
        print(f"  Connected to IBKR on port {port}")
        return _ib
    except Exception as e:
        print(f"  Could not connect to IBKR: {e}")
        _ib = None
        return None


def fetch_bars_ibkr(ib, ticker, date, bars_cache_dir):
    """Fetch 1-min bars from IBKR and cache them."""
    contract = Stock(ticker, "SMART", "USD")
    try:
        if not ib.qualifyContracts(contract):
            print(f"  ✗ {ticker}: could not qualify contract")
            return None
    except Exception as e:
        print(f"  ✗ {ticker}: qualify error: {e}")
        return None

    # 15-sec bars: IBKR limits ~2000 bars per request, so fetch in two halves
    end_dt_1 = datetime.strptime(date, "%Y-%m-%d").replace(hour=12, minute=0)
    end_dt_2 = datetime.strptime(date, "%Y-%m-%d").replace(hour=20, minute=0)

    all_dfs = []
    for end_dt, dur in [(end_dt_1, "28800 S"), (end_dt_2, "28800 S")]:
        try:
            bars = ib.reqHistoricalData(
                contract, endDateTime=end_dt.strftime("%Y%m%d-%H:%M:%S"),
                durationStr=dur, barSizeSetting="15 secs",
                whatToShow="TRADES", useRTH=False, formatDate=1, timeout=60,
            )
        except Exception as e:
            print(f"  ✗ {ticker}: historical data error: {e}")
            bars = []
        if bars:
            chunk = util.df(bars)
            chunk["date"] = pd.to_datetime(chunk["date"])
            chunk = chunk.set_index("date")[["open", "high", "low", "close", "volume"]]
            all_dfs.append(chunk)
        import time as _time
        _time.sleep(0.5)  # rate limit between requests

    if not all_dfs:
        print(f"  ✗ {ticker} {date}: no bars returned")
        return None

    df = pd.concat(all_dfs)
    df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()
    df = df[df.index.strftime("%Y-%m-%d") == date]

    # Cache
    os.makedirs(bars_cache_dir, exist_ok=True)
    df.to_csv(os.path.join(bars_cache_dir, f"{ticker}_{date}.csv"))
    print(f"  ✓ {ticker} {date}: {len(df)} bars fetched and cached")
    return df


def load_bars(ticker, date, bars_cache_dir=BARS_15S_CACHE_DIR, port=4002):
    """Load cached bars, or fetch from IBKR if not cached."""
    cp = os.path.join(bars_cache_dir, f"{ticker}_{date}.csv")
    if os.path.exists(cp):
        try:
            df = pd.read_csv(cp, parse_dates=["date"], index_col="date")
        except (ValueError, pd.errors.EmptyDataError):
            df = pd.DataFrame()
        if len(df) > 0:
            if df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            return df

    # Try IBKR
    ib = get_ib(port)
    if ib is None:
        return None
    df = fetch_bars_ibkr(ib, ticker, date, bars_cache_dir)
    if df is not None and df.index.tz is not None:
        df.index = df.index.tz_localize(None)
    return df


def generate_chart_html(ticker, date, bars_df, alert_list, trade_list):
    """Generate a self-contained HTML chart for one ticker-date."""

    # Prepare OHLC data
    candles = []
    for ts, row in bars_df.iterrows():
        candles.append({
            "t": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "o": round(row["open"], 4),
            "h": round(row["high"], 4),
            "l": round(row["low"], 4),
            "c": round(row["close"], 4),
            "v": int(row["volume"]) if pd.notna(row["volume"]) else 0,
        })

    # Alert markers — plot every alert for this ticker on this day
    alert_markers = []
    for a in alert_list:
        alert_markers.append({
            "time": a["time"],
            "price": a["price"],
            "alert_num": a.get("alert_num", "?"),
            "label": f"Alert #{a.get('alert_num', '?')} ({a.get('pct_change', '')})",
        })

    # Trade markers
    # Detect short vs long: use direction column if present, else bid/ask proximity
    open_markers = []
    close_markers = []
    is_short = False
    for t in trade_list:
        if t["action"] == "OPEN":
            entry_price = float(t["entry_price"])

            # Check explicit direction column first
            if t.get("direction", "").upper() == "SHORT":
                is_short = True
            elif t.get("direction", "").upper() == "LONG":
                is_short = False
            else:
                # Fallback: infer from bid/ask proximity
                try:
                    snap_bid = float(t["snap_bid"]) if t.get("snap_bid") else None
                    snap_ask = float(t["snap_ask"]) if t.get("snap_ask") else None
                except (ValueError, TypeError):
                    snap_bid = snap_ask = None

                if snap_bid is not None and snap_ask is not None and snap_bid < snap_ask:
                    dist_bid = abs(entry_price - snap_bid)
                    dist_ask = abs(entry_price - snap_ask)
                    is_short = dist_bid <= dist_ask
                elif snap_bid is not None and snap_ask is None:
                    is_short = True

            entry_label = f"SHORT {t['shares']} @ ${entry_price:.4f}" if is_short else f"BUY {t['shares']} @ ${entry_price:.4f}"
            open_markers.append({
                "time": t["entry_time"],
                "price": entry_price,
                "shares": t["shares"],
                "label": entry_label,
                "slippage": t.get("slippage", ""),
                "alert_price": t.get("alert_price", ""),
            })
        elif t["action"] in ("EOD_CLOSE", "BRACKET_TP", "BRACKET_SL"):
            pnl = float(t["pnl"]) if t.get("pnl") else 0
            pnl_pct = float(t["pnl_pct"]) if t.get("pnl_pct") else 0
            exit_price = float(t["exit_price"]) if t.get("exit_price") else None
            exit_label_prefix = "COVER" if is_short else t["action"]
            close_markers.append({
                "time": t.get("exit_time", t["timestamp"]),
                "price": exit_price,
                "action": exit_label_prefix,
                "label": f"{exit_label_prefix} @ ${exit_price:.4f}" if exit_price else exit_label_prefix,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
            })

    # Summary info
    entry = open_markers[0] if open_markers else None
    exit_ = close_markers[0] if close_markers else None
    alert = alert_markers[0] if alert_markers else None

    # Build a detail table instead of a one-liner
    info_rows = []
    direction = "SHORT" if is_short else "LONG"
    info_rows.append(("Direction", f'<span style="color:{"#ef4444" if is_short else "#22c55e"}">{direction}</span>'))
    if alert:
        info_rows.append(("Alert Time", alert["time"][11:]))
        info_rows.append(("Alert Price", f"${alert['price']:.4f}"))
    if entry:
        entry_label = "Short Price" if is_short else "Entry Price"
        info_rows.append(("Entry Time", entry["time"][11:] if len(entry["time"]) > 11 else entry["time"]))
        info_rows.append((entry_label, f"${entry['price']:.4f}"))
        info_rows.append(("Shares", str(entry.get("shares", ""))))
        if alert and alert["price"]:
            if is_short:
                # Short slippage: sold below alert = unfavorable
                slip = alert["price"] - entry["price"]
            else:
                slip = entry["price"] - alert["price"]
            slip_pct = slip / alert["price"] * 100
            info_rows.append(("Slippage", f"${slip:.4f} ({slip_pct:+.2f}%)"))
    if exit_:
        exit_label = "Cover Price" if is_short else "Exit Price"
        info_rows.append(("Exit Time", exit_["time"][11:] if len(exit_["time"]) > 11 else exit_["time"]))
        info_rows.append((exit_label, f"${exit_['price']:.4f}" if exit_["price"] else "N/A"))
        info_rows.append(("Exit Type", exit_.get("action", "")))
        if entry and exit_["price"]:
            hold_sec = None
            try:
                from datetime import datetime as _dt
                t0 = _dt.strptime(entry["time"][:19], "%Y-%m-%d %H:%M:%S")
                t1 = _dt.strptime(exit_["time"][:19], "%Y-%m-%d %H:%M:%S")
                hold_sec = (t1 - t0).total_seconds()
            except:
                pass
            if hold_sec is not None:
                mins = int(hold_sec // 60)
                info_rows.append(("Hold Time", f"{mins}m {int(hold_sec % 60)}s"))
        pnl_class = "positive" if exit_["pnl"] >= 0 else "negative"
        info_rows.append(("P&L", f'<span class="{pnl_class}">${exit_["pnl"]:.2f} ({exit_["pnl_pct"]:+.1f}%)</span>'))

    # Also pull alert metadata from trade_list
    open_row = next((t for t in trade_list if t["action"] == "OPEN"), None)
    if open_row:
        for field, label in [("pct_change", "Alert % Change"), ("mcap", "Market Cap"),
                             ("mon_vol", "Mon Volume"), ("rel_vol", "Rel Volume"),
                             ("float_shares", "Float")]:
            val = open_row.get(field, "")
            if val and val != "N/A":
                info_rows.append((label, val))

    info_html = ""
    if info_rows:
        cells = "".join(f"<div class='info-cell'><div class='info-label'>{k}</div><div class='info-value'>{v}</div></div>" for k, v in info_rows)
        info_html = f"<div class='info-bar'>{cells}</div>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{ticker} {date}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-chart-financial@0.2.1/dist/chartjs-chart-financial.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=DM+Sans:wght@400;500;600&display=swap');
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0a0e17; color: #e2e8f0; font-family: 'DM Sans', sans-serif; padding: 20px; }}
  .header {{ margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #1e2a3e; }}
  .header h1 {{ font-family: 'JetBrains Mono', monospace; font-size: 20px; margin-bottom: 6px; }}
  .summary {{ font-size: 13px; color: #8896ae; font-family: 'JetBrains Mono', monospace; }}
  .positive {{ color: #22c55e; }}
  .negative {{ color: #ef4444; }}
  .chart-wrap {{ background: #111827; border: 1px solid #1e2a3e; border-radius: 10px; padding: 16px; margin-bottom: 16px; }}
  .chart-wrap h3 {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #8896ae; text-transform: uppercase; letter-spacing: 0.8px; margin-bottom: 10px; }}
  canvas {{ width: 100%; }}
  .legend {{ display: flex; gap: 20px; margin-top: 12px; font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #8896ae; }}
  .legend-item {{ display: flex; align-items: center; gap: 6px; }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; }}
  .info-bar {{ display: flex; flex-wrap: wrap; gap: 4px 16px; margin-top: 10px; font-family: 'JetBrains Mono', monospace; font-size: 11px; }}
  .info-cell {{ display: flex; align-items: baseline; gap: 5px; padding: 3px 10px; background: #111827; border: 1px solid #1e2a3e; border-radius: 6px; }}
  .info-label {{ color: #8896ae; white-space: nowrap; }}
  .info-value {{ color: #e2e8f0; font-weight: 600; white-space: nowrap; }}
</style>
</head>
<body>

<div class="header">
  <h1>{ticker} — {date}</h1>
  {info_html}
</div>

<div class="chart-wrap">
  <h3>{"📉 SHORT" if is_short else "📈 LONG"} — 15-Second Price + Volume</h3>
  <canvas id="priceChart" style="max-height:600px"></canvas>
  <div class="legend">
    <div class="legend-item"><div class="legend-dot" style="background:#f59e0b"></div> Discord Alert</div>
    <div class="legend-item"><div class="legend-dot" style="background:{"#ef4444" if is_short else "#3b82f6"}"></div> {"Short Entry" if is_short else "Bot Entry"}</div>
    <div class="legend-item"><div class="legend-dot" style="background:#a78bfa"></div> {"Cover" if is_short else "Bot Exit"}</div>
    <div class="legend-item"><div class="legend-dot" style="background:rgba(100,116,139,0.5);border-radius:2px"></div> Volume</div>
  </div>
</div>

<script>
const candles = {json.dumps(candles)};
const alerts = {json.dumps(alert_markers)};
const entries = {json.dumps(open_markers)};
const exits = {json.dumps(close_markers)};
const isShort = {'true' if is_short else 'false'};

Chart.defaults.color = '#8896ae';
Chart.defaults.borderColor = '#1e2a3e';
Chart.defaults.font.family = "'JetBrains Mono', monospace";
Chart.defaults.font.size = 10;

const labels = candles.map(c => c.t);
const closes = candles.map(c => c.c);
const highs = candles.map(c => c.h);
const lows = candles.map(c => c.l);
const volumes = candles.map(c => c.v);

// Find indices for markers
function findIdx(timeStr) {{
  // Find closest bar
  let bestIdx = 0;
  let bestDiff = Infinity;
  for (let i = 0; i < labels.length; i++) {{
    const diff = Math.abs(new Date(labels[i]) - new Date(timeStr));
    if (diff < bestDiff) {{ bestDiff = diff; bestIdx = i; }}
  }}
  return bestIdx;
}}

// Build alert index lookup for tooltips
const alertsByIdx = {{}};
alerts.forEach(a => {{
  const idx = findIdx(a.time);
  if (!alertsByIdx[idx]) alertsByIdx[idx] = [];
  alertsByIdx[idx].push(a);
}});

// Build marker datasets
function makeMarkerDataset(markers, color, radius, label) {{
  const data = new Array(labels.length).fill(null);
  markers.forEach(m => {{
    const idx = findIdx(m.time);
    data[idx] = m.price || closes[idx];
  }});
  return {{
    label: label,
    data: data,
    pointRadius: data.map(d => d !== null ? radius : 0),
    pointHoverRadius: data.map(d => d !== null ? radius + 3 : 0),
    pointHitRadius: data.map(d => d !== null ? 25 : 0),
    pointBackgroundColor: color,
    pointBorderColor: '#fff',
    pointBorderWidth: 1.5,
    pointStyle: 'circle',
    showLine: false,
    type: 'scatter',
    order: 0,
  }};
}}

// Price chart with close line + markers
const priceCtx = document.getElementById('priceChart').getContext('2d');

// Entry/exit zone shading
const entryIdx = entries.length > 0 ? findIdx(entries[0].time) : null;
const exitIdx = exits.length > 0 ? findIdx(exits[0].time) : null;
const entryPrice = entries.length > 0 ? entries[0].price : null;

// Background color based on P&L relative to entry
const bgColors = closes.map((c, i) => {{
  if (entryPrice === null || entryIdx === null) return 'rgba(59,130,246,0.08)';
  if (i < entryIdx) return 'rgba(59,130,246,0.03)';
  if (exitIdx !== null && i > exitIdx) return 'rgba(59,130,246,0.03)';
  if (isShort) {{
    // SHORT: green when price is BELOW entry (profitable), red when ABOVE
    return c <= entryPrice ? 'rgba(34,197,94,0.06)' : 'rgba(239,68,68,0.06)';
  }} else {{
    return c >= entryPrice ? 'rgba(34,197,94,0.06)' : 'rgba(239,68,68,0.06)';
  }}
}});

// Line color segments
const lineColors = closes.map((c, i) => {{
  if (i === 0) return '#64748b';
  return c >= closes[i-1] ? '#22c55e' : '#ef4444';
}});

new Chart(priceCtx, {{
  type: 'line',
  data: {{
    labels: labels,
    datasets: [
      {{
        label: 'Close',
        data: closes,
        borderColor: '#64748b',
        backgroundColor: bgColors,
        fill: true,
        tension: 0,
        pointRadius: 0,
        borderWidth: 1.5,
        order: 2,
        segment: {{
          borderColor: ctx => {{
            if (ctx.p0DataIndex === undefined) return '#64748b';
            const curr = closes[ctx.p1DataIndex];
            const prev = closes[ctx.p0DataIndex];
            return curr >= prev ? '#22c55e' : '#ef4444';
          }}
        }}
      }},
      // High/Low range as faint band
      {{
        label: 'High',
        data: highs,
        borderColor: 'transparent',
        backgroundColor: 'transparent',
        pointRadius: 0,
        fill: false,
        order: 3,
      }},
      {{
        label: 'Low',
        data: lows,
        borderColor: 'transparent',
        backgroundColor: 'rgba(100,116,139,0.05)',
        pointRadius: 0,
        fill: '-1',
        order: 3,
      }},
      // Entry price horizontal line
      ...(entryPrice ? [{{
        label: isShort ? 'Short Price' : 'Entry Price',
        data: labels.map(() => entryPrice),
        borderColor: isShort ? 'rgba(239,68,68,0.3)' : 'rgba(59,130,246,0.3)',
        borderDash: [4, 4],
        borderWidth: 1,
        pointRadius: 0,
        fill: false,
        order: 1,
      }}] : []),
      // Markers
      makeMarkerDataset(alerts, '#f59e0b', 6, 'Alert'),
      makeMarkerDataset(entries, isShort ? '#ef4444' : '#3b82f6', 7, 'Entry'),
      makeMarkerDataset(exits, '#a78bfa', 7, 'Exit'),
      // Volume bars on secondary axis
      {{
        label: 'Volume',
        data: volumes,
        type: 'bar',
        yAxisID: 'vol',
        backgroundColor: closes.map((c, i) => {{
          if (i === 0) return 'rgba(100,116,139,0.7)';
          return c >= closes[i-1] ? 'rgba(34,197,94,0.7)' : 'rgba(239,68,68,0.7)';
        }}),
        borderWidth: 0,
        order: 4,
      }},
    ]
  }},
  options: {{
    responsive: true,
    interaction: {{ mode: 'nearest', intersect: false, axis: 'xy' }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        displayColors: true,
        titleFont: {{ size: 12, weight: 'bold' }},
        bodyFont: {{ size: 11 }},
        padding: 12,
        cornerRadius: 8,
        callbacks: {{
          title: function(items) {{
            if (!items.length) return '';
            return items[0].label;
          }},
          label: function(ctx) {{
            if (ctx.dataset.label === 'Alert') {{
              const idx = ctx.dataIndex;
              const aa = alertsByIdx[idx];
              if (!aa || !aa.length) return '';
              const lines = [];
              aa.forEach(a => {{
                lines.push(`⚡ Alert #${{a.alert_num}} @ ${'$'}${{a.price}} (${{a.label}})`);
                lines.push(`   Time: ${{a.time}}`);
              }});
              return lines;
            }}
            if (ctx.dataset.label === 'Entry') {{
              const e = entries[0];
              if (!e) return '';
              const entryType = isShort ? '🔴 SHORT SELL' : '🔵 BOT ENTRY';
              const lines = [
                entryType,
                `   Price: ${'$'}${{e.price.toFixed(4)}}`,
                `   Time: ${{e.time}}`,
                `   Shares: ${{e.shares}}`,
              ];
              if (e.slippage) lines.push(`   Slippage: ${'$'}${{e.slippage}}`);
              if (e.alert_price) lines.push(`   Alert was: ${'$'}${{e.alert_price}}`);
              return lines;
            }}
            if (ctx.dataset.label === 'Exit') {{
              const x = exits[0];
              if (!x) return '';
              const pnlSign = x.pnl >= 0 ? '+' : '';
              const exitType = isShort ? '🟣 BUY TO COVER' : '🟣 BOT EXIT';
              return [
                exitType,
                `   Type: ${{x.action}}`,
                `   Price: ${'$'}${{x.price ? x.price.toFixed(4) : 'N/A'}}`,
                `   Time: ${{x.time}}`,
                `   P&L: ${{pnlSign}}${'$'}${{x.pnl.toFixed(2)}} (${{pnlSign}}${{x.pnl_pct.toFixed(1)}}%)`,
              ];
            }}
            if (ctx.dataset.label === 'Close') return `Close: ${'$'}${{ctx.parsed.y.toFixed(4)}}`;
            if (ctx.dataset.label === 'High') return `High: ${'$'}${{ctx.parsed.y.toFixed(4)}}`;
            if (ctx.dataset.label === 'Low') return `Low: ${'$'}${{ctx.parsed.y.toFixed(4)}}`;
            if (ctx.dataset.label === 'Volume') {{
              const v = ctx.parsed.y;
              return 'Vol: ' + (v >= 1e6 ? (v/1e6).toFixed(1)+'M' : v >= 1e3 ? (v/1e3).toFixed(0)+'K' : v);
            }}
            return null;
          }}
        }}
      }}
    }},
    scales: {{
      x: {{
        ticks: {{
          maxTicksLimit: 24,
          maxRotation: 45,
          font: {{ size: 9 }},
          callback: function(val, idx) {{
            const lbl = labels[idx];
            return lbl ? lbl.substring(11, 16) : '';
          }}
        }},
      }},
      y: {{
        position: 'left',
        ticks: {{ callback: v => '$' + v.toFixed(2) }},
      }},
      vol: {{
        position: 'right',
        grid: {{ display: false }},
        max: Math.max(...volumes) * 1.3,
        ticks: {{
          callback: v => v >= 1e6 ? (v/1e6).toFixed(1)+'M' : v >= 1e3 ? (v/1e3).toFixed(0)+'K' : v,
          font: {{ size: 9 }},
          maxTicksLimit: 4,
        }},
      }},
    }}
  }}
}});

</script>
</body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description="Alert vs Entry Chart Generator")
    parser.add_argument("--ticker", type=str, help="Single ticker to chart")
    parser.add_argument("--date", type=str, help="Date (YYYY-MM-DD)")
    parser.add_argument("--all", action="store_true", help="Chart all trades")
    parser.add_argument("--channel-export", type=str, default=CHANNEL_EXPORT)
    parser.add_argument("--trades", type=str, default=DETAILED_TRADES)
    parser.add_argument("--bars-cache", type=str, default=BARS_15S_CACHE_DIR)
    parser.add_argument("--output", type=str, default=OUTPUT_DIR)
    parser.add_argument("--port", type=int, default=4002, help="IBKR gateway port for fetching bars")
    # Entry filters
    parser.add_argument("--min-price", type=float, default=None, help="Min entry price filter")
    parser.add_argument("--min-monvol", type=float, default=None, help="Min monetary volume filter")
    parser.add_argument("--min-hour", type=int, default=None, help="Min entry hour (ET) filter")
    args = parser.parse_args()

    bars_cache = args.bars_cache

    print("Loading alerts...")
    alerts = load_alerts(args.channel_export)
    print(f"  {sum(len(v) for v in alerts.values())} alerts across {len(alerts)} ticker-days")

    print("Loading trades...")
    all_trades = load_trades(args.trades)
    print(f"  {sum(len(v) for v in all_trades.values())} trade rows across {len(all_trades)} ticker-days")

    # Determine which ticker-dates to chart
    if args.ticker and args.date:
        keys = [(args.ticker, args.date)]
    elif args.date:
        keys = [(k, d) for (k, d) in all_trades.keys() if d == args.date]
    elif args.all:
        keys = list(all_trades.keys())
    else:
        parser.error("Specify --ticker + --date, --date, or --all")
        return

    # Apply entry filters
    if args.min_price or args.min_monvol or args.min_hour:
        filtered_keys = []
        for key in keys:
            trade_rows = all_trades.get(key, [])
            open_row = next((t for t in trade_rows if t["action"] == "OPEN"), None)
            if not open_row:
                continue
            if args.min_price and float(open_row["entry_price"]) < args.min_price:
                continue
            if args.min_hour and int(open_row["entry_time"][11:13]) < args.min_hour:
                continue
            if args.min_monvol:
                mv = parse_money(open_row.get("mon_vol", ""))
                if not mv or mv < args.min_monvol:
                    continue
            filtered_keys.append(key)
        print(f"  Filtered: {len(keys)} → {len(filtered_keys)} ticker-days")
        keys = filtered_keys

    os.makedirs(args.output, exist_ok=True)
    generated = 0
    skipped = 0

    for ticker, date in sorted(keys):
        bars = load_bars(ticker, date, bars_cache, args.port)
        if bars is None:
            skipped += 1
            continue

        alert_list = alerts.get((ticker, date), [])
        trade_list = all_trades.get((ticker, date), [])

        html = generate_chart_html(ticker, date, bars, alert_list, trade_list)
        out_path = os.path.join(args.output, f"{ticker}_{date}.html")
        with open(out_path, "w") as f:
            f.write(html)
        generated += 1

    print(f"\nDone: {generated} charts in {args.output}/, {skipped} skipped (no bars)")

    if generated == 1:
        print(f"Open: {os.path.join(args.output, f'{keys[0][0]}_{keys[0][1]}.html')}")
    elif generated > 1:
        # Generate index page
        index_path = os.path.join(args.output, "index.html")
        with open(index_path, "w") as f:
            f.write(f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Trade Charts</title>
<style>
  body {{ background: #0a0e17; color: #e2e8f0; font-family: 'Segoe UI', sans-serif; padding: 24px; }}
  a {{ color: #3b82f6; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 8px; }}
  .card {{ background: #111827; border: 1px solid #1e2a3e; border-radius: 8px; padding: 12px 16px; }}
  .ticker {{ font-weight: 700; font-size: 16px; }}
  .date {{ color: #8896ae; font-size: 13px; }}
</style></head><body>
<h1>Trade Charts ({generated})</h1><div class="grid">""")
            for ticker, date in sorted(keys):
                fname = f"{ticker}_{date}.html"
                if os.path.exists(os.path.join(args.output, fname)):
                    # Get P&L if available
                    trade_list = all_trades.get((ticker, date), [])
                    close_row = next((t for t in trade_list if t["action"] != "OPEN"), None)
                    open_row = next((t for t in trade_list if t["action"] == "OPEN"), None)
                    pnl_str = ""
                    if close_row and close_row.get("pnl"):
                        pnl = float(close_row["pnl"])
                        color = "#22c55e" if pnl >= 0 else "#ef4444"
                        pnl_str = f' · <span style="color:{color}">${pnl:.2f}</span>'
                    dir_str = ""
                    if open_row:
                        if open_row.get("direction", "").upper() == "SHORT":
                            dir_str = ' · <span style="color:#ef4444">SHORT</span>'
                        elif open_row.get("direction", "").upper() == "LONG":
                            pass  # no badge for long
                        elif open_row.get("snap_bid") and open_row.get("snap_ask"):
                            try:
                                ep = float(open_row["entry_price"])
                                sb = float(open_row["snap_bid"])
                                sa = float(open_row["snap_ask"])
                                if sb < sa and abs(ep - sb) <= abs(ep - sa):
                                    dir_str = ' · <span style="color:#ef4444">SHORT</span>'
                            except (ValueError, TypeError):
                                pass
                    f.write(f'<div class="card"><a href="{fname}"><span class="ticker">{ticker}</span> <span class="date">{date}{dir_str}{pnl_str}</span></a></div>\n')
            f.write("</div></body></html>")
        print(f"Index: {index_path}")


if __name__ == "__main__":
    main()