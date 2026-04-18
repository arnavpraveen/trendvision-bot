#!/usr/bin/env python3
"""
Backtest Results → Markdown Report
===================================
Reads strategy_summary.csv and per-strategy trade CSVs from backtest_results/,
generates a clean markdown report.

Usage:
    python visualize_backtest.py [--input backtest_results/] [--output backtest_report.md]
"""

import argparse
import csv
import os
from collections import defaultdict


def load_summary(input_dir):
    with open(os.path.join(input_dir, "strategy_summary.csv")) as f:
        return list(csv.DictReader(f))


def load_strategy_trades(input_dir, name):
    path = os.path.join(input_dir, f"{name}_trades.csv")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def fmt_pnl(v):
    v = float(v)
    return f"+${v:.2f}" if v >= 0 else f"-${abs(v):.2f}"


def fmt_pct(v):
    v = float(v) * 100
    return f"{v:.1f}%"


def fmt_num(v, decimals=2):
    v = float(v)
    if abs(v) > 50:
        return "∞"
    return f"{v:.{decimals}f}"


def build_report(input_dir, output_path):
    summary = load_summary(input_dir)
    n_strats = len(summary)
    n_trades = summary[0]["n_trades"] if summary else "?"

    lines = []
    w = lines.append

    # ── Header ──
    w("# Exit Strategy Backtest Report\n")
    w(f"**{n_trades} trades** across **{n_strats} strategies**, ranked by total P&L.\n")

    # ── Top picks ──
    best = summary[0]
    worst = summary[-1]
    best_wr = max(summary, key=lambda s: float(s["win_rate"]))
    best_sharpe = max(summary, key=lambda s: float(s["sharpe"]))

    w("## Key Findings\n")
    w("| Metric | Strategy | Value |")
    w("|--------|----------|-------|")
    w(f"| Best P&L | **{best['strategy']}** | {fmt_pnl(best['total_pnl'])} |")
    w(f"| Worst P&L | {worst['strategy']} | {fmt_pnl(worst['total_pnl'])} |")
    w(f"| Highest Win Rate | **{best_wr['strategy']}** | {fmt_pct(best_wr['win_rate'])} |")
    w(f"| Best Sharpe | **{best_sharpe['strategy']}** | {fmt_num(best_sharpe['sharpe'], 3)} |")
    w("")

    # ── Full ranking table ──
    w("## Full Rankings\n")
    w("| # | Strategy | Total P&L | Mean | Median | WR | Wins/N | PF | R:R | Sharpe | BE WR | Avg Hold | Worst |")
    w("|---|----------|-----------|------|--------|----|--------|-----|-----|--------|-------|----------|-------|")

    for i, s in enumerate(summary):
        pnl = float(s["total_pnl"])
        tag = " 🏆" if i == 0 else ""
        w(f"| {i+1} | **{s['strategy']}**{tag} "
          f"| {fmt_pnl(pnl)} "
          f"| ${float(s['mean_pnl']):.2f} "
          f"| ${float(s['median_pnl']):.2f} "
          f"| {fmt_pct(s['win_rate'])} "
          f"| {s['n_wins']}/{s['n_trades']} "
          f"| {fmt_num(s['profit_factor'])} "
          f"| {fmt_num(s['reward_risk'])} "
          f"| {fmt_num(s['sharpe'], 3)} "
          f"| {fmt_pct(s['breakeven_wr'])} "
          f"| {float(s['avg_hold_min']):.0f}m "
          f"| {float(s['worst_trade_pct']):.1f}% |")

    w("")

    # ── Per-strategy breakdowns (top 5 + worst) ──
    show = summary[:5] + [summary[-1]]
    seen = set()
    show = [s for s in show if s["strategy"] not in seen and not seen.add(s["strategy"])]

    w("## Strategy Breakdowns\n")

    for s in show:
        name = s["strategy"]
        trades = load_strategy_trades(input_dir, name)
        if not trades:
            continue

        w(f"### {name}\n")

        # Exit breakdown
        exits = defaultdict(int)
        pnls = []
        for t in trades:
            exits[t["exit_reason"]] += 1
            pnls.append(float(t["pnl"]))

        exit_str = " · ".join(f"**{k}**: {v}" for k, v in sorted(exits.items(), key=lambda x: -x[1]))
        w(f"**Exits:** {exit_str}\n")

        # P&L distribution
        buckets = {"< -20%": 0, "-20 to -10%": 0, "-10 to -5%": 0, "-5 to 0%": 0,
                   "0 to 5%": 0, "5 to 10%": 0, "10 to 20%": 0, "> 20%": 0}
        for t in trades:
            p = float(t["pnl_pct"])
            if p < -20: buckets["< -20%"] += 1
            elif p < -10: buckets["-20 to -10%"] += 1
            elif p < -5: buckets["-10 to -5%"] += 1
            elif p < 0: buckets["-5 to 0%"] += 1
            elif p < 5: buckets["0 to 5%"] += 1
            elif p < 10: buckets["5 to 10%"] += 1
            elif p < 20: buckets["10 to 20%"] += 1
            else: buckets["> 20%"] += 1

        w("**P&L Distribution:**\n")
        w("| Bucket | Count | Bar |")
        w("|--------|------:|-----|")
        max_count = max(buckets.values()) if any(buckets.values()) else 1
        for bucket, count in buckets.items():
            bar_len = int(count / max_count * 20) if max_count > 0 else 0
            bar = "█" * bar_len
            w(f"| {bucket} | {count} | {bar} |")
        w("")

        # Top 5 winners / losers
        sorted_trades = sorted(trades, key=lambda t: float(t["pnl"]), reverse=True)

        w("**Top 5 Winners:**\n")
        w("| Ticker | P&L | P&L % | Hold | Exit |")
        w("|--------|-----|-------|------|------|")
        for t in sorted_trades[:5]:
            w(f"| {t['ticker']} | ${float(t['pnl']):.2f} | {float(t['pnl_pct']):.1f}% | {float(t['hold_time_min']):.0f}m | {t['exit_reason']} |")
        w("")

        w("**Top 5 Losers:**\n")
        w("| Ticker | P&L | P&L % | Hold | Exit |")
        w("|--------|-----|-------|------|------|")
        for t in sorted_trades[-5:]:
            w(f"| {t['ticker']} | ${float(t['pnl']):.2f} | {float(t['pnl_pct']):.1f}% | {float(t['hold_time_min']):.0f}m | {t['exit_reason']} |")
        w("")

        # Equity curve stats
        cum = 0.0
        peak = 0.0
        trough = 0.0
        for p in pnls:
            cum += p
            peak = max(peak, cum)
            trough = min(trough, cum)

        w(f"**Equity curve:** peak ${peak:.2f} · trough ${trough:.2f} · final ${cum:.2f} · "
          f"max drawdown ${peak - trough:.2f}\n")
        w("---\n")

    # ── Strategy category comparison ──
    w("## Category Comparison\n")
    w("Strategies grouped by exit type:\n")

    categories = {
        "Fixed Bracket": [s for s in summary if s["strategy"].startswith("TP") and not any(c.isalpha() for c in s["strategy"].split("_")[-1]) == False and "TRAIL" not in s["strategy"] and "TIME" not in s["strategy"] and s["strategy"].count("_") <= 2],
        "Trailing Stop": [s for s in summary if s["strategy"].startswith("TRAIL")],
        "Time-Based": [s for s in summary if s["strategy"].startswith("TIME")],
        "Hybrid (Bracket+Time)": [s for s in summary if s["strategy"].startswith("TP") and s["strategy"].count("_") > 2],
        "EOD Only": [s for s in summary if s["strategy"] == "EOD_ONLY"],
    }

    w("| Category | Best | P&L | Avg P&L | Avg WR |")
    w("|----------|------|-----|---------|--------|")
    for cat, strats in categories.items():
        if not strats:
            continue
        best_in_cat = max(strats, key=lambda s: float(s["total_pnl"]))
        avg_pnl = sum(float(s["total_pnl"]) for s in strats) / len(strats)
        avg_wr = sum(float(s["win_rate"]) for s in strats) / len(strats) * 100
        w(f"| {cat} | {best_in_cat['strategy']} | {fmt_pnl(best_in_cat['total_pnl'])} | ${avg_pnl:.2f} | {avg_wr:.1f}% |")

    w("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Report saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Backtest Results → Markdown Report")
    parser.add_argument("--input", type=str, default="backtest_results")
    parser.add_argument("--output", type=str, default="backtest_report.md")
    args = parser.parse_args()

    if not os.path.exists(os.path.join(args.input, "strategy_summary.csv")):
        print(f"Error: {args.input}/strategy_summary.csv not found. Run exit_strategy_backtest.py first.")
        return

    build_report(args.input, args.output)


if __name__ == "__main__":
    main()