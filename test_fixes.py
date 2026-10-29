"""Run: python test_fixes.py  (tick rounding, commission cap, SEC filing parsing)."""
from discord_ibkr_bot import round_tick, calc_commission

# Sub-$1: keep the buffer (old round(x, 2) returned 0.17, i.e. no buffer).
assert round_tick(0.17 * 1.02, up=True) == 0.1734
assert round_tick(0.4567 * 0.995, up=False) == 0.4544
# $1+: cents, rounded toward the aggressive side.
assert round_tick(2.501, up=True) == 2.51
assert round_tick(2.509, up=False) == 2.50
assert round_tick(3.10, up=True) == 3.10  # exact prices stay put
# Commission: $1 min, capped at 1% of trade value.
assert calc_commission(500, 0.20) == 1.00   # 1% of $100
assert calc_commission(100, 0.50) == 0.50   # cap beats the $1 min
assert calc_commission(1000, 5.00) == 5.00  # $0.005/share
print("ok")

# SEC filings arrive as markdown links; the old regex expected lines starting with "FORM".
from discord_ibkr_bot import parse_alert
from export_discord_channel import parse
alert = ("ABC :flag_us: - Alert #1 | ↑ 30.00%\nPrice\n$0.50\n"
         "Today's SEC Filing\n[FORM 8-K](https://www.sec.gov/a.htm)\n[FORM SCH 13G](https://www.sec.gov/b.xml)\n"
         "Recent IPO\nListed Today")
assert parse(alert)["sec_filing"] == "FORM 8-K, FORM SCH 13G"
assert parse_alert(alert)["sec_filing"] == "FORM 8-K, FORM SCH 13G"
assert parse("ABC - Alert #1\nToday's SEC Filing\nFORM 6-K\n")["sec_filing"] == "FORM 6-K"  # plain text still works
print("sec ok")

# Only alerts already up MIN_PCT_CHANGE (25%) are traded.
assert parse_alert("Title: ABC - Alert #1 | ↑ 25.00%\nPrice: $1.00")["pct_change"] == "+25.00%"
assert parse_alert("Title: ABC - Alert #1 | ↑ 24.99%\nPrice: $1.00") is None
assert parse_alert("Title: ABC - Alert #1 | ↓ 40.00%\nPrice: $1.00") is None
assert parse_alert("Title: ABC - Alert #1\nPrice: $1.00") is None  # no move shown
print("min move ok")

# Alpaca fetch groups alerts by US/Eastern date: 00:30 UTC belongs to the previous ET day.
from fetch_alpaca_bars import ticker_days
days = ticker_days([{"timestamp": "2026-04-09 20:30:00 UTC", "ticker": "ABC"},
                    {"timestamp": "2026-04-10 00:30:00 UTC", "ticker": "XYZ"},
                    {"timestamp": "2026-02-02 09:00:16 UTC", "ticker": "ABC"}])
assert days == {"2026-04-09": {"ABC", "XYZ"}, "2026-02-02": {"ABC"}}, days
print("alpaca ok")

# Event study: entry is the open of the bar after the alert's minute; returns are short returns.
import numpy as np
from event_study import forward, num
t = np.arange(100, 200)                    # bars at minutes 100..199
o = c = h = l = np.full(100, 10.0)
c = c.copy(); c[30] = 8.0                  # bar 130 closes at 8 -> 30-min horizon from entry 101
h = h.copy(); h[5] = 12.0                  # spike to 12 inside the window
r = forward(t, o, h, l, c, alert_min=100, end_min=1000, close_min=1000)
assert r["entry_lag"] == 1 and r["entry"] == 10.0
assert abs(r["r30"] - (10 / 8 - 1)) < 1e-9 and abs(r["mae30"] - 0.2) < 1e-9
assert np.isnan(forward(t, o, h, l, c, 100, end_min=120, close_min=1000)["r30"])  # past 8pm
assert forward(t, o, h, l, c, alert_min=300, end_min=1000, close_min=1000) is None  # no bar after alert
assert num("$137.50K") == 137500 and num("4.3M") == 4.3e6 and num("-15.88%") == -15.88 and np.isnan(num("N/A"))
print("event study ok")
