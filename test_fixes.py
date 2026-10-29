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
alert = ("ABC :flag_us: - Alert #1\n↑ 20.00%\nPrice\n$0.50\n"
         "Today's SEC Filing\n[FORM 8-K](https://www.sec.gov/a.htm)\n[FORM SCH 13G](https://www.sec.gov/b.xml)\n"
         "Recent IPO\nListed Today")
assert parse(alert)["sec_filing"] == "FORM 8-K, FORM SCH 13G"
assert parse_alert(alert)["sec_filing"] == "FORM 8-K, FORM SCH 13G"
assert parse("ABC - Alert #1\nToday's SEC Filing\nFORM 6-K\n")["sec_filing"] == "FORM 6-K"  # plain text still works
print("sec ok")
