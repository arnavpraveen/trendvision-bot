"""Event study: what does price do after a TrendVision alert? No fills, fees or borrow modelled.

Event = first alert per ticker per ET day. Entry = open of the first 1-min bar after the alert's minute
(skipped if that bar is >5 min later). Returns are SHORT returns: +2% means price fell 2%.
Discovery = Feb-Jun, holdout = Jul-Oct; trust a pattern only if it shows up in both.
t-stats are clustered by day (alerts on the same day move together).

Needs bars_alpaca/ from fetch_alpaca_bars.py. Run: python event_study.py
Writes logs/event_study_report.txt and logs/event_returns.csv.
"""
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

EXPORT = "channel_export_1467323184905850921.csv"
BARS = Path("bars_alpaca")
ET = ZoneInfo("America/New_York")
HORIZONS = [5, 15, 30, 60]
HOLDOUT_START = "2026-07-01"
MAX_ENTRY_LAG = 5


def et_minute(day, hh, mm):
    """Minutes since epoch (UTC) for an ET wall-clock time on `day`."""
    d = datetime.fromisoformat(day).replace(hour=hh, minute=mm, tzinfo=ET)
    return int(d.timestamp() // 60)


def forward(t, o, h, l, c, alert_min, end_min, close_min):
    """Short returns after an alert. t = bar start minutes (sorted); bar t covers [t, t+1)."""
    i = np.searchsorted(t, alert_min + 1)  # first bar fully after the alert
    if i == len(t) or t[i] - (alert_min + 1) > MAX_ENTRY_LAG:
        return None
    entry, t0 = o[i], t[i]
    out = {"entry": entry, "entry_lag": t[i] - alert_min}
    for hz in HORIZONS:
        if t0 + hz > end_min:
            out[f"r{hz}"] = np.nan  # window runs past 8pm ET
            continue
        j = np.searchsorted(t, t0 + hz) - 1  # last bar starting before t0+hz
        out[f"r{hz}"] = entry / c[j] - 1
        if hz == 5:
            c5 = c[j]
        else:  # close-to-close from +5 min: no bid-ask bounce from entering on the alert print
            out[f"r5_{hz}"] = c5 / c[j] - 1
        if hz == 30:
            out["mae30"] = h[i:j + 1].max() / entry - 1  # worst rise against a short
            out["mfe30"] = 1 - l[i:j + 1].min() / entry  # best fall
    if t0 < close_min:
        j = np.searchsorted(t, close_min) - 1
        out["r_close"] = entry / c[j] - 1  # hold to 4pm close
    else:
        out["r_close"] = np.nan
    return out


def num(s):
    """'4.3M' / '$137.50K' / '34.17x' / '+11.86%' -> float; 'N/A' -> NaN."""
    m = re.fullmatch(r"[$+]*(-?[\d.]+)([KMB]?)[x%]?", str(s).replace(",", "").strip())
    if not m:
        return np.nan
    return float(m.group(1)) * {"": 1, "K": 1e3, "M": 1e6, "B": 1e9}[m.group(2)]


def stats(df, col):
    r, days = df[col], df["day"]
    ok = r.notna()
    r, days = r[ok], days[ok]
    n = len(r)
    if n < 30:
        return pd.Series({"n": n})
    mean = r.mean()
    se = np.sqrt(((r - mean).groupby(days).sum() ** 2).sum()) / n  # day-clustered
    return pd.Series({"n": n, "days": days.nunique(), "mean%": 100 * mean, "median%": 100 * r.median(),
                      "fell%": 100 * (r > 0).mean(), "t": mean / se if se and days.nunique() >= 10 else np.nan})


def load_events():
    a = pd.read_csv(EXPORT, dtype=str, keep_default_na=False)
    ts = pd.to_datetime(a["timestamp"].str[:19], utc=True)
    a["et"] = ts.dt.tz_convert(ET)
    a["day"] = a["et"].dt.strftime("%Y-%m-%d")
    a["alert_min"] = (ts.astype("datetime64[s, UTC]").astype("int64") // 60).astype("int64")
    a = a.sort_values("alert_min").drop_duplicates(["ticker", "day"])  # first alert per ticker-day
    return a


def compute(a):
    rows = []
    for day, ev in a.groupby("day"):
        f = BARS / f"{day}.csv"
        if not f.exists():
            continue
        b = pd.read_csv(f, usecols=["symbol", "t", "o", "h", "l", "c"])
        b["m"] = np.array(b["t"].str[:16], dtype="datetime64[m]").astype("int64")
        bars = {s: g.sort_values("m") for s, g in b.groupby("symbol")}
        end_min, close_min = et_minute(day, 20, 0), et_minute(day, 16, 0)
        for _, e in ev.iterrows():
            g = bars.get(e["ticker"])
            if g is None:
                continue
            res = forward(g["m"].values, g["o"].values, g["h"].values, g["l"].values, g["c"].values,
                          e["alert_min"], end_min, close_min)
            if res:
                rows.append({"idx": e.name, **res})
    return a.join(pd.DataFrame(rows).set_index("idx"), how="inner")


def features(d):
    mins = d["et"].dt.hour * 60 + d["et"].dt.minute
    d["session"] = np.select([mins < 570, mins < 960], ["1 pre 4-9:30", "2 regular"], "3 after 16-20")
    d["reason"] = d["alert_reason"].replace("", "(none)")
    d["move_at_alert"] = pd.cut(d["pct_change"].map(num), [-np.inf, 0, 10, 25, 50, 100, np.inf],
                                labels=["<0%", "0-10%", "10-25%", "25-50%", "50-100%", ">100%"])
    d["price_b"] = pd.cut(d["price"].map(num), [0, 1, 2, 5, 10, np.inf], labels=["<$1", "$1-2", "$2-5", "$5-10", ">$10"])
    d["float_b"] = pd.cut(d["float_shares"].map(num), [0, 2e6, 5e6, 20e6, np.inf],
                          labels=["<2M", "2-5M", "5-20M", ">20M"]).cat.add_categories("N/A").fillna("N/A")
    d["mcap_b"] = pd.cut(d["mcap"].map(num), [0, 10e6, 50e6, 200e6, np.inf], labels=["<10M", "10-50M", "50-200M", ">200M"])
    d["relvol_b"] = pd.cut(d["rel_vol"].map(num), [0, 1, 5, 20, 100, np.inf], labels=["<1x", "1-5x", "5-20x", "20-100x", ">100x"])
    d["dollar_vol_b"] = pd.cut(d["mon_vol"].map(num), [0, 50e3, 200e3, 1e6, np.inf], labels=["<$50K", "$50-200K", "$200K-1M", ">$1M"])
    for tag in ["Known Runner", "Potential Squeeze", "Reg SHO"]:
        d[tag] = d["tags"].str.contains(tag)
    d["zero_borrow"] = d["zero_borrow"] != ""
    d["has_news"] = d["news"] != ""
    d["has_sec"] = d["sec_filing"] != ""
    d["recent_rs"] = d["recent_rs"] != ""
    d["recent_ipo"] = d["recent_ipo"] != ""
    d["split"] = np.where(d["day"] < HOLDOUT_START, "discovery", "holdout")
    return d


GROUPS = ["session", "reason", "move_at_alert", "price_b", "float_b", "mcap_b", "relvol_b", "dollar_vol_b",
          "Known Runner", "Potential Squeeze", "Reg SHO", "zero_borrow", "has_news", "has_sec", "recent_rs", "recent_ipo"]


def report(d):
    pd.set_option("display.width", 200)
    fmt = lambda x: x.round(2).to_string()
    out = [f"{len(d)} events (first alert per ticker-day), {d['day'].nunique()} days. "
           "Short returns, entry = next 1-min bar open, no costs.",
           "Cost reference: ~2% round-trip commission at $100/trade, plus spread and borrow.\n",
           "== All events by horizon =="]
    out.append(fmt(pd.DataFrame({c: stats(d, c) for c in [f"r{h}" for h in HORIZONS] + ["r_close", "r5_15", "r5_30", "r5_60"]}).T))
    out.append("r5_X = short at the close 5 min after entry, cover at X min (removes bounce off the alert print).")
    out.append(f"Entry vs alert price, median: {100 * (d['entry'] / d['price'].map(num) - 1).median():.2f}%")
    out.append("\n== Price excursion in first 30 min (median %) ==")
    out.append(f"worst rise against short {100 * d['mae30'].median():.2f}%, best fall {100 * d['mfe30'].median():.2f}%, "
               f"share that rose 20%+ at some point: {100 * (d['mae30'] >= 0.2).mean():.1f}%")
    if "q30" in d:
        out.append("\n== With real quotes: short at the entry bid, cover at the +30 min ask ==")
        out.append(fmt(pd.DataFrame({c: stats(d, c) for c in ["r30", "m30", "q30"]}).T))
        out.append(f"r30 = bar prices, m30 = mid to mid, q30 = bid to ask. Events with both quotes: {d['q30'].notna().sum()}. "
                   f"Median spread at entry: {100 * d['spread0'].median():.2f}% "
                   f"(share wider than 2%: {100 * (d['spread0'].dropna() > 0.02).mean():.1f}%)")
    by = lambda keys, col="r30": pd.DataFrame({k: stats(x, col) for k, x in d.groupby(keys, observed=True)}).T
    out.append(f"\n== 30-min short return by month ==\n{fmt(by(d['day'].str[:7]))}")
    for g in GROUPS:
        a, b = by([g, "split"]), by([g, "split"], "r5_30")
        q = by([g, "split"], "q30") if "q30" in d else pd.DataFrame()
        t = pd.concat({"n": a.get("n"), "mean%": a.get("mean%"), "median%": a.get("median%"), "t": a.get("t"),
                       "late median%": b.get("median%"), "bid/ask median%": q.get("median%"),
                       "bid/ask t": q.get("t")}, axis=1).unstack(1)
        out.append(f"\n== 30-min short return by {g} (discovery Feb-Jun | holdout Jul-Oct; "
                   f"late = r5_30, bid/ask = q30) ==\n{fmt(t)}")
    return "\n".join(out)


if __name__ == "__main__":
    d = features(compute(load_events()))
    Path("logs").mkdir(exist_ok=True)
    d.drop(columns=["raw_text", "et"]).to_csv("logs/event_returns.csv", index=False)
    if Path("logs/event_quotes.csv").exists():  # from fetch_alpaca_quotes.py
        d = d.merge(pd.read_csv("logs/event_quotes.csv", dtype={"message_id": str}), on="message_id", how="left")
        mid0, mid30 = (d["bid0"] + d["ask0"]) / 2, (d["bid30"] + d["ask30"]) / 2
        d["q30"] = d["bid0"] / d["ask30"] - 1
        d["m30"] = mid0 / mid30 - 1
        d["spread0"] = (d["ask0"] - d["bid0"]) / mid0
    text = report(d)
    Path("logs/event_study_report.txt").write_text(text)
    print(text)
