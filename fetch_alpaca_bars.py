"""Fetch 1-min bars (4am-8pm ET) from Alpaca for every ticker-day in the channel export.

Setup: add ALPACA_API_KEY and ALPACA_SECRET_KEY (free paper-account keys) to .env.
Run:   python fetch_alpaca_bars.py            # all days, skips days already saved
       python fetch_alpaca_bars.py 2026-04-09 # just these dates (quick test)
Output: bars_alpaca/YYYY-MM-DD.csv  (symbol,t,o,h,l,c,v,n,vw; t in UTC; raw prices, not split-adjusted,
        so they line up with the alert prices). Tickers with no bars are listed in bars_alpaca/_missing.csv.
"""
import csv, os, sys, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

import env  # loads .env

EXPORT = "channel_export_1467323184905850921.csv"
OUT = Path("bars_alpaca")
URL = "https://data.alpaca.markets/v2/stocks/bars"
ET = ZoneInfo("America/New_York")
CHUNK = 100       # symbols per request
PAUSE = 0.35      # free plan: 200 requests/min
FIELDS = ["symbol", "t", "o", "h", "l", "c", "v", "n", "vw"]


def ticker_days(rows):
    """{ET date: set of tickers} from export rows (timestamp like '2026-02-02 09:00:16 UTC')."""
    days = defaultdict(set)
    for r in rows:
        ts = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
        days[ts.astimezone(ET).date().isoformat()].add(r["ticker"])
    return days


def get(session, params, url=URL):
    for attempt in range(6):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429 or resp.status_code >= 500:
            time.sleep(2 ** attempt * 5)
            continue
        sys.exit(f"Alpaca error {resp.status_code}: {resp.text[:300]}")
    sys.exit("Alpaca kept rate-limiting / failing; try again later")


def fetch_day(session, day, symbols):
    # ponytail: 4am-8pm ET same day only; after-hours alerts near 8pm get short windows.
    d = datetime.fromisoformat(day).replace(tzinfo=ET)
    start, end = d + timedelta(hours=4), d + timedelta(hours=20)
    bars = []
    for i in range(0, len(symbols), CHUNK):
        params = {"symbols": ",".join(symbols[i:i + CHUNK]), "timeframe": "1Min",
                  "start": start.isoformat(), "end": end.isoformat(), "limit": 10000,
                  "feed": "sip", "adjustment": "raw", "sort": "asc"}
        while True:
            data = get(session, params)
            for sym, rows in (data.get("bars") or {}).items():
                bars += [{"symbol": sym, **{k: b.get(k) for k in FIELDS[1:]}} for b in rows]
            time.sleep(PAUSE)
            if not data.get("next_page_token"):
                break
            params["page_token"] = data["next_page_token"]
        params.pop("page_token", None)
    return bars


def main():
    key, secret = os.environ.get("ALPACA_API_KEY"), os.environ.get("ALPACA_SECRET_KEY")
    if not (key and secret):
        sys.exit("ALPACA_API_KEY / ALPACA_SECRET_KEY not set - add them to .env")
    with open(EXPORT, newline="", encoding="utf-8") as f:
        days = ticker_days(csv.DictReader(f))
    wanted = sys.argv[1:] or sorted(days)
    OUT.mkdir(exist_ok=True)
    session = requests.Session()
    session.headers.update({"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret})

    missing_path = OUT / "_missing.csv"
    for n, day in enumerate(wanted, 1):
        path = OUT / f"{day}.csv"
        if path.exists() or day not in days:
            continue
        symbols = sorted(days[day])
        bars = fetch_day(session, day, symbols)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            w.writeheader()
            w.writerows(bars)
        tmp.rename(path)  # only complete days count as done
        got = {b["symbol"] for b in bars}
        missing = [s for s in symbols if s not in got]
        if missing:
            new = not missing_path.exists()
            with open(missing_path, "a", newline="") as f:
                w = csv.writer(f)
                if new:
                    w.writerow(["date", "ticker"])
                w.writerows([day, s] for s in missing)
        print(f"[{n}/{len(wanted)}] {day}: {len(symbols)} tickers, {len(got)} with bars, {len(bars)} bars", flush=True)
    print("done")


if __name__ == "__main__":
    main()
