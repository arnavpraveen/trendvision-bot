"""Fetch the NBBO bid/ask at entry and at +30 min for each event in logs/event_returns.csv.

Run after event_study.py:  python fetch_alpaca_quotes.py [max_events]
Output: logs/event_quotes.csv (appends; rerun to resume). Then rerun event_study.py to score
shorts as: sell at the entry bid, buy back at the +30 min ask.
~2 requests per event at up to 200/min: ~4 hours for all events.
"""
import csv, os, sys, time
from datetime import datetime, timezone
from pathlib import Path

import requests

import env  # loads .env
from fetch_alpaca_bars import get

URL = "https://data.alpaca.markets/v2/stocks/quotes"
EVENTS, OUT = Path("logs/event_returns.csv"), Path("logs/event_quotes.csv")
MIN_GAP = 0.31  # free plan: 200 requests/min
_last = [0.0]
FIELDS = ["message_id", "bid0", "ask0", "q0_age_s", "bid30", "ask30", "q30_age_s"]


def iso(sec):
    return datetime.fromtimestamp(sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def first_quote(session, sym, sec):
    """NBBO in force at `sec`: last sane quote in the 5 min before -> (bid, ask, age seconds) or blanks."""
    time.sleep(max(0, _last[0] + MIN_GAP - time.monotonic()))
    _last[0] = time.monotonic()
    data = get(session, {"symbols": sym, "start": iso(sec - 300), "end": iso(sec), "limit": 50,
                         "feed": "sip", "sort": "desc"}, URL)
    for q in (data.get("quotes") or {}).get(sym, []):
        if q["bp"] > 0 and q["ap"] >= q["bp"]:
            t = datetime.fromisoformat(q["t"][:19]).replace(tzinfo=timezone.utc)  # drop nanoseconds
            return q["bp"], q["ap"], sec - int(t.timestamp())
    return "", "", ""


def main():
    key, secret = os.environ.get("ALPACA_API_KEY"), os.environ.get("ALPACA_SECRET_KEY")
    if not (key and secret):
        sys.exit("ALPACA_API_KEY / ALPACA_SECRET_KEY not set - add them to .env")
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    done = set()
    if OUT.exists():
        with open(OUT, newline="") as f:
            done = {r["message_id"] for r in csv.DictReader(f)}
    with open(EVENTS, newline="") as f:
        events = [r for r in csv.DictReader(f) if r["message_id"] not in done][:limit]
    session = requests.Session()
    session.headers.update({"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret})
    new = not OUT.exists()
    with open(OUT, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(FIELDS)
        for n, e in enumerate(events, 1):
            t0 = (int(e["alert_min"]) + int(float(e["entry_lag"]))) * 60  # same minute the bar study enters
            w.writerow([e["message_id"], *first_quote(session, e["ticker"], t0),
                        *first_quote(session, e["ticker"], t0 + 30 * 60)])
            f.flush()
            if n % 200 == 0:
                print(f"{n}/{len(events)} ({len(done) + n} total)", flush=True)
    print("done")


if __name__ == "__main__":
    main()
