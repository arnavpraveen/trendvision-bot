#!/usr/bin/env python3
"""
Discord Channel Exporter
=========================
Exports all embed messages from a Discord channel to a CSV.
Uses your personal Discord user token and the requests library only.

Usage:
    pip install requests
    python export_discord_channel.py

How to get your user token:
    1. Open Discord in your browser (discord.com/app)
    2. Press F12 → Network tab
    3. Refresh the page, click any request to discord.com
    4. In Request Headers look for "Authorization" – that value is your token
    5. Put it in .env as DISCORD_TOKEN=... (see .env.example)

Output:
    channel_export_<CHANNEL_ID>.csv
    Re-running appends only new messages (deduped by message_id).
"""

import requests
import csv
import re
import sys
import os
import time
import env  # loads .env
from datetime import datetime, timezone

# ============ CONFIGURATION ============
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")  # personal user token, from .env
CHANNEL_ID    = "1467323184905850921" # ← channel ID (as a string)

OUTPUT_FILE = f"channel_export_{CHANNEL_ID}.csv"

# ============ CSV COLUMNS ============
COLUMNS = [
    "message_id", "timestamp", "author",
    "ticker", "alert_num", "pct_change",
    "price", "mcap", "float_shares",
    "min_vol", "rel_vol", "mon_vol",
    "alert_reason", "tags", "news",
    "sec_filing", "recent_ipo", "recent_rs", "zero_borrow",
    "raw_text",
]

# ============ EXISTING-ID LOADER ============

def load_existing_ids(path):
    """Return a set of message_id strings already present in the CSV."""
    if not os.path.isfile(path):
        return set()
    ids = set()
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mid = row.get("message_id", "").strip()
            if mid:
                ids.add(mid)
    return ids


def find_latest_id(path):
    """Return the highest (newest) message_id in the CSV, or None."""
    ids = load_existing_ids(path)
    return str(max(int(i) for i in ids)) if ids else None

# ============ API ============
BASE = "https://discord.com/api/v10"


def get(path, params=None):
    """GET with automatic rate-limit retry. User token — no 'Bot ' prefix."""
    headers = {"Authorization": DISCORD_TOKEN}
    while True:
        r = requests.get(f"{BASE}{path}", headers=headers, params=params)
        if r.status_code == 429:
            wait = r.json().get("retry_after", 1.0)
            print(f"  ⏳ Rate limited – waiting {wait:.1f}s …")
            time.sleep(wait)
            continue
        if r.status_code == 401:
            print("❌ 401 Unauthorized – token is wrong or expired.")
            print("   Re-grab it from the browser DevTools Network tab.")
            sys.exit(1)
        if r.status_code == 403:
            print(f"❌ 403 Forbidden – your account can't read channel {CHANNEL_ID}.")
            sys.exit(1)
        r.raise_for_status()
        return r.json()


def fetch_all_messages(after_id=None):
    """
    Fetch messages oldest-first using cursor pagination.

    If *after_id* is given, only messages newer than that ID are fetched.
    """
    messages = []
    before   = None
    batch    = 0

    if after_id:
        # Use after= to only grab messages newer than what we already have.
        # Discord's after= returns newest-first, so we still paginate with
        # after= moving forward and collect everything, then sort.
        cursor = after_id
        while True:
            params = {"limit": 100, "after": cursor}
            chunk = get(f"/channels/{CHANNEL_ID}/messages", params)
            if not chunk:
                break
            messages.extend(chunk)
            # chunk is newest-first; the *largest* id is chunk[0]
            cursor = chunk[0]["id"]
            batch += 1
            print(f"  … batch {batch}: {len(messages)} new messages fetched")
            if len(chunk) < 100:
                break
            time.sleep(0.25)
    else:
        # Full history fetch using before= pagination
        while True:
            params = {"limit": 100}
            if before:
                params["before"] = before
            chunk = get(f"/channels/{CHANNEL_ID}/messages", params)
            if not chunk:
                break
            messages.extend(chunk)
            before = chunk[-1]["id"]
            batch += 1
            print(f"  … batch {batch}: {len(messages)} messages fetched")
            time.sleep(0.25)

    # Sort chronologically (smallest id = oldest)
    messages.sort(key=lambda m: int(m["id"]))
    return messages

# ============ PARSING ============

def _field(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def assemble_embed_text(msg):
    """Flatten a message's embeds into one string for regex parsing."""
    parts = [msg.get("content") or ""]
    for e in msg.get("embeds", []):
        if e.get("author", {}).get("name"):
            parts.append(f"Author: {e['author']['name']}")
        if e.get("title"):
            parts.append(e["title"])
        if e.get("description"):
            parts.append(e["description"])
        for f in e.get("fields", []):
            parts.append(f"{f['name']}\n{f['value']}")
        if e.get("footer", {}).get("text"):
            parts.append(e["footer"]["text"])
    return "\n".join(p for p in parts if p)


def parse(text):
    row = {col: "" for col in COLUMNS}

    m = re.search(r'\b([A-Z]{1,6})\b[^\n\-]*-\s*Alert\s*#(\d+)', text)
    if m:
        row["ticker"]    = m.group(1)
        row["alert_num"] = m.group(2)

    m = re.search(r'Alert\s*#\d+[^\n↑↓]*([↑↓])\s*([\d.]+)%', text)
    if m:
        row["pct_change"] = ('+' if m.group(1) == '↑' else '-') + m.group(2) + '%'

    m = re.search(r'Price\s*[:\n]\s*\$?([\d.]+)', text, re.IGNORECASE)
    if m:
        row["price"] = m.group(1).strip()

    row["mcap"]         = _field(text, r'MCap\s*[:\n]\s*([^\n]+)')
    row["float_shares"] = _field(text, r'Float\s*[:\n]\s*([^\n]+)')
    row["min_vol"]      = _field(text, r'Min\s*Vol\s*[:\n]\s*([^\n]+)')
    row["rel_vol"]      = _field(text, r'Rel\s*Vol\s*[:\n]\s*([^\n]+)')
    row["mon_vol"]      = _field(text, r'Mon\s*Vol\s*[:\n]\s*([^\n]+)')
    row["alert_reason"] = _field(text, r'Alert\s*Reason\s*[:\n]\s*([^\n]+)')

    m = re.search(r'^Author:\s*(.+)$', text, re.MULTILINE)
    if m:
        candidate = m.group(1).strip()
        if '- Alert' not in candidate and 'Price' not in candidate:
            tags = re.sub(r'[^\x00-\x7F]+', ' ', candidate).strip()
            row["tags"] = re.sub(r'\s{2,}', ' ', tags)

    m = re.search(
        r"Today'?s?\s*News\s*[:\n]\s*(.+?)"
        r"(?=\n(?:Today'?s?|Alert\s*Reason|Recent|0\s*Borrow|TrendVision)|$)",
        text, re.IGNORECASE | re.DOTALL
    )
    if m:
        headlines = [h.strip() for h in re.split(r'\s*•\s*', m.group(1).strip()) if h.strip()]
        row["news"] = ' | '.join(headlines)

    # Filings arrive as markdown links: "[FORM 8-K](https://www.sec.gov/...)"
    m = re.search(r"Today'?s?\s*SEC\s*Fil(?:ing)?\s*[:\n]\s*((?:\[?FORM[^\n]+\n?)+)", text, re.IGNORECASE)
    if m:
        row["sec_filing"] = ', '.join(re.findall(r'FORM [^\]\n]+', m.group(1)))

    row["recent_ipo"]  = _field(text, r'Recent\s*IPO\s*[:\n]\s*([^\n]+)')
    row["recent_rs"]   = _field(text, r'Recent\s*R/S\s*[:\n]\s*([^\n]+)')
    row["zero_borrow"] = _field(text, r'0\s*Borrow\s*[:\n]\s*([^\n]+)')

    return row

# ============ MAIN ============

def main():
    if not DISCORD_TOKEN:
        print("❌ DISCORD_TOKEN is not set – add it to .env (see .env.example).")
        sys.exit(1)

    # --- Determine whether to do a full fetch or incremental ---
    latest_id = find_latest_id(OUTPUT_FILE)
    if latest_id:
        print(f"📂 Found existing {OUTPUT_FILE} (latest message_id: {latest_id})")
        print(f"📥 Fetching only new messages from channel {CHANNEL_ID} …\n")
    else:
        print(f"📥 Fetching all messages from channel {CHANNEL_ID} …\n")

    messages = fetch_all_messages(after_id=latest_id)

    if not messages:
        print("\n✅ No new messages – CSV is already up to date.")
        return

    print(f"\n✅ {len(messages)} messages fetched – parsing …\n")

    # --- Parse ---
    rows    = []
    skipped = 0
    for msg in messages:
        if not msg.get("embeds"):
            skipped += 1
            continue

        raw    = assemble_embed_text(msg)
        parsed = parse(raw)

        if not parsed["ticker"]:
            skipped += 1
            continue

        ts = msg.get("timestamp", "")
        if ts:
            ts = (datetime.fromisoformat(ts.replace("Z", "+00:00"))
                          .astimezone(timezone.utc)
                          .strftime("%Y-%m-%d %H:%M:%S UTC"))

        author = msg.get("author", {})
        parsed["message_id"] = msg["id"]
        parsed["timestamp"]  = ts
        parsed["author"]     = author.get("global_name") or author.get("username", "")
        parsed["raw_text"]   = raw.replace("\n", " | ")
        rows.append(parsed)

    print(f"📊 {len(rows)} new alerts parsed, {skipped} messages skipped")

    if not rows:
        print("✅ No new alerts to add.")
        return

    # --- Write: create with header, or append without header ---
    file_exists = os.path.isfile(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    total = len(load_existing_ids(OUTPUT_FILE))
    print(f"✅ Appended {len(rows)} rows → {OUTPUT_FILE}  ({total} total rows)")


if __name__ == "__main__":
    main()