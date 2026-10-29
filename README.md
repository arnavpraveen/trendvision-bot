# TrendVision Bot

Automated trading bot that reads TrendVision volume alerts from a Discord channel and executes trades through Interactive Brokers (IBKR).

---

## Prerequisites

- Python 3.8+
- An active Interactive Brokers account with API access enabled
- A Discord account (user token) with access to the TrendVision alert channel

---

## Installation

Create and activate a virtual environment, then install dependencies into it:

```bash
python3 -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
pip install "discord.py<2.0" ib_insync pandas numpy requests
```

> **Important — Discord version:** The bot requires `discord.py < 2.0`. Version 2.0+ changed how message content is accessed (requires explicit intents) and will break the alert parsing logic without additional code changes. Always pin to `<2.0`.

---

## Discord Setup

The bot logs in as **your personal Discord account** (a user token, not a bot token), which is why it needs `discord.py < 2.0`.

1. Open [discord.com/app](https://discord.com/app) in a browser and log in.
2. Press F12 → **Network** tab → refresh the page.
3. Click any request to `discord.com` and copy the **Authorization** request header.
4. Copy `.env.example` to `.env` and paste the value after `DISCORD_TOKEN=`.

`.env` is gitignored. Never commit the token; changing your Discord password invalidates it.

---

## IBKR Setup

1. Install and open **TWS** (Trader Workstation) or **IBKR Gateway**.
2. In TWS/Gateway: **File > Global Configuration > API > Settings**
   - Enable **Enable ActiveX and Socket Clients**
   - Set **Socket port**: `7497` for TWS, `4002` for Gateway (must match `IBKR_PORT` in the config)
   - Optionally uncheck **Read-Only API** to allow order submission
3. Keep TWS/Gateway running while the bot is active.

---

## Configuration

All config is at the top of [discord_ibkr_bot.py](discord_ibkr_bot.py) (lines 76–100). Edit these values before running:

```python
# ============ CONFIGURATION ============
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")  # read from .env
CHANNEL_ID    = 1467323184905850921       # ID of the TrendVision alert channel

# IBKR connection
IBKR_HOST      = "127.0.0.1"
IBKR_PORT      = 4002          # 4002 = Gateway, 7497 = TWS
IBKR_CLIENT_ID = 1

# Trading
TRADE_DIRECTION = 'SHORT'      # 'LONG' or 'SHORT'
TRADE_AMOUNT    = 100          # dollars per trade
```

To get the channel ID: right-click the channel in Discord > **Copy Channel ID** (requires Developer Mode enabled in Discord settings).

---

## Running the Bot

Make sure the venv is activated and IBKR TWS or Gateway is already running, then:

```bash
source venv/bin/activate        # if not already active
python3 discord_ibkr_bot.py
```

---

## Utility Scripts

| Script | Purpose |
|--------|---------|
| `discord_test.py` | Test that the Discord token in .env logs in |
| `ibkr_test.py` | Test IBKR order submission and stop order mechanics |
| `export_discord_channel.py` | Export Discord channel messages to CSV for backtesting |
| `analyze_results.py` | Analyze trading performance from trade history CSV |
| `exit_strategy_backtest.py` | Simulate different exit strategies on historical data |
| `trade_inspector.py` | Deep per-trade analysis |
| `chart_trades.py` | Generate intraday price charts with entry/exit markers |
| `visualize_backtest.py` | Generate a markdown report from backtest results |
