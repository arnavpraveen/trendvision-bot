#!/bin/sh
# Cold-restarts IB Gateway when IBC is stuck on a failed re-login.
# IBKR sometimes forces a "Re-login"; when that returns "Unrecognized Username or
# Password", IBC waits on the dialog until its Sunday cold restart (seen Nov 1, 4, 8 2026).
# A full login always works, so: drop any auto-restart token and restart the job.
log=$(ls -t "$HOME"/ibc/logs/ibc-*.txt 2>/dev/null | head -1)
[ -n "$log" ] || exit 0
last=$(grep -E "Login has completed|Unrecognized Username or Password" "$log" | tail -1)
case "$last" in *Unrecognized*) ;; *) exit 0 ;; esac
echo "$(date '+%F %T') stuck on failed re-login: $last"
find "$HOME/Jts" -maxdepth 2 -name autorestart -delete
launchctl kickstart -k "gui/$(id -u)/com.srv.tradingbot.ibgateway"
