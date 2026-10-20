#!/bin/bash
# Activate a staged opencode2api binary once the gateway and the agent
# harness are quiet.
#
# A plain `systemctl restart` during an active model stream cuts the very
# turn that requested it, because this T3 session's provider is the
# gateway itself. This helper waits a minimum arming delay, then requires a
# stable quiet window (no OpenCode message writes and no meaningful
# client-facing byte flow on 127.0.0.1:8080) before restarting, so only idle
# connections are dropped. If the window never arrives it leaves the service
# running and says so in the log.
#
# Usage: systemd-run --user --collect --unit=o2a-activate-restart \
#          /home/ubuntu/code/opencode2api/scripts/activate-restart.sh
set -u

SERVICE="${O2A_SERVICE:-opencode2api-ipv6.service}"
HOME_DIR="${HOME:-/home/ubuntu}"
LOG="${O2A_ACTIVATION_LOG:-$HOME_DIR/.local/share/o2a/activate-restart.log}"
WAL="${OPENCODE_DB_WAL:-$HOME_DIR/.local/share/opencode/opencode.db-wal}"
DB="${OPENCODE_DB:-$HOME_DIR/.local/share/opencode/opencode.db}"
MIN_DELAY="${O2A_ACTIVATION_MIN_DELAY:-90}"
SAMPLE_SECONDS="${O2A_ACTIVATION_SAMPLE_SECONDS:-5}"
QUIET_SAMPLES="${O2A_ACTIVATION_QUIET_SAMPLES:-6}"   # 6 x 5s = 30s quiet
MAX_SAMPLES="${O2A_ACTIVATION_MAX_SAMPLES:-720}"    # give up after an hour
BYTE_GROWTH_LIMIT="${O2A_ACTIVATION_BYTE_LIMIT:-16384}"

log() { printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$LOG"; }

log "armed: min ${MIN_DELAY}s, then $((QUIET_SAMPLES * SAMPLE_SECONDS))s of no agent writes and no 8080 byte growth"
sleep "$MIN_DELAY"

wal_mtime=$(stat -c %Y "$WAL" 2>/dev/null || echo 0)
db_mtime=$(stat -c %Y "$DB" 2>/dev/null || echo 0)
prev_bytes=""
quiet=0
for _ in $(seq 1 "$MAX_SAMPLES"); do
	wal_now=$(stat -c %Y "$WAL" 2>/dev/null || echo 0)
	db_now=$(stat -c %Y "$DB" 2>/dev/null || echo 0)
	if [ "$wal_now" != "$wal_mtime" ] || [ "$db_now" != "$db_mtime" ]; then
		quiet=0
	else
		quiet=$((quiet + 1))
	fi
	wal_mtime=$wal_now
	db_mtime=$db_now

	bytes=$(ss -Htin state established '( sport = :8080 )' 2>/dev/null |
		grep -oE 'bytes_(sent|received):[0-9]+' | awk -F: '{s += $2} END {print s + 0}')
	if [ -n "$prev_bytes" ] && [ $((bytes - prev_bytes)) -gt "$BYTE_GROWTH_LIMIT" ]; then
		quiet=0
	fi
	prev_bytes=$bytes

	if [ "$quiet" -ge "$QUIET_SAMPLES" ]; then
		log "quiet for $((quiet * SAMPLE_SECONDS))s; restarting ${SERVICE}"
		if systemctl --user restart "$SERVICE" >>"$LOG" 2>&1; then
			log "restart ok; service=$(systemctl --user is-active "$SERVICE" 2>&1)"
		else
			log "restart failed rc=$?"
		fi
		exit 0
	fi
	sleep "$SAMPLE_SECONDS"
done
log "gateway never went quiet for the required window; leaving the service running"
