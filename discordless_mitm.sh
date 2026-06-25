#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
env_file="$script_dir/.env"
log_file="$script_dir/discordless_mitm.log"
pid_file="$script_dir/discordless_mitm.pid"
allow_hosts='^(((.+\.)?discord\.com)|((.+\.)?discordapp\.com)|((.+\.)?discord\.net)|((.+\.)?discordapp\.net)|((.+\.)?discord\.gg))(?::\d+)?$'

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

: "${MITM_WEB_PASSWORD:?MITM_WEB_PASSWORD must be set in .env or the environment}"

nohup mitmweb \
  -s "$script_dir/wumpus_in_the_middle.py" \
  --listen-port="${MITM_LISTEN_PORT:-8080}" \
  --set console_eventlog_verbosity=debug \
  --set "web_password=${MITM_WEB_PASSWORD}" \
  --allow-hosts "$allow_hosts" \
  >"$log_file" 2>&1 &

pid="$!"
echo "$pid" >"$pid_file"

echo "Started mitmweb in the background (PID $pid). Logs: $log_file. PID file: $pid_file"
