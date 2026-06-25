#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
env_file="$script_dir/.env"
log_file="$script_dir/discordless_mitm.log"
pid_file="$script_dir/discordless_mitm.pid"
venv_dir="${MITM_VENV_DIR:-$HOME/mitmproxy-venv}"
venv_python="$venv_dir/bin/python"
venv_pip="$venv_dir/bin/pip"
venv_mitmweb="$venv_dir/bin/mitmweb"
allow_hosts='^(((.+\.)?discord\.com)|((.+\.)?discordapp\.com)|((.+\.)?discord\.net)|((.+\.)?discordapp\.net)|((.+\.)?discord\.gg))(?::\d+)?$'

if [[ -f "$env_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
fi

: "${MITM_WEB_PASSWORD:?MITM_WEB_PASSWORD must be set in .env or the environment}"

if [[ ! -x "$venv_python" ]]; then
  python3 -m venv "$venv_dir"
fi

should_install_deps=false
if [[ ! -x "$venv_mitmweb" ]]; then
  should_install_deps=true
elif ! "$venv_python" -c "import psycopg" >/dev/null 2>&1; then
  should_install_deps=true
fi

if [[ "$should_install_deps" == true ]]; then
  "$venv_pip" install mitmproxy "psycopg[binary]"
fi

nohup "$venv_mitmweb" \
  -s "$script_dir/wumpus_in_the_middle.py" \
  --listen-port="${MITM_LISTEN_PORT:-8080}" \
  --set console_eventlog_verbosity=debug \
  --set "web_password=${MITM_WEB_PASSWORD}" \
  --allow-hosts "$allow_hosts" \
  >"$log_file" 2>&1 &

pid="$!"
echo "$pid" >"$pid_file"

echo "Started mitmweb in the background (PID $pid). Logs: $log_file. PID file: $pid_file"
