#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pid_file="$script_dir/discordless_mitm.pid"

if [[ ! -f "$pid_file" ]]; then
  echo "No PID file found at $pid_file. Is mitmweb running?"
  exit 1
fi

pid="$(cat "$pid_file")"
if [[ -z "$pid" ]]; then
  echo "PID file is empty: $pid_file"
  exit 1
fi

if ! kill -0 "$pid" 2>/dev/null; then
  echo "No running process found for PID $pid."
  rm -f "$pid_file"
  exit 1
fi

kill "$pid"
rm -f "$pid_file"
echo "Stopped mitmweb process $pid and removed PID file."
