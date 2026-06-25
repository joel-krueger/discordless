#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pid_file="$script_dir/discordless_mitm.pid"
max_wait_iterations=10
sleep_interval=0.2

if [[ ! -f "$pid_file" ]]; then
  echo "No PID file found at $pid_file. Is mitmweb running?"
  exit 1
fi

pid="$(cat "$pid_file")"
if [[ -z "$pid" ]]; then
  echo "PID file is empty: $pid_file"
  exit 1
fi

if [[ ! "$pid" =~ ^[1-9][0-9]*$ ]]; then
  echo "Invalid PID in $pid_file: $pid"
  exit 1
fi

if ! kill -0 "$pid" 2>/dev/null; then
  echo "No running process found for PID $pid."
  rm -f "$pid_file"
  exit 1
fi

kill "$pid"

for _ in $(seq 1 "$max_wait_iterations"); do
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    echo "Stopped mitmweb process $pid and removed PID file."
    exit 0
  fi
  sleep "$sleep_interval"
done

echo "Process $pid did not stop after SIGTERM; sending SIGKILL."
kill -9 "$pid"
for _ in $(seq 1 "$max_wait_iterations"); do
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$pid_file"
    echo "Stopped mitmweb process $pid with SIGKILL and removed PID file."
    exit 0
  fi
  sleep "$sleep_interval"
done

echo "Failed to stop process $pid."
exit 1
