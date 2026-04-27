#!/usr/bin/env bash
# Launcher for EnLogosGRAG. Installs dependencies on first run, starts
# the server in this terminal, and opens the browser once it's listening.
#
# Usage: scripts/run.sh
#        scripts/run.sh --no-open        # skip the browser launch
#        PORT=9000 scripts/run.sh        # override the listen port

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT_TO_OPEN="${PORT:-8780}"

# Verify Node is on PATH and >= 22 (for built-in node:sqlite).
if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: Node.js is not on PATH. Install Node 22+ from https://nodejs.org/" >&2
  exit 1
fi
NODE_VER="$(node --version)"
echo "Node version: $NODE_VER"
NODE_MAJOR="${NODE_VER#v}"
NODE_MAJOR="${NODE_MAJOR%%.*}"
if [ "$NODE_MAJOR" -lt 22 ]; then
  echo "ERROR: Node 22+ is required for node:sqlite. Found $NODE_VER." >&2
  exit 1
fi

# Install dependencies on first run.
if [ ! -d "$REPO_DIR/node_modules" ]; then
  echo "Installing dependencies..."
  (cd "$REPO_DIR" && npm install)
fi

# Open the browser in the background after a short delay. Skipped on
# --no-open or when no opener is available (headless servers).
if [ "${1:-}" != "--no-open" ]; then
  url="http://localhost:$PORT_TO_OPEN/"
  if   command -v xdg-open >/dev/null 2>&1; then opener=xdg-open
  elif command -v open     >/dev/null 2>&1; then opener=open
  elif command -v start    >/dev/null 2>&1; then opener=start
  else opener=""
  fi
  if [ -n "$opener" ]; then
    ( sleep 2 && "$opener" "$url" >/dev/null 2>&1 ) &
  fi
fi

echo "Starting EnLogosGRAG on http://localhost:$PORT_TO_OPEN/"
echo "Press Ctrl-C to stop."
cd "$REPO_DIR"
exec npm start
