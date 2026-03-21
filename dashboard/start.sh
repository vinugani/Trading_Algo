#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
# Delta Exchange Bot Dashboard
# Run from: Algo_Trading/
#   bash dashboard/start.sh
# ─────────────────────────────────────────────────────────────────
set -e
cd "$(dirname "$0")/.."   # always runs from Algo_Trading/

echo ""
echo "  ⬡  DELTA BOT DASHBOARD"
echo "  ───────────────────────"

# Install only what's needed (fastapi + uvicorn)
echo "  Installing backend deps..."
python -m pip install fastapi uvicorn --break-system-packages -q 2>/dev/null || \
python -m pip install fastapi uvicorn -q 2>/dev/null || true

echo ""
echo "  Backend  →  http://localhost:8080"
echo "  API docs →  http://localhost:8080/docs"
echo "  Dashboard→  http://localhost:8080/ui/index.html"
echo ""
echo "  Press Ctrl+C to stop."
echo ""

python dashboard/server.py
