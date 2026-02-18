#!/usr/bin/env zsh
set -euo pipefail

cd /Users/joesmart/data/ur-sprider
exec python3 /Users/joesmart/data/ur-sprider/scan_ur_api.py --config /Users/joesmart/data/ur-sprider/scan_config.json
