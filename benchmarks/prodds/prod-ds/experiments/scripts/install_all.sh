#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "$ROOT"

bash experiments/scripts/setup_eval_env.sh
bash experiments/scripts/install_engines.sh

echo "All installs complete."
