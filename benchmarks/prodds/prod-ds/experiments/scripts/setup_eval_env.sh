#!/usr/bin/env bash
set -euo pipefail

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required."
  exit 1
fi

sudo apt-get update
sudo apt-get install -y \
  build-essential cmake ninja-build pkg-config \
  git curl wget unzip ca-certificates \
  python3 python3-venv python3-pip python3-dev \
  jq

cat > requirements.txt <<'REQ'
numpy>=1.26,<3
pandas>=2.2,<3
pyarrow>=15,<20

sqlglot>=24,<30

orjson>=3.9,<4
jsonschema>=4.21,<5

psutil>=5.9,<6
tqdm>=4.66,<5
python-dateutil>=2.9,<3
pyyaml>=6.0,<7

jinja2>=3.1,<4

matplotlib>=3.8,<4
reportlab>=4,<5

click>=8.1,<9
rich>=13.7,<14

scipy>=1.12,<2
tabulate>=0.9,<1
REQ

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

echo "Done."
echo "Activate with: source .venv/bin/activate"
