# AGENTS.md

## Project
Prod-DS Kit — data- and query-centric extension to TPC-DS.

## Setup
./install.sh && source .venv/bin/activate

## Generate Data & Queries
python3 wrap_dsdgen.py --default        # STR=10, NULL=medium, MCV=medium, SF=10
python3 wrap_dsqgen.py --default        # 107 queries (99 standard + 8 micro-suite)

## Reproduce Paper Results
./reproduce.sh --all --sf 1 --engines all   # Full end-to-end (E1-E5 + plots)

## Run Tests
pytest tests/ -v

## Key Documentation
- README.md — parameters, architecture, extension details
- REPRODUCIBILITY.md — step-by-step reviewer guide
- docs/experimental-protocol.md — frozen evaluation protocol
