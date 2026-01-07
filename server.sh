#!/usr/bin/env bash

# Activate virtual environment
source .venv/bin/activate

# Run the HTTP server
python3 server.py "$@"
