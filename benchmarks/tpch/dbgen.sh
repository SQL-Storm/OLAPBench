#!/usr/bin/env bash
set -euo pipefail
set -o xtrace

SF=${1:-1}

echo "Generating TPC-H database with scale factor $SF"

mkdir -p data/tpch/
cd data/tpch/

# Reuse existing datasets
if [ ! -d "sf$SF" ]; then
  (
    echo '4e238abf675c5e9aef224c378b043955  tpch-kit.zip' | md5sum --check --status 2>/dev/null || curl -OL --no-progress-meter https://db.in.tum.de/~fent/dbgen/tpch/tpch-kit.zip
    echo '4e238abf675c5e9aef224c378b043955  tpch-kit.zip' | md5sum --check --status
    unzip -q -u tpch-kit.zip

    cd tpch-kit-852ad0a5ee31ebefeed884cea4188781dd9613a3/dbgen
    rm -rf ./*.tbl
    MACHINE=LINUX make -sj "$(nproc)" dbgen 2>/dev/null
    ./dbgen -f -s "$SF"
    mkdir -p "../../sf$SF"
    for table in ./*.tbl; do
      sed 's/|$//' "$table" >"../../sf$SF/$table"
      rm "$table"
    done
  )
fi
