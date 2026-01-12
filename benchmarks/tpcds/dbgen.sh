#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating TPC-DS database with scale factor $SF"

mkdir -p data/tpcds/
cd data/tpcds/

# Reuse existing datasets
if [ ! -d "sf$SF" ]; then
  (
    echo '7cca604ce38dbbdfbe1a3c849a413f14  tpcds-kit.zip' | md5sum --check --status 2>/dev/null || curl -OL --no-progress-meter https://db.in.tum.de/~schmidt/tpcds-kit.zip
    echo '7cca604ce38dbbdfbe1a3c849a413f14  tpcds-kit.zip' | md5sum --check --status
    unzip -q -u tpcds-kit.zip

    cd tpcds-kit/tools
    rm -rf ./*.dat
    CPPFLAGS=-Wno-implicit-int make -sj "$(nproc)" dsdgen
    ./dsdgen -FORCE -SCALE "$SF"
    mkdir -p "../../sf$SF"
    for table in ./*.dat; do
      sed 's/|$//' "$table" >"../../sf$SF/$table"
      rm "$table"
    done
  )
fi
