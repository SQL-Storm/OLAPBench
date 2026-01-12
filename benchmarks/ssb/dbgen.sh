#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}

echo "Generating Star Schema Benchmark with scale factor $SF"

mkdir -p data/ssb/
cd data/ssb/

# Reuse existing datasets
if [ ! -d "sf$SF" ]; then
  (
    # Originally from: https://www.cs.umb.edu/%7Eponeil/dbgen.zip
    echo 'd37618c646a6918be8ccc4bc79704061  dbgen.zip' | md5sum --check --status 2>/dev/null || curl -OL https://db.in.tum.de/~fent/dbgen/ssb/dbgen.zip
    echo 'd37618c646a6918be8ccc4bc79704061  dbgen.zip' | md5sum --check --status
    unzip -u dbgen.zip

    cd dbgen
    rm -rf ./*.tbl
    sed -i 's/#define  MAXAGG_LEN    10/#define  MAXAGG_LEN    20/' shared.h
    CPPFLAGS="-Wno-implicit-int -Wno-implicit-function-declaration" MACHINE=LINUX make -sj "$(nproc)" CC=gcc-13 dbgen
    ./dbgen -f -T c -s "$SF"
    ./dbgen -qf -T d -s "$SF"
    ./dbgen -qf -T p -s "$SF"
    ./dbgen -qf -T s -s "$SF"
    ./dbgen -q -T l -s "$SF"
    mkdir -p "../sf$SF"
    for table in ./*.tbl; do
      sed 's/|$//' "$table" >"../sf$SF/$table"
      rm "$table"
    done
  )
fi
