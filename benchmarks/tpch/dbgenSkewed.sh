#!/usr/bin/env bash
set -euo pipefail
SF=${1:-1}
SKEW=${2:-2}

echo "Generating Skewed TPC-H database with scale factor $SF and zipfian skew $SKEW"

mkdir -p data/tpch/
cd data/tpch/

# Reuse existing datasets
if [ ! -d "../sf${SF}skew${SKEW}" ]; then
  (
    # Originally from: https://download.microsoft.com/download/6/A/A/6AA77214-3402-457E-938A-E7A1C737639A/TPCDSkew.zip
    echo '99ee2268fabc690cf74684deb2f8559c  TPCDSkew.zip' | md5sum --check --status 2>/dev/null || curl -OL https://db.in.tum.de/~fent/dbgen/tpch/TPCDSkew.zip
    echo '99ee2268fabc690cf74684deb2f8559c  TPCDSkew.zip' | md5sum --check --status
    unzip -q -u TPCDSkew.zip

    cd TPCDSkew
    rm -rf ./*.tbl
    mv -f makefile.suite makefile
    sed -i 's/DATABASE=.*/DATABASE=DB2/' makefile
    sed -i 's/PLATFORM=.*/PLATFORM=LINUX/' makefile
    sed -i '/CFLAGS/ s/$/ -O3/' makefile
    echo '
#ifndef DSS_HUGE
#define DSS_HUGE        long
#define HUGE_COUNT      2
#endif' >>config.h
    echo '
#ifdef LINUX
#define STDLIB_HAS_GETOPT
#endif /* LINUX */' >>config.h
    make -sj "$(nproc)" dbgen 2>/dev/null
    ./dbgen -f -s "$SF" -z "$SKEW"
    mkdir -p "../sf${SF}skew${SKEW}"
    for table in ./*.tbl; do
      sed 's/|$//' "$table" >"../sf${SF}skew${SKEW}/$table"
      rm "$table"
    done
    ln -s ../sf${SF}skew${SKEW}/order.tbl ../sf${SF}skew${SKEW}/orders.tbl
  )
fi
