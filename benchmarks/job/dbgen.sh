#!/usr/bin/env bash
set -euo pipefail

echo "Generating JOB database"

mkdir -p data
cd data

# Reuse existing datasets
if [ ! -d "job" ]; then
  (
    mkdir job
    cd job

    echo '552a24e5bbe0b9bd727649200294bbac imdb.tzst' | md5sum --check --status 2>/dev/null || curl -OL https://db.in.tum.de/~schmidt/dbgen/job/imdb.tzst
    echo '552a24e5bbe0b9bd727649200294bbac imdb.tzst' | md5sum --check --status
    tar --skip-old-files -xf imdb.tzst
    rm imdb.tzst

    for file in *.csv; do
      sed -E -i 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$file"
      sed -E -i 's/([^\\])(\\{2})*(\\)(")/\1\2""/g' "$file"
    done
  )
fi
