#!/bin/bash

SCRIPT_DIR="$(dirname $(readlink -f $0))"
source "${SCRIPT_DIR}/.venv/bin/activate"

# Initialize the flags
NORETRY=false

# Parse command-line options
while [[ $# -gt 0 ]]; do
    case $1 in
    --clear)
        CLEAR="--clear"
        shift # Move to the next argument
        ;;
    --launch)
        LAUNCH="--launch"
        shift # Move to the next argument
        ;;
    --noretry)
        NORETRY=true
        shift # Move to the next argument
        ;;
    --verbose | -v)
        VERBOSE="-v"
        shift # Move to the next argument
        ;;
    --veryverbose | -vv)
        VERBOSE="-vv"
        shift # Move to the next argument
        ;;
    *)
        break # Stop parsing when we reach the JSON argument
        ;;
    esac
done

JSON=$1
shift
BENCHMARK=${*:-default}

while true; do
    # Call the benchmark script
    "${SCRIPT_DIR}/benchmark.py" ${VERBOSE} ${CLEAR} ${LAUNCH} -j $JSON $BENCHMARK
    EXIT_CODE=$?

    # Do not clear the previous benchmark results
    CLEAR=""

    if [[ $EXIT_CODE -eq 0 ]]; then
        echo "Benchmark succeeded."
        break
    else
        echo "Benchmark failed with exit code $EXIT_CODE. Retrying..."
    fi

    if $NORETRY; then
        echo "Exiting without retrying."
        break
    fi
done
