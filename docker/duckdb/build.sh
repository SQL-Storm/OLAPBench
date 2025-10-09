#!/usr/bin/env bash

# The versions of DuckDB to build
versions=(
    0.7.0
    0.7.1
    0.8.0
    0.8.1
    0.9.0
    0.9.1
    0.9.2
    0.10.0
    0.10.1
    0.10.2
    0.10.3
    1.0.0
    1.1.0
    1.1.1
    1.1.2
    1.1.3
    1.2.0
    1.2.1
    1.2.2
    1.3.0
    latest
)

for version in "${versions[@]}"; do
    if [ "$version" = "latest" ]; then
        duckdb_version=$(pip index versions duckdb 2>/dev/null | grep -oP 'Available versions: \K[^,]+' | head -1)
    else
        duckdb_version=$version
    fi

    echo "Building DuckDB version ${version} (${duckdb_version})"
    # Build docker image
    if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/duckdb:${version} --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 --build-arg VERSION="${duckdb_version}" .; then
        echo "Successfully built DuckDB version ${version}"
    else
        echo "Failed to build DuckDB version ${version}" >&2
        continue
    fi

    # Login to docker registry
    if docker login gitlab.db.in.tum.de:5005; then
        echo "Successfully logged in to docker registry"
    else
        echo "Failed to login to docker registry" >&2
        continue
    fi

    # Push docker image
    if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/duckdb:${version}; then
        echo "Successfully pushed DuckDB version ${version}"
    else
        echo "Failed to push DuckDB version ${version}" >&2
    fi
done
