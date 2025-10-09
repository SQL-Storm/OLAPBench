#!/usr/bin/env bash

# The versions of Umbra to build
versions=(
    23.01
    23.02
    23.03
    23.04
    23.05
    23.06
    23.07
    23.08
    23.09
    23.10
    23.11
    23.12
    24.01
    24.02
    24.03
    24.04
    24.05
    24.06
    24.07
    24.08
    24.09
    24.10
    24.11
    24.12
    25.01
    25.02
    25.03
    25.04
    25.05
    25.06
    25.07
    latest
)

for version in "${versions[@]}"; do
    echo "Building Umbra version ${version}"

    # Build docker image
    if docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/umbra:${version} --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 --build-arg VERSION=${version} .; then
        echo "Successfully built Umbra version ${version}"
    else
        echo "Failed to build Umbra version ${version}" >&2
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
    if docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/umbra:${version}; then
        echo "Successfully pushed Umbra version ${version}"
    else
        echo "Failed to push Umbra version ${version}" >&2
    fi
done
