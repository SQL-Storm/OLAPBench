#!/usr/bin/env bash
set -euo pipefail

if ! command -v sudo >/dev/null 2>&1; then
  echo "sudo is required."
  exit 1
fi

ARCH="$(uname -m)"
if [ "$ARCH" = "x86_64" ]; then
  DUCKDB_ARCH="amd64"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
  DUCKDB_ARCH="aarch64"
else
  echo "Unsupported architecture for DuckDB CLI: $ARCH"
  exit 1
fi

install_duckdb_cli() {
  echo "Installing DuckDB CLI"
  mkdir -p ~/opt/duckdb && cd ~/opt/duckdb

  if [ -n "${DUCKDB_VERSION:-}" ]; then
    ZIP="duckdb_cli-linux-${DUCKDB_ARCH}.zip"
    URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${ZIP}"
  else
    URL="https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${DUCKDB_ARCH}.zip"
  fi

  curl -L -o duckdb.zip "$URL"
  unzip -o duckdb.zip
  chmod +x duckdb
  sudo ln -sf "$PWD/duckdb" /usr/local/bin/duckdb
  duckdb --version || true
}

install_clickhouse() {
  echo "Installing ClickHouse"

  sudo apt-get update
  sudo apt-get install -y apt-transport-https ca-certificates curl gnupg

  sudo mkdir -p /usr/share/keyrings

  # Prefer the current documented key URL
  KEYRING="/usr/share/keyrings/clickhouse-keyring.gpg"
  ARCH="$(dpkg --print-architecture)"

  key_ok=0
  for url in \
    "https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key" \
    "https://packages.clickhouse.com/rpm/stable/repodata/repomd.xml.key" \
    "https://packages.clickhouse.com/CLICKHOUSE-KEY.GPG"
  do
    echo "Fetching ClickHouse key: $url"
    if curl -fLsS -A "Mozilla/5.0" "$url" | sudo gpg --dearmor -o "$KEYRING"; then
      key_ok=1
      break
    fi
  done

  if [ "$key_ok" -ne 1 ]; then
    echo "Key download failed. Trying keyserver fallback (deprecated, but sometimes works)."
    sudo apt-get install -y dirmngr >/dev/null 2>&1 || true
    if sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754; then
      echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list >/dev/null
      sudo apt-get update
      sudo apt-get install -y clickhouse-server clickhouse-client
      sudo systemctl enable --now clickhouse-server
      clickhouse-client --query "SELECT version()" || true
      return 0
    fi

    echo "ERROR: Could not obtain ClickHouse signing key."
    echo "If you are behind a restrictive network, use Docker mode for ClickHouse, or download the key via a different network."
    return 1
  fi

  echo "deb [signed-by=${KEYRING} arch=${ARCH}] https://packages.clickhouse.com/deb stable main" | \
    sudo tee /etc/apt/sources.list.d/clickhouse.list >/dev/null

  sudo apt-get update
  sudo apt-get install -y clickhouse-server clickhouse-client
  sudo systemctl enable --now clickhouse-server

  clickhouse-client --query "SELECT version()" || true
}


install_postgresql() {
  echo "Installing PostgreSQL"
  sudo apt-get update
  sudo apt-get install -y postgresql postgresql-client
  sudo systemctl enable --now postgresql
  psql --version || true
}

install_cedardb() {
  echo "Installing CedarDB (standalone binary)"
  sudo apt-get update
  sudo apt-get install -y curl ca-certificates postgresql-client

  mkdir -p ~/opt/cedardb && cd ~/opt/cedardb
  # Downloads and decompresses the appropriate CedarDB binary
  curl -sSL https://get.cedardb.com | bash
  chmod +x cedardb
  sudo ln -sf "$PWD/cedardb" /usr/local/bin/cedardb

  echo "CedarDB installed at /usr/local/bin/cedardb"
  echo "Basic usage examples:"
  echo "  Start server and create db dir: cedardb --createdb mydb"
  echo "  Connect locally via socket:     psql -h /tmp -U postgres"
  echo "  Enable remote connections:      cedardb mydb --address=::"
  cedardb --help >/dev/null || true
}

install_duckdb_cli
install_clickhouse
install_postgresql
install_cedardb

echo "Done."
