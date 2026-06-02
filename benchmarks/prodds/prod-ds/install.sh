#!/usr/bin/env bash
# install.sh — set up Prod-DS Kit and build the TPC-DS toolkit.
#
# Idempotent: safe to re-run. Auto-detects Linux vs macOS for the build step.
# By running this script you agree to the TPC End User License Agreement
# (see NOTICE.md for details).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
TPCDS_KIT_DIR="$REPO_ROOT/tpcds-kit"
TPCDS_KIT_COMMIT="5a3a81796992b725c2a8b216767e142609966752"
TPCDS_KIT_REPO="https://github.com/gregrahn/tpcds-kit.git"
VENV_DIR="$REPO_ROOT/.venv"

# ---------- helpers ----------
info()  { printf '\033[1;34m[install]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[install]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[install]\033[0m %s\n' "$*"; }
fail()  { printf '\033[1;31m[install]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------- Step 1: Check Python >= 3.9 ----------
info "Checking Python version..."
PYTHON=""
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
        ver=$("$candidate" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        major=${ver%%.*}
        minor=${ver#*.}
        if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
            PYTHON="$candidate"
            break
        fi
    fi
done
[ -n "$PYTHON" ] || fail "Python >= 3.9 is required. Found none."
ok "Using $PYTHON ($ver)"

# ---------- Step 2: Create virtual environment ----------
if [ ! -d "$VENV_DIR" ]; then
    info "Creating virtual environment in .venv/"
    "$PYTHON" -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "Virtual environment active: $VENV_DIR"

# ---------- Step 3: Install Python package ----------
info "Installing Prod-DS Kit (editable) with test dependencies..."
pip install --upgrade pip --quiet
pip install -e ".[test]" --quiet
ok "Python package installed."

# ---------- Step 4: Clone TPC-DS toolkit ----------
if [ ! -d "$TPCDS_KIT_DIR" ]; then
    info "Cloning TPC-DS toolkit (gregrahn/tpcds-kit)..."
    git clone --quiet "$TPCDS_KIT_REPO" "$TPCDS_KIT_DIR"
    (cd "$TPCDS_KIT_DIR" && git checkout --quiet "$TPCDS_KIT_COMMIT")
    ok "TPC-DS toolkit cloned at pinned commit ${TPCDS_KIT_COMMIT:0:12}."
else
    info "TPC-DS toolkit already present at $TPCDS_KIT_DIR"
    CURRENT_COMMIT=$(cd "$TPCDS_KIT_DIR" && git rev-parse HEAD 2>/dev/null || echo "unknown")
    if [ "$CURRENT_COMMIT" != "$TPCDS_KIT_COMMIT" ]; then
        warn "Pinned commit: $TPCDS_KIT_COMMIT"
        warn "Current commit: $CURRENT_COMMIT"
        warn "Consider removing tpcds-kit/ and re-running install.sh to update."
    fi
fi

# ---------- Step 5: Build dsdgen and dsqgen ----------
info "Building TPC-DS tools (dsdgen, dsqgen)..."
case "$(uname -s)" in
    Linux*)  OS_FLAG="LINUX" ;;
    Darwin*) OS_FLAG="MACOS" ;;
    *)       fail "Unsupported OS: $(uname -s). Supported: Linux, macOS." ;;
esac

# Patch PARAM_MAX_LEN: default 80 bytes silently truncates long paths.
if grep -q 'PARAM_MAX_LEN.*80' "$TPCDS_KIT_DIR/tools/r_params.c" 2>/dev/null; then
    info "Patching r_params.c: PARAM_MAX_LEN 80 → 4096"
    sed -i.bak 's/PARAM_MAX_LEN\t80/PARAM_MAX_LEN\t4096/' "$TPCDS_KIT_DIR/tools/r_params.c"
    rm -f "$TPCDS_KIT_DIR/tools/r_params.c.bak"
fi

# macOS: ensure bison and flex are available (Xcode CLT yacc often fails)
if [ "$OS_FLAG" = "MACOS" ]; then
    NEED_BREW_BISON=false
    if ! bison --version >/dev/null 2>&1; then
        NEED_BREW_BISON=true
    fi
    if [ "$NEED_BREW_BISON" = true ]; then
        BREW_BISON="/opt/homebrew/opt/bison/bin"
        BREW_FLEX="/opt/homebrew/opt/flex/bin"
        if [ -d "$BREW_BISON" ]; then
            info "Using Homebrew bison/flex"
            export PATH="$BREW_BISON:$BREW_FLEX:$PATH"
        else
            fail "bison is required but not found. Install via: brew install bison flex"
        fi
    fi
fi

# Modern compilers reject old K&R-style C and duplicate globals; add permissive flags
EXTRA_CFLAGS="-fcommon"
if cc -Wno-implicit-int -x c -c /dev/null -o /dev/null 2>/dev/null; then
    EXTRA_CFLAGS="$EXTRA_CFLAGS -Wno-implicit-int -Wno-implicit-function-declaration -Wno-return-type"
fi
echo "(cd \"$TPCDS_KIT_DIR/tools\" && make clean >/dev/null 2>&1 || true && make OS=\"$OS_FLAG\" MACOS_CFLAGS=\"-g -Wall ${EXTRA_CFLAGS}\" LINUX_CFLAGS=\"-g -Wall ${EXTRA_CFLAGS}\" -j\"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)\" >/dev/null 2>&1)"
(cd "$TPCDS_KIT_DIR/tools" && make clean >/dev/null 2>&1 || true && make OS="$OS_FLAG" MACOS_CFLAGS="-g -Wall ${EXTRA_CFLAGS}" LINUX_CFLAGS="-g -Wall ${EXTRA_CFLAGS}" -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)" >/dev/null 2>&1)
ok "TPC-DS tools built for $OS_FLAG."

# ---------- Step 6: Verify binaries ----------
for bin in dsdgen dsqgen; do
    if [ ! -f "$TPCDS_KIT_DIR/tools/$bin" ]; then
        fail "Expected binary not found: tpcds-kit/tools/$bin"
    fi
done
ok "Verified: dsdgen and dsqgen binaries present."

# ---------- Step 7: Optionally build stringify_cpp ----------
STRINGIFY_CPP_DIR="$REPO_ROOT/workload/dsdgen"
if [ -f "$STRINGIFY_CPP_DIR/Makefile" ]; then
    if command -v g++ >/dev/null 2>&1 || command -v clang++ >/dev/null 2>&1; then
        # Install yaml-cpp and libsodium (required by stringify_cpp)
        info "Checking stringify_cpp dependencies (yaml-cpp, libsodium)..."
        _install_stringify_deps() {
            if [ "$OS_FLAG" = "MACOS" ]; then
                if command -v brew >/dev/null 2>&1; then
                    brew list yaml-cpp >/dev/null 2>&1 || brew install yaml-cpp 2>/dev/null
                    brew list libsodium >/dev/null 2>&1 || brew install libsodium 2>/dev/null
                    # Homebrew keg-only: set search paths (Makefile uses ?= so we must not
                    # override CXXFLAGS/LDFLAGS which would clobber -std=c++17)
                    export CPLUS_INCLUDE_PATH="/opt/homebrew/include${CPLUS_INCLUDE_PATH:+:$CPLUS_INCLUDE_PATH}"
                    export LIBRARY_PATH="/opt/homebrew/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
                else
                    warn "Homebrew not found. Install yaml-cpp and libsodium manually: brew install yaml-cpp libsodium"
                fi
            elif command -v apt-get >/dev/null 2>&1; then
                dpkg -s libyaml-cpp-dev >/dev/null 2>&1 || sudo apt-get install -y libyaml-cpp-dev 2>/dev/null || true
                dpkg -s libsodium-dev >/dev/null 2>&1 || sudo apt-get install -y libsodium-dev 2>/dev/null || true
            elif command -v dnf >/dev/null 2>&1; then
                rpm -q yaml-cpp-devel >/dev/null 2>&1 || sudo dnf install -y yaml-cpp-devel 2>/dev/null || true
                rpm -q libsodium-devel >/dev/null 2>&1 || sudo dnf install -y libsodium-devel 2>/dev/null || true
            elif command -v yum >/dev/null 2>&1; then
                rpm -q yaml-cpp-devel >/dev/null 2>&1 || sudo yum install -y yaml-cpp-devel 2>/dev/null || true
                rpm -q libsodium-devel >/dev/null 2>&1 || sudo yum install -y libsodium-devel 2>/dev/null || true
            else
                warn "Unknown package manager. Please install yaml-cpp and libsodium development headers manually."
            fi
        }
        _install_stringify_deps

        info "Building stringify_cpp (optional C++ accelerator)..."
        if (cd "$STRINGIFY_CPP_DIR" && make stringify_cpp 2>&1); then
            ok "stringify_cpp built successfully."
        else
            warn "stringify_cpp build failed (non-fatal). Python fallback will be used."
            warn "The Python fallback is significantly slower (10-50x) due to GIL limitations."
            warn "Ensure yaml-cpp and libsodium development headers are installed and try again."
        fi
    else
        info "No C++ compiler found. Skipping stringify_cpp (Python fallback will be used)."
    fi
fi

# ---------- Step 8: Bootstrap schema files ----------
if [ ! -f "$REPO_ROOT/tools/tpcds.sql" ]; then
    info "Copying TPC-DS base schema to tools/tpcds.sql..."
    cp "$TPCDS_KIT_DIR/tools/tpcds.sql" "$REPO_ROOT/tools/tpcds.sql"
    ok "tools/tpcds.sql copied."
else
    info "tools/tpcds.sql already present."
fi

if [ ! -f "$REPO_ROOT/tools/prodds.sql" ]; then
    info "Bootstrapping tools/prodds.sql from TPC-DS base schema..."
    cp "$TPCDS_KIT_DIR/tools/tpcds.sql" "$REPO_ROOT/tools/prodds.sql"
    "$PYTHON" "$REPO_ROOT/tools/update_prodds_schema.py" \
        --base-schema "$TPCDS_KIT_DIR/tools/tpcds.sql" \
        --prod-schema "$REPO_ROOT/tools/prodds.sql" >/dev/null 2>&1
    ok "tools/prodds.sql generated."
else
    info "tools/prodds.sql already present."
fi

info "Generating query_templates/templates.lst from TPC-DS toolkit..."
if [ -f "$TPCDS_KIT_DIR/query_templates/templates.lst" ]; then
    cp "$TPCDS_KIT_DIR/query_templates/templates.lst" "$REPO_ROOT/query_templates/templates.lst"
    ok "templates.lst copied from TPC-DS toolkit."
else
    (cd "$TPCDS_KIT_DIR/query_templates" && ls query*.tpl 2>/dev/null | sort -V > "$REPO_ROOT/query_templates/templates.lst")
    ok "templates.lst generated from TPC-DS template directory."
fi

# Copy all base TPC-DS templates (query*.tpl + dialect/helper templates)
info "Copying base TPC-DS templates into query_templates/..."
COPIED=0
for tpl in "$TPCDS_KIT_DIR"/query_templates/*.tpl; do
    base="$(basename "$tpl")"
    if [ ! -f "$REPO_ROOT/query_templates/$base" ]; then
        cp "$tpl" "$REPO_ROOT/query_templates/$base"
        COPIED=$((COPIED + 1))
    fi
done
ok "Copied $COPIED base TPC-DS templates."

# ---------- Step 9: Smoke test ----------
info "Running smoke test..."
"$PYTHON" -c "from workload import stringification; print('Prod-DS Kit installed successfully.')"

# ---------- Step 10: Verify installation completeness ----------
info "Verifying installation..."
INSTALL_OK=true
MISSING=""
for expected in \
    "$TPCDS_KIT_DIR/tools/dsdgen" \
    "$TPCDS_KIT_DIR/tools/dsqgen" \
    "$REPO_ROOT/tools/tpcds.sql" \
    "$REPO_ROOT/tools/prodds.sql" \
    "$REPO_ROOT/query_templates/templates.lst"; do
    if [ ! -f "$expected" ]; then
        INSTALL_OK=false
        MISSING="$MISSING\n  - $expected"
    fi
done
if [ "$INSTALL_OK" = true ]; then
    touch "$REPO_ROOT/.install_complete"
    ok "All expected outputs verified."
else
    rm -f "$REPO_ROOT/.install_complete"
    warn "Installation incomplete. Missing files:$MISSING"
    warn "Try removing tpcds-kit/ and re-running install.sh."
fi

# ---------- Summary ----------
echo ""
echo "============================================="
echo "  Prod-DS Kit installation complete."
echo "============================================="
echo ""
echo "  Python:          $PYTHON ($ver)"
echo "  Virtual env:     $VENV_DIR"
echo "  TPC-DS toolkit:  $TPCDS_KIT_DIR"
echo "  dsdgen:          $TPCDS_KIT_DIR/tools/dsdgen"
echo "  dsqgen:          $TPCDS_KIT_DIR/tools/dsqgen"
echo ""
echo "  Quick start:"
echo "    source .venv/bin/activate"
echo "    python3 wrap_dsdgen.py --default"
echo "    python3 wrap_dsqgen.py --output-dir ./queries --stringification-level 10"
echo ""
