import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TPCDS_TOOLS = REPO_ROOT / "tpcds-kit" / "tools"
CPP_BIN = REPO_ROOT / "workload" / "dsdgen" / "stringify_cpp"


def _has_tpcds_tools() -> bool:
    return (TPCDS_TOOLS / "dsdgen").exists() or (TPCDS_TOOLS / "dsdgen.exe").exists()


def _has_cpp_binary() -> bool:
    """Check if stringify_cpp binary exists (requires yaml-cpp and libsodium)."""
    return CPP_BIN.exists()


def pytest_configure(config):
    config.addinivalue_line("markers", "needs_tpcds_tools: requires built TPC-DS toolkit")
    config.addinivalue_line("markers", "needs_engine: requires running database engine")
    config.addinivalue_line("markers", "needs_cpp: requires stringify_cpp binary (yaml-cpp + libsodium)")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "needs_tpcds_tools" in [m.name for m in item.iter_markers()]:
            if not _has_tpcds_tools():
                item.add_marker(pytest.mark.skip(reason="TPC-DS toolkit not built (run install.sh)"))
        if "needs_cpp" in [m.name for m in item.iter_markers()]:
            if not _has_cpp_binary():
                item.add_marker(pytest.mark.skip(
                    reason="stringify_cpp binary not available (install yaml-cpp and libsodium, then run make)"
                ))
