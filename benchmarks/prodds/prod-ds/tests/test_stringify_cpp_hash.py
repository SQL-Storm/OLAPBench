import hashlib
import os
import subprocess
import sys
import unittest
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CPP_BIN = REPO_ROOT / "workload" / "dsdgen" / "stringify_cpp"
CPP_DIR = CPP_BIN.parent


def _python_hash_u64(seed: str, parts: list[str]) -> int:
    hasher = hashlib.blake2b(digest_size=8)
    hasher.update(seed.encode("utf-8"))
    for part in parts:
        hasher.update(b"\x1f")
        hasher.update(part.encode("utf-8", errors="backslashreplace"))
    return int.from_bytes(hasher.digest(), "big")


def _ensure_cpp_binary() -> None:
    subprocess.run(["make", "stringify_cpp"], cwd=str(CPP_DIR), check=True)


@pytest.mark.needs_cpp
class StringifyCppHashTests(unittest.TestCase):
    def test_cpp_hash_matches_python(self) -> None:
        _ensure_cpp_binary()

        vectors = [
            ("0", []),
            ("1", ["table", "column"]),
            ("42", ["table", "column", "select"]),
            ("123", ["t", "c", "mcv", "5"]),
            ("999", ["store_sales", "ss_ticket_number", "select-mcv"]),
        ]

        for seed, parts in vectors:
            expected = _python_hash_u64(seed, parts)
            result = subprocess.check_output(
                [str(CPP_BIN), "--print-hash", seed, *parts],
                text=True,
            ).strip()
            self.assertEqual(str(expected), result)


if __name__ == "__main__":
    unittest.main()
