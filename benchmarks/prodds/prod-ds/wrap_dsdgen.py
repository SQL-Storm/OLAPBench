#!/usr/bin/env python3
"""
Entry point that delegates to workload.dsdgen.wrap_dsdgen.
"""

import sys

from workload.dsdgen.wrap_dsdgen import main


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
