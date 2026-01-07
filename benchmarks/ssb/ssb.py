import argparse
import os
import pathlib

from benchmarks import benchmark


class SSB(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args["scale"]

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "ssb"

    @property
    def nice_name(self) -> str:
        return "SSB"

    @property
    def fullname(self) -> str:
        return "Star Schema Benchmark"

    @property
    def description(self) -> str:
        return f"SSB (SF {self.scale})"

    @property
    def unique_name(self) -> str:
        return f"ssbSf{self.scale}"

    @property
    def data_dir(self) -> str:
        return os.path.join("ssb", f"sf{self.scale}")

    def dbgen(self):
        self._load_with_command(f'{os.path.join(self.path, "dbgen.sh")} {self.scale}')


class SSBDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "ssb"

    @staticmethod
    def get_description() -> str:
        return "Star Schema Benchmark"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-s", "--scale", dest="scale", type=int, default=1, help="scale factor (default: 1)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return SSB(base_dir, args, included_queries, excluded_queries)
