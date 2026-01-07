import argparse
import os
import pathlib

from benchmarks import benchmark


class ClickBench(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "clickbench"

    @property
    def nice_name(self) -> str:
        return "ClickBench"

    @property
    def fullname(self) -> str:
        return "ClickBench"

    @property
    def description(self) -> str:
        return "ClickBench"

    @property
    def unique_name(self) -> str:
        return "clickbench"

    @property
    def data_dir(self) -> str:
        return "clickbench"

    def dbgen(self):
        self._load_with_command(os.path.join(self.path, "dbgen.sh"))


class ClickBenchDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "clickbench"

    @staticmethod
    def get_description() -> str:
        return "ClickBench"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)

    @staticmethod
    def instantiate(base_dir: str, args: dict) -> benchmark.Benchmark:
        return ClickBench(base_dir, args)
