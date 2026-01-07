import argparse
import os
import pathlib

from benchmarks import benchmark


class JOB(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.zero = args["zero"] if "zero" in args.keys() else False

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "job"

    @property
    def nice_name(self) -> str:
        return "JOB"

    @property
    def fullname(self) -> str:
        return "Join Ordering Benchmark"

    @property
    def description(self) -> str:
        return "JOB (Empty)" if self.zero else "JOB"

    @property
    def unique_name(self) -> str:
        return "job" + ("_zero" if self.zero else "")

    @property
    def data_dir(self) -> str:
        return "job"

    @property
    def default_runtime_plot(self) -> str:
        return "runtime" if self.query_dir is None else None

    def dbgen(self):
        self._load_with_command(os.path.join(self.path, "dbgen.sh"))

    def empty(self) -> bool:
        return self.zero


class JOBDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "job"

    @staticmethod
    def get_description() -> str:
        return "Join Order Benchmark"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-z", "--zero", dest="zero", default=False, action="store_true", help="empty tables")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return JOB(base_dir, args, included_queries, excluded_queries)
