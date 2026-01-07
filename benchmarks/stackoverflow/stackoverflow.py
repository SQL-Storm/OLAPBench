import argparse
import os
import pathlib

from benchmarks import benchmark


class StackOverflow(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        super().__init__(base_dir, args, included_queries, excluded_queries)
        self.scale = args.get("scale")
        self.zero = args.get("zero", False) or self.scale == 0
        self.dba = args.get("dba", False) or self.scale == 1
        self.math = args.get("math", False) or self.scale == 12
        self.scale = 0 if self.zero else 1 if self.dba else 12 if self.math else self.scale

        if self.scale not in [0, 1, 12, 222]:
            raise ValueError(f"Invalid scale factor: {self.scale}. Valid values are 0, 1, 12, or 222.")

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "stackoverflow"

    @property
    def nice_name(self) -> str:
        return "StackOverflow"

    def fullname(self) -> str:
        return "StackOverflow Benchmark"

    @property
    def description(self) -> str:
        if self.zero:
            return "StackOverflow (Empty)"
        elif self.dba:
            return "StackOverflow (DBA)"
        elif self.math:
            return "StackOverflow (Math)"
        else:
            return f"StackOverflow (SF {self.scale})"

    @property
    def unique_name(self) -> str:
        return "stackoverflow" + ("_zero" if self.zero else "_dba" if self.dba else "_math" if self.math else "")

    @property
    def data_dir(self) -> str:
        return "stackoverflow" + ("_dba" if self.dba else "_math" if self.math else "")

    def dbgen(self):
        self._load_with_command(f'{os.path.join(self.path, "dbgen.sh")} {self.scale}')

    def empty(self) -> bool:
        return self.zero


class StackOverflowDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "stackoverflow"

    @staticmethod
    def get_description() -> str:
        return "StackOverflow Benchmark"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-z", "--zero", dest="zero", default=False, action="store_true", help="empty tables")
        parser.add_argument("--dba", dest="dba", default=False, action="store_true", help="use database administrators stackexchange")
        parser.add_argument("--math", dest="math", default=False, action="store_true", help="use math stackexchange")
        parser.add_argument("-s", "--scale", dest="scale", type=int, default=222, help="scale factor (default: 222)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return StackOverflow(base_dir, args, included_queries, excluded_queries)
