import argparse
import os
import pathlib

from benchmarks import benchmark


STR_LEVEL_MIN = 1
STR_LEVEL_MAX = 15


def _str_level_type(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid int value: {raw!r}")
    if not STR_LEVEL_MIN <= value <= STR_LEVEL_MAX:
        raise argparse.ArgumentTypeError(
            f"--str-level must be in [{STR_LEVEL_MIN}, {STR_LEVEL_MAX}], got {value}"
        )
    return value


class ProdDS(benchmark.Benchmark):
    def __init__(self, base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None):
        self.scale = args["scale"]
        self.str_level = args["str_level"]
        self.null_profile = args["null_profile"]
        self.mcv_profile = args["mcv_profile"]
        self.dialect = args["dialect"]
        # Tag results with the dialect via the existing query_dir mechanism (also used in result_name).
        if self.dialect != "ansi" and args.get("query_dir") is None:
            args["query_dir"] = self.dialect
        super().__init__(base_dir, args, included_queries, excluded_queries)

    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def name(self) -> str:
        return "prodds"

    @property
    def nice_name(self) -> str:
        return "Prod-DS"

    @property
    def fullname(self) -> str:
        return "Prod-DS Benchmark"

    @property
    def description(self) -> str:
        return f"Prod-DS (SF {self.scale}, STR={self.str_level}, null={self.null_profile}, mcv={self.mcv_profile})"

    @property
    def unique_name(self) -> str:
        return f"prodds_sf{self.scale}_str{self.str_level}_null{self.null_profile}_mcv{self.mcv_profile}"

    @property
    def data_dir(self) -> str:
        return os.path.join("prodds", f"sf{self.scale}_str{self.str_level}_null{self.null_profile}_mcv{self.mcv_profile}")

    @property
    def queries_path(self) -> str:
        suffix = "" if self.dialect == "ansi" else f"_{self.dialect}"
        return os.path.join(self._base_dir, "data", self.data_dir, f"queries{suffix}")

    def schema_path(self) -> str:
        return os.path.join(self._base_dir, "data", self.data_dir, "schema.json")

    def dbgen(self):
        cmd = (
            f'{os.path.join(self.path, "dbgen.sh")} '
            f'{self.scale} {self.str_level} {self.null_profile} {self.mcv_profile}'
        )
        self._load_with_command(cmd)

    def empty(self) -> bool:
        return self.scale == 0


class ProdDSDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "prodds"

    @staticmethod
    def get_description() -> str:
        return "Prod-DS Benchmark (TPC-DS extension with stringification, NULL skew, MCV skew)"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-s", "--scale", dest="scale", type=int, default=1,
                            help="scale factor (default: 1)")
        parser.add_argument("--str-level", dest="str_level", type=_str_level_type, default=10,
                            help=f"stringification level {STR_LEVEL_MIN}-{STR_LEVEL_MAX} (default: 10, production)")
        parser.add_argument("--null-profile", dest="null_profile", type=str, default="medium",
                            choices=["none", "low", "medium", "high"],
                            help="NULL sparsity injection profile, or 'none' to disable (default: medium)")
        parser.add_argument("--mcv-profile", dest="mcv_profile", type=str, default="medium",
                            choices=["none", "low", "medium", "high"],
                            help="MCV skew injection profile, or 'none' to disable (default: medium)")
        parser.add_argument("--dialect", dest="dialect", type=str, default="postgres",
                            choices=["ansi", "duckdb", "postgres"],
                            help="SQL dialect for generated queries (default: postgres)")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return ProdDS(base_dir, args, included_queries, excluded_queries)
