from abc import ABC
from enum import Enum

from util.logger import log_warn


class DBMSType(int, Enum):
    Umbra = 1
    Postgres = 2
    Hyper = 3
    DuckDB = 4


class OperatorType(Enum):
    # Top level operator, added manually
    Result = 0,
    # Scan operators
    TableScan = 1,
    InlineTable = 2,
    Temp = 3,
    PipelineBreakerScan = 4,
    # Basic operators
    Select = 5,
    Map = 6,
    Sort = 7,
    GroupBy = 8,
    Join = 9,
    # Advanced operators
    GroupJoin = 10,
    EarlyProbe = 11,
    SetOperation = 12,
    Window = 13,
    # Recursive operators
    Iteration = 14,
    IterationScan = 15,
    # Table functions
    ArrayUnnest = 16,
    RegexSplit = 17,
    # Correlation
    Subquery = 18,
    # Miscellaneous operators
    CustomOperator = 42,


class QueryOperator(ABC):

    def __init__(self, operator_type: OperatorType, operator_id: int):
        self.operator_type = operator_type
        self.operator_id = operator_id

    def fill(self, plan, dbms_type: DBMSType):
        return


class Result(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Result, operator_id)


class TableScan(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.TableScan, operator_id)
        self.table_name = None
        self.table_size = None
        self.type = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.table_name = plan["tablename"]
            self.table_size = plan["tableSize"]
        elif dbms_type == DBMSType.Hyper:
            self.table_name = plan["debugName"]["value"]
        elif dbms_type == DBMSType.Postgres:
            match plan["Node Type"]:
                case "Seq Scan":
                    self.table_name = plan["Relation Name"]
                    self.type = "sequential"
                case "Index Scan" | "Index Only Scan":
                    self.table_name = plan["Relation Name"]
                    self.type = "index"
                case _:
                    log_warn(f"Unknown table scan type: {plan['Node Type']}")
        elif dbms_type == DBMSType.DuckDB:
            table_name = plan["extra_info"]["Text"] if "Text" in plan["extra_info"] else None
            self.table_name = table_name


class InlineTable(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.InlineTable, operator_id)
        self.type = None


class Temp(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Temp, operator_id)


class PipelineBreakerScan(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.PipelineBreakerScan, operator_id)
        self.scanned_id = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.scanned_id = plan["scannedOperator"]
        if dbms_type == DBMSType.Hyper:
            if isinstance(plan["input"], int):
                self.scanned_id = plan["input"]
            else:
                self.scanned_id = plan["input"]["operatorId"]


class Select(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Select, operator_id)


class Map(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Map, operator_id)


class Sort(QueryOperator):

    def __init__(self, operator_id: int = None):
        super().__init__(OperatorType.Sort, operator_id)
        self.limit = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra or dbms_type == DBMSType.Hyper:
            self.limit = plan.get("limit")


class GroupBy(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.GroupBy, operator_id)
        self.method = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.method = "hash"
        elif dbms_type == DBMSType.DuckDB:
            name = plan["operator_type"]
            if name in ["HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY"]:
                self.method = "hash"
        elif dbms_type == DBMSType.Hyper:
            self.method = "hash"
        elif dbms_type == DBMSType.Postgres:
            self.method = plan["Node Type"] if plan["Node Type"] in ["Unique", "Group"] else plan["Strategy"]


class Join(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Join, operator_id)
        self.type = None
        self.method = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.type = plan["type"]
            op = plan["physicalOperator"]
            if op == "hashjoin":
                self.method = "hash"
            elif op == "indexnljoin":
                self.method = "index"
            elif op == "bnljoin":
                self.method = "nl"
            else:
                self.method = op.replace("join", "")
        elif dbms_type == DBMSType.DuckDB:
            type = plan["extra_info"]["Join Type"].lower()
            match type:
                case "inner" | "single":
                    self.type = type
                # these appear to be outer joins, switch direction
                case "right":
                    self.type = "leftouter"
                case "left":
                    self.type = "rightouter"
                case "full":
                    self.type = "fullouter"
                case "right_semi":
                    self.type = "leftsemi"
                case "left_semi":
                    self.type = "rightsemi"
                case "right_anti":
                    self.type = "leftanti"
                case "left_anti":
                    self.type = "rightanti"
                # DuckDB does not specify a side
                # we assume that they are referring to the left input
                # since DuckDB swaps join inputs, use "right" prefix instead
                case "mark" | "semi" | "anti":
                    self.type = "right" + type
                case _:
                    raise Exception("Unknown join type for DuckDB: " + type)
            name = plan["operator_type"].replace("_JOIN", "").lower
            if name == "piecewise_merge":
                self.method = "merge"
            elif name == "nested_loop":
                self.method = "nl"
            elif name == "index_join":
                self.method = "index"
        elif dbms_type == DBMSType.Hyper:
            self.method = plan["method"]

            self.type = plan["operator"].replace("join", "")
            # inner join is just called "join"
            if self.type == "":
                self.type = "inner"
        elif dbms_type == DBMSType.Postgres:
            name = plan["Node Type"]
            self.type = plan["Join Type"]
            if name == "Merge Join":
                self.method = "merge"
            elif name == "Hash Join":
                self.method = "hash"
            elif name == "Nested Loop":
                self.method = "nl"


class GroupJoin(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.GroupJoin, operator_id)
        self.type = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.type = plan["behavior"]
            self.method = plan["physicalOperator"].replace("groupjoin", "")
        if dbms_type == DBMSType.Hyper:
            self.type = plan["semantic"]


class EarlyProbe(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.EarlyProbe, operator_id)
        self.source = None

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.source = plan["source"]
        elif dbms_type == DBMSType.Hyper:
            self.source = plan["builder"]


class SetOperation(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.SetOperation, operator_id)
        self.type = None
        self.active = True

    def fill(self, plan, dbms_type: DBMSType):
        if dbms_type == DBMSType.Umbra:
            self.type = plan["operation"]
        if dbms_type == DBMSType.DuckDB:
            name = plan["operator_type"]
            if name == "UNION":
                self.type = "unionall"
        if dbms_type == DBMSType.Hyper:
            self.type = plan["operator"]


class Window(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Window, operator_id)


class Iteration(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Iteration, operator_id)


class IterationScan(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.IterationScan, operator_id)


class ArrayUnnest(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.ArrayUnnest, operator_id)


class RegexSplit(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.RegexSplit, operator_id)


class Subquery(QueryOperator):

    def __init__(self, operator_id: int):
        super().__init__(OperatorType.Subquery, operator_id)


class CustomOperator(QueryOperator):

    def __init__(self, name: str, operator_id: int):
        super().__init__(OperatorType.CustomOperator, operator_id)
        self.name = name
