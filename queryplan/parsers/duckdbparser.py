from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import ArrayUnnest, CustomOperator, DBMSType, GroupBy, InlineTable, Iteration, IterationScan, Join, PipelineBreakerScan, QueryOperator, Result, Select, SetOperation, Sort, TableScan, Temp, Window
from queryplan.queryplan import QueryPlan


class DuckDBParser(PlanParser):

    def __init__(self, include_system_representation=True):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.op_counter = 0

    def parse_json_plan(self, query: str, json_plan: dict) -> QueryPlan:
        assert len(json_plan["children"]) == 1
        json_plan = json_plan["children"][0]
        assert json_plan["operator_type"] == "EXPLAIN_ANALYZE" and len(json_plan["children"]) == 1
        json_plan = json_plan["children"][0]

        plan = self.build_initial_plan(json_plan)
        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality,
                         estimated_cardinality=plan.estimated_cardinality, children=[plan], system_representation="// added by benchy")
        return QueryPlan(text=query, plan=root)

    def build_initial_plan(self, json_plan: dict) -> PlanNode:
        operator_type = json_plan["operator_type"]
        operator_id = self.op_counter
        self.op_counter += 1
        operator = self.create_empty_operator(operator_type, operator_id)
        operator.fill(json_plan, DBMSType.DuckDB)

        estimated_cardinality = int(json_plan["extra_info"]["Estimated Cardinality"]) if "Estimated Cardinality" in json_plan["extra_info"] else None
        exact_cardinality = json_plan["operator_cardinality"]

        # append full duckdb representation (excluding children) to operator
        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["children"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        if self.is_leaf_operator(json_plan):
            # Has no children
            return LeafNode(operator, estimated_cardinality=estimated_cardinality, exact_cardinality=exact_cardinality, system_representation=system_representation)
        else:
            children = []
            for child in json_plan["children"]:
                children.append(self.build_initial_plan(child))

            if operator_type == "PROJECTION":
                assert len(children) == 1
                if children[0].estimated_cardinality is None:
                    children[0].estimated_cardinality = estimated_cardinality
                return children[0]
            
            return InnerNode(operator, estimated_cardinality=estimated_cardinality, exact_cardinality=exact_cardinality, children=children, system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int) -> QueryOperator:
        match operator_name:
            case "ORDER_BY":
                return Sort(operator_id)
            case "HASH_GROUP_BY" | "PERFECT_HASH_GROUP_BY" | "SIMPLE_AGGREGATE" | "UNGROUPED_AGGREGATE":
                return GroupBy(operator_id)
            case "PROJECTION":
                return CustomOperator("Projection", operator_id)
            case "COLUMN_DATA_SCAN" | "DUMMY_SCAN":
                return InlineTable(operator_id)
            case "TABLE_SCAN":
                return TableScan(operator_id)
            case "DELIM_SCAN":
                return CustomOperator("DelimScan", operator_id)
            case operator_name if operator_name.endswith("JOIN"):
                return Join(operator_id)
            case "TOP_N":
                return Sort(operator_id)
            case "FILTER":
                return Select(operator_id)
            case "LIMIT" | "STREAMING_LIMIT":
                return CustomOperator("Limit", operator_id)
            case "EMPTY_RESULT":
                return CustomOperator("EmptyResult", operator_id)
            case "UNION":
                return SetOperation(operator_id)
            case "CROSS_PRODUCT":
                return CustomOperator("CrossProduct", operator_id)
            case "WINDOW" | "STREAMING_WINDOW":
                return Window(operator_id)
            case "CTE":
                return Temp(operator_id)
            case "CTE_SCAN":
                return PipelineBreakerScan(operator_id)
            case "RECURSIVE_CTE":
                return Iteration(operator_id)
            case "RECURSIVE_CTE_SCAN":
                return IterationScan(operator_id)
            case "UNNEST":
                return ArrayUnnest(operator_id)
            case "INOUT_FUNCTION":
                return CustomOperator("INOUT_FUNCTION", operator_id)
            case other:
                raise ValueError(f"'{other}' is not a recognized DUCKDB operator")

    def is_leaf_operator(self, json_plan) -> bool:
        if not json_plan["children"]:
            return True
        return False
