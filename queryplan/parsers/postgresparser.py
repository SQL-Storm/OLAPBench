from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import CustomOperator, DBMSType, GroupBy, Iteration, IterationScan, Join, Map, OperatorType, PipelineBreakerScan, Result, SetOperation, Sort, Subquery, TableScan, Temp, Window
from queryplan.queryplan import QueryPlan


class PostgresParser(PlanParser):

    def __init__(self, include_system_representation=True):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.op_counter = 0
        self.ctes = {}

    def parse_json_plan(self, query: str, json_plan: dict) -> QueryPlan:
        plan = self.build_initial_plan(json_plan["Plan"])
        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality, estimated_cardinality=plan.estimated_cardinality,
                         children=[plan], system_representation="// added by benchy")
        return QueryPlan(text=query, plan=root)

    def build_initial_plan(self, json_plan: dict) -> PlanNode:
        operator_name = json_plan["Node Type"]
        operator_id = self.op_counter
        self.op_counter += 1
        operator = self.create_empty_operator(operator_name, operator_id)
        operator.fill(json_plan, DBMSType.Postgres)

        estimated_cardinality = json_plan["Plan Rows"]
        exact_cardinality = json_plan["Actual Rows"]

        # append full umbra representation (excluding children) to operator
        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["Plans"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        def is_cte(e: dict) -> bool:
            return e.get("Parent Relationship", "") == "InitPlan" and e.get("Subplan Name", "").startswith("CTE ")

        def is_leaf(e: dict) -> bool:
            return self.is_leaf_operator(e)

        # Prepare ctes
        if not is_leaf(json_plan):
            for entry in json_plan["Plans"]:
                if is_cte(entry):
                    temp = self.build_initial_plan(entry)
                    temp = InnerNode(Temp(self.op_counter), temp.estimated_cardinality, temp.exact_cardinality, [temp], system_representation="// added by benchy")
                    self.ctes[entry["Subplan Name"].replace("CTE ", "")] = (self.op_counter, False, temp)
                    self.op_counter += 1

        if operator.operator_type == OperatorType.PipelineBreakerScan:
            cte_name = json_plan["CTE Name"]
            cte_id, already_included, cte_node = self.ctes[cte_name]
            operator.scanned_id = cte_id
            if not already_included:
                self.ctes[cte_name] = (cte_id, True, cte_node)
                return InnerNode(operator, estimated_cardinality, exact_cardinality, [cte_node], system_representation=system_representation)

        if is_leaf(json_plan):
            # Has no children
            return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)
        else:
            children = []
            for entry in json_plan["Plans"]:
                if is_cte(entry):
                    continue

                children.append(self.build_initial_plan(entry))
            return InnerNode(operator, estimated_cardinality, exact_cardinality, children, system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int):
        match operator_name:
            case "Aggregate" | "Unique" | "Group":
                return GroupBy(operator_id)
            case "Gather" | "Gather Merge":
                return CustomOperator("Gather", operator_id)
            case "Append" | "Merge Append":
                return CustomOperator("Append", operator_id)
            case operator_name if operator_name.endswith("Sort"):
                return Sort(operator_id)
            case "Seq Scan" | "Index Scan" | "Index Only Scan" | "Bitmap Heap Scan" | "Bitmap Index Scan":
                return TableScan(operator_id)
            case "Limit":
                return CustomOperator("Limit", operator_id)
            case operator_name if operator_name.endswith("Join"):
                return Join(operator_id)
            case "Hash":
                return CustomOperator("Hash", operator_id)
            case "Nested Loop":
                return Join(operator_id)
            case "Materialize":
                return CustomOperator("Materialize", operator_id)
            case "WindowAgg":
                return Window(operator_id)
            case "Result":
                return Map(operator_id)
            case "Recursive Union":
                return Iteration(operator_id)
            case "WorkTable Scan":
                return IterationScan(operator_id)
            case "Subquery Scan":
                return Subquery(operator_id)
            case "CTE Scan":
                return PipelineBreakerScan(operator_id)
            case "Memoize":
                return CustomOperator("Memoize", operator_id)
            case "SetOp":
                return SetOperation(operator_id)
            case "ProjectSet":
                return CustomOperator("ProjectSet", operator_id)
            case "Function Scan":
                return CustomOperator("Function Scan", operator_id)
            case "Values Scan":
                return CustomOperator("Values Scan", operator_id)
            case "BitmapOr":
                return CustomOperator("BitmapOr", operator_id)
            case other:
                raise ValueError(f"'{other}' is not a recognized POSTGRES operator")

    def is_leaf_operator(self, plan):
        if "Plans" in plan:
            return False
        return True
