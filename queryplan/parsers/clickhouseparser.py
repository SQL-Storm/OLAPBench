from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import ArrayUnnest, CustomOperator, DBMSType, GroupBy, Join, Map, OperatorType, Result, Select, SetOperation, Sort, TableScan
from queryplan.queryplan import QueryPlan


class ClickHouseParser(PlanParser):

    def __init__(self, include_system_representation=True):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.op_counter = 0

    def parse_json_plan(self, query: str, json_plan: dict) -> QueryPlan:
        plan_root = json_plan.get("Plan") or json_plan.get("plan") or json_plan
        if isinstance(plan_root, list):
            plan_root = plan_root[0] if plan_root else {}
        plan = self.build_initial_plan(plan_root)
        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality,
                         estimated_cardinality=plan.estimated_cardinality, children=[plan],
                         system_representation="// added by benchy")
        return QueryPlan(text=query, plan=root)

    def build_initial_plan(self, json_plan: dict) -> PlanNode:
        operator_name = self._extract_operator_name(json_plan)
        operator_id = self._extract_operator_id(json_plan)

        # Skip over non-relational expression nodes to focus on core algebra operators
        normalized_name = operator_name.lower() if isinstance(operator_name, str) else str(operator_name).lower()
        children_nodes = self._extract_children(json_plan)
        if normalized_name.startswith("expression"):
            if children_nodes:
                # Recurse directly into the child subplan, bypassing the expression wrapper
                return self.build_initial_plan(children_nodes[0])
            # No children to descend into; fall back to a no-op map representation
            operator_name = "Map"

        operator = self.create_empty_operator(operator_name, operator_id)
        operator.fill(json_plan, DBMSType.ClickHouse)

        estimated_cardinality, exact_cardinality = self._extract_cardinalities(json_plan)

        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["Plans", "plans", "Children", "children", "sources"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        # Merge a Limit into an immediately following Sort to keep a single node
        if operator.operator_type == OperatorType.CustomOperator and getattr(operator, "name", "") == "Limit" and children_nodes:
            merged_child = self.build_initial_plan(children_nodes[0])
            if merged_child.operator.operator_type == OperatorType.Sort:
                if getattr(operator, "limit", None) is not None:
                    merged_child.operator.limit = operator.limit
                return merged_child

        if self.is_leaf_operator(json_plan):
            return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)

        children = [self.build_initial_plan(child) for child in children_nodes]
        return InnerNode(operator, estimated_cardinality, exact_cardinality, children, system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int):
        normalized = operator_name.lower() if isinstance(operator_name, str) else str(operator_name).lower()
        if normalized.startswith("read"):
            return TableScan(operator_id)
        if normalized.startswith("aggregating") or normalized.startswith("group"):
            return GroupBy(operator_id)
        if normalized.startswith("sorting") or " sort" in normalized or normalized.startswith("order by"):
            return Sort(operator_id)
        if "join" in normalized:
            return Join(operator_id)
        if normalized.startswith("filter"):
            return Select(operator_id)
        if normalized in ["arrayjoin", "array join"]:
            return ArrayUnnest(operator_id)
        if normalized in ["union", "intersect", "except"]:
            operator = SetOperation(operator_id)
            operator.type = normalized
            return operator
        if normalized.startswith("limit"):
            return CustomOperator("Limit", operator_id)
        if normalized.startswith("projection") or normalized.startswith("expression"):
            return Map(operator_id)
        return CustomOperator(operator_name, operator_id)

    def is_leaf_operator(self, plan):
        return len(self._extract_children(plan)) == 0

    def _extract_children(self, plan):
        if not isinstance(plan, dict):
            return []
        for key in ["Plans", "plans", "Children", "children", "sources"]:
            if key in plan and plan[key]:
                return plan[key]
        return []

    def _extract_operator_name(self, plan):
        if not isinstance(plan, dict):
            return str(plan)
        for key in ["Node Type", "NodeType", "PlanStep", "Plan Node Type", "Name", "type", "Step"]:
            if key in plan and plan[key]:
                return plan[key]
        return plan.get("plan_node_name", "Unknown")

    def _extract_operator_id(self, plan):
        if not isinstance(plan, dict):
            operator_id = self.op_counter
            self.op_counter += 1
            return operator_id
        for key in ["PlanNodeId", "Plan Node Id", "PlanNodeID", "NodeId", "Node ID"]:
            if key in plan and plan[key] is not None:
                try:
                    return int(plan[key])
                except Exception:
                    return plan[key]
        operator_id = self.op_counter
        self.op_counter += 1
        return operator_id

    def _extract_cardinalities(self, plan):
        if not isinstance(plan, dict):
            return None, None
        stats = plan.get("Statistics") or plan.get("statistics") or {}
        estimated = self._safe_int(stats.get("row_count") or stats.get("estimated_rows") or stats.get("rowCount") or stats.get("rows_before_limit_at_least"))
        exact = self._safe_int(stats.get("rows") or stats.get("rows_before_limit") or stats.get("output_rows"))

        if exact is None:
            exact = estimated
        return estimated, exact

    def _safe_int(self, value):
        try:
            if isinstance(value, str):
                return int(float(value))
            return int(value)
        except (TypeError, ValueError):
            return None
