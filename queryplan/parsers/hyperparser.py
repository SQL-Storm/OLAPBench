from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import CustomOperator, DBMSType, EarlyProbe, GroupBy, GroupJoin, Join, Map, OperatorType, PipelineBreakerScan, Result, Select, SetOperation, Sort, TableScan, Temp, Window
from queryplan.queryplan import QueryPlan

join_types = ["join", "fullouterjoin",
              "leftsemijoin", "leftouterjoin", "leftantijoin", "leftmarkjoin",
              "rightsemijoin", "rightouterjoin", "rightantijoin", "rightmarkjoin"]

set_operations = ["union", "intersect", "except",
                  "unionall", "intersectall", "exceptall"]


class HyperParser(PlanParser):

    def __init__(self, include_system_representation=True, duplicate_shared_pipelines=False):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.duplicate_shared_pipelines = duplicate_shared_pipelines

    def parse_json_plan(self, query: str, json_plan) -> QueryPlan:
        plan = self.build_initial_plan(json_plan)
        if self.duplicate_shared_pipelines:
            self.append_shared_pipelines()
        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality, estimated_cardinality=plan.estimated_cardinality,
                         children=[plan], system_representation="// added by benchy")
        return QueryPlan(text=query, plan=root)

    # Builds the plan without shared pipelines
    def build_initial_plan(self, json_plan) -> PlanNode:
        operator_name = json_plan["operator"]
        operator_id = json_plan["operatorId"]
        operator = self.create_empty_operator(operator_name, operator_id)
        operator.fill(json_plan, DBMSType.Hyper)

        estimated_cardinality = 0
        if "cardinality" in json_plan:
            estimated_cardinality = json_plan["cardinality"]

        exact_cardinality = json_plan["analyze"]["tuple-count"]

        # append full hyper representation (excluding children) to operator
        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["input", "left", "right"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        if self.is_leaf_operator(json_plan):
            # Has no children
            return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)
        else:
            children = []
            if operator.operator_type == OperatorType.PipelineBreakerScan:
                # Build and store shared pipelines
                if not isinstance(json_plan["input"], int):
                    if self.duplicate_shared_pipelines:
                        scanned_id = json_plan["input"]["operatorId"]
                        child = self.build_initial_plan(json_plan["input"])
                        self.shared_pipelines[scanned_id] = child
                    else:
                        children.append(self.build_initial_plan(json_plan["input"]))
            elif "input" in json_plan:
                if isinstance(json_plan["input"], list):
                    # Set operator with multiple children
                    for item in json_plan["input"]:
                        children.append(self.build_initial_plan(item))
                else:
                    # Has one child in the plan
                    children.append(self.build_initial_plan(json_plan["input"]))
            elif "left" in json_plan and "right" in json_plan:
                # Has two children in the plan
                children.append(self.build_initial_plan(json_plan["left"]))
                children.append(self.build_initial_plan(json_plan["right"]))

            if self.duplicate_shared_pipelines:
                # Store temp scan inner nodes
                for child in children:
                    if child.operator.operator_type == OperatorType.PipelineBreakerScan:
                        self.temp_scan_nodes.append(child)

            # If the operator is an index nested loop join, hyper will not count the cardinality of the table as tuples
            # TODO this is only an estimate, is there a way to get just the table size?
            if operator.operator_type == OperatorType.Join and operator.method == "indexnl":
                if children[1].operator.operator_type == OperatorType.TableScan:
                    children[1].exact_cardinality = children[1].estimated_cardinality

            return InnerNode(operator, estimated_cardinality, exact_cardinality, children, system_representation=system_representation)

    def append_shared_pipelines(self):
        for scan_node in self.temp_scan_nodes:
            scanned_child = self.shared_pipelines[scan_node.operator.scanned_id]
            scan_node.children.append(scanned_child)

    def create_empty_operator(self, operator_name: str, operator_id: int):
        match operator_name:
            case "tablescan":
                return TableScan(operator_id)
            case "sort":
                return Sort(operator_id)
            case _ if operator_name in join_types:
                return Join(operator_id)
            case "groupjoin":
                return GroupJoin(operator_id)
            case "groupby":
                return GroupBy(operator_id)
            case "map":
                return Map(operator_id)
            case "earlyprobe":
                return EarlyProbe(operator_id)
            case _ if operator_name in set_operations:
                return SetOperation(operator_id)
            case "window":
                return Window(operator_id)
            case "explicitscan":
                return PipelineBreakerScan(operator_id)
            case "select" | "filter":
                # Hyper renamed the "select" operator to "filter" starting in 0.0.25080
                return Select(operator_id)
            case "assertsingle":
                return CustomOperator("AssertSingle", operator_id)
            case "temp":
                return Temp(operator_id)
            case other:
                raise ValueError(f"{other} is not a recognized UMBRA operator")

    def is_leaf_operator(self, plan):
        children_tags = ["input", "left", "right", "source"]
        for tag in children_tags:
            if tag in plan:
                return False
        return True
