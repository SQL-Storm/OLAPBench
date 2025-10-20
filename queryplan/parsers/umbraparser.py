from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import ArrayUnnest, CustomOperator, DBMSType, EarlyProbe, GroupBy, GroupJoin, InlineTable, Iteration, IterationScan, Join, Map, OperatorType, PipelineBreakerScan, RegexSplit, Result, Select, SetOperation, Sort, TableScan, Temp, Window
from queryplan.queryplan import QueryPlan


class UmbraParser(PlanParser):

    def __init__(self, include_system_representation=True, duplicate_shared_pipelines=False):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.duplicate_shared_pipelines = duplicate_shared_pipelines

    def parse_json_plan(self, query: str, json_plan: dict) -> QueryPlan:
        plan = self.build_initial_plan(json_plan['plan'])
        if self.duplicate_shared_pipelines:
            self.append_shared_pipelines()

        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            if "plan" in system_representation:
                system_representation.pop("plan")

        root = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality, estimated_cardinality=plan.estimated_cardinality,
                         children=[plan], system_representation=system_representation)
        return QueryPlan(text=query, plan=root)

    # Builds the plan without shared pipelines
    def build_initial_plan(self, json_plan: dict) -> PlanNode:
        operator_name = json_plan["operator"]
        operator_id = json_plan["operatorId"]
        operator = self.create_empty_operator(operator_name, operator_id)
        operator.fill(json_plan, DBMSType.Umbra)

        # append full umbra representation (excluding children) to operator
        system_representation = None
        if self.include_system_representation:
            system_representation = json_plan.copy()
            for child_key in ["magic", "pipelineBreaker", "input", "left", "right", "arguments", "inputs"]:
                if child_key in system_representation:
                    system_representation.pop(child_key)

        estimated_cardinality = 0
        if "cardinality" in json_plan:
            estimated_cardinality = json_plan["cardinality"]

        exact_cardinality = estimated_cardinality
        if "analyzePlanCardinality" in json_plan:
            exact_cardinality = json_plan["analyzePlanCardinality"]

        if self.is_leaf_operator(json_plan):
            # Has no children
            return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)
        else:
            children = []
            # Handle magic operator
            if "magic" in json_plan:
                child = self.build_initial_plan(json_plan["magic"])
                magic_id = child.operator.operator_id
                self.shared_pipelines[magic_id] = child
                children.append(child)

            if operator.operator_type == OperatorType.PipelineBreakerScan:
                # Build and store shared pipelines
                if "pipelineBreaker" in json_plan:
                    if self.duplicate_shared_pipelines:
                        scanned_id = json_plan["scannedOperator"]
                        child = self.build_initial_plan(json_plan["pipelineBreaker"])
                        self.shared_pipelines[scanned_id] = child
                    else:
                        children.append(self.build_initial_plan(json_plan["pipelineBreaker"]))
            elif "input" in json_plan:
                # Has one child in the plan
                children.append(self.build_initial_plan(json_plan["input"]))
            elif "left" in json_plan and "right" in json_plan:
                # Has two children in the plan
                children.append(self.build_initial_plan(json_plan["left"]))
                children.append(self.build_initial_plan(json_plan["right"]))

                # Set estimated cardinality of the table scan to 0, we do not execute this operation
                if json_plan["physicalOperator"] == "indexnljoin":
                    children[1].estimated_cardinality = 0
            elif "arguments" in json_plan:
                # Set operator with multiple children
                for item in json_plan["arguments"]:
                    children.append(self.build_initial_plan(item["input"]))
            elif "inputs" in json_plan:
                # Multiway join with multiple inputs
                for item in json_plan["inputs"]:
                    children.append(self.build_initial_plan(item["op"]))

            if self.duplicate_shared_pipelines:
                # Store temp scan inner nodes
                for child in children:
                    if child.operator.operator_type == OperatorType.PipelineBreakerScan:
                        self.temp_scan_nodes.append(child)

            return InnerNode(operator, estimated_cardinality, exact_cardinality, children,
                             system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int):
        match operator_name:
            case "tablescan":
                return TableScan(operator_id)
            case "inlinetable":
                return InlineTable(operator_id)
            case "sort":
                return Sort(operator_id)
            case "join":
                return Join(operator_id)
            case "groupjoin":
                return GroupJoin(operator_id)
            case "groupby":
                return GroupBy(operator_id)
            case "map":
                return Map(operator_id)
            case "select":
                return Select(operator_id)
            case "pipelinebreakerscan" | "tempscan":
                return PipelineBreakerScan(operator_id)
            case "temp":
                return Temp(operator_id)
            case "earlyprobe":
                return EarlyProbe(operator_id)
            case "setoperation":
                return SetOperation(operator_id)
            case "assertsingle":
                return CustomOperator("AssertSingle", operator_id)
            case "window":
                return Window(operator_id)
            case "multiwayjoin":
                return CustomOperator("MultiwayJoin", operator_id)
            case "earlyexecution":
                return CustomOperator("EarlyExecution", operator_id)
            case "iteration":
                return Iteration(operator_id)
            case "iterationincrementscan":
                return IterationScan(operator_id)
            case "arrayunnest":
                return ArrayUnnest(operator_id)
            case "regexsplit":
                return RegexSplit(operator_id)
            case "generateseries":
                return CustomOperator("GenerateSeries", operator_id)
            case other:
                raise ValueError(f"{other} is not a recognized UMBRA operator")

    def append_shared_pipelines(self):
        for scan_node in self.temp_scan_nodes:
            scanned_child = self.shared_pipelines[scan_node.operator.scanned_id]
            scan_node.children.append(scanned_child)

    def is_leaf_operator(self, plan):
        children_tags = ["input", "left", "right", "arguments", "magic", "scannedOperator", "inputs"]
        for tag in children_tags:
            if tag in plan:
                return False
        return True
