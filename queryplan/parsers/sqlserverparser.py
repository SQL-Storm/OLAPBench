import re
import xml.etree.ElementTree as ET

from queryplan.parsers.planparser import PlanParser
from queryplan.plannode import InnerNode, LeafNode, PlanNode
from queryplan.queryoperator import CustomOperator, DBMSType, GroupBy, Join, Map, Result, Select, SetOperation, Sort, TableScan
from queryplan.queryplan import QueryPlan


class SQLServerParser(PlanParser):

    def __init__(self, include_system_representation=True):
        super().__init__()
        self.include_system_representation = include_system_representation
        self.op_counter = 0

    def parse_json_plan(self, query: str, plan_payload) -> QueryPlan:
        if isinstance(plan_payload, str):
            root = ET.fromstring(plan_payload)
            relop = root.find(".//RelOp")
            if relop is None:
                raise ValueError("No RelOp node found in SQL Server plan")
            plan = self.build_relop(relop)
        else:
            plan = self._build_from_rows(plan_payload)

        result = InnerNode(Result(-1), exact_cardinality=plan.exact_cardinality,
                           estimated_cardinality=plan.estimated_cardinality, children=[plan],
                           system_representation="// added by benchy")
        return QueryPlan(text=query, plan=result)

    def build_relop(self, relop_elem: ET.Element) -> PlanNode:
        physical = relop_elem.attrib.get("PhysicalOp", "Unknown")
        logical = relop_elem.attrib.get("LogicalOp", physical)
        operator_name = self._decide_operator_name(physical, logical)
        operator_id = self._extract_operator_id(relop_elem)

        plan_info = {
            "PhysicalOp": physical,
            "LogicalOp": logical,
        }

        # Estimated and actual cardinalities if available
        estimated_cardinality = self._safe_int(relop_elem.attrib.get("EstimateRows"))
        exact_cardinality = self._safe_int(relop_elem.attrib.get("ActualRows"))
        if exact_cardinality is None:
            exact_cardinality = estimated_cardinality

        # Attach join/grouping metadata
        self._annotate_join_or_agg(physical, logical, plan_info, relop_elem)

        # Capture limit information for TOP
        if operator_name.lower() == "top":
            top_elem = relop_elem.find("Top")
            if top_elem is not None:
                plan_info["limit"] = self._safe_int(top_elem.attrib.get("RowCount"))

        # Capture scan object information
        self._annotate_object(relop_elem, plan_info)

        operator = self.create_empty_operator(operator_name, operator_id)
        operator.fill(plan_info, DBMSType.SQLServer)

        system_representation = None
        if self.include_system_representation:
            system_representation = ET.tostring(relop_elem, encoding="unicode")

        children_relops = self._extract_children(relop_elem)
        if not children_relops:
            return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)

        children = [self.build_relop(child) for child in children_relops]
        return InnerNode(operator, estimated_cardinality, exact_cardinality, children, system_representation=system_representation)

    def create_empty_operator(self, operator_name: str, operator_id: int):
        name = operator_name.lower()
        if name.startswith("table scan") or name.startswith("index scan") or name.startswith("index seek") or name.startswith("clustered index"):
            return TableScan(operator_id)
        if name.startswith("sort"):
            return Sort(operator_id)
        if name.startswith("filter"):
            return Select(operator_id)
        if name.startswith("compute"):
            return Map(operator_id)
        if name.startswith("hash match") or name.startswith("merge join") or name.startswith("nested loops") or "join" in name:
            if "aggregate" in name:
                return GroupBy(operator_id)
            return Join(operator_id)
        if "aggregate" in name:
            return GroupBy(operator_id)
        if name.startswith("top"):
            return CustomOperator("Top", operator_id)
        if name.startswith("concat") or name.startswith("concatenation"):
            op = SetOperation(operator_id)
            op.type = "unionall"
            return op
        return CustomOperator(operator_name, operator_id)

    def is_leaf_operator(self, plan) -> bool:
        if isinstance(plan, ET.Element):
            return len(self._extract_children(plan)) == 0
        if isinstance(plan, dict) and "children" in plan:
            return len(plan["children"]) == 0
        return False

    def _build_from_rows(self, rows) -> PlanNode:
        columns = ["Rows", "Executes", "StmtText", "StmtId", "NodeId", "Parent", "PhysicalOp", "LogicalOp",
                   "Argument", "DefinedValues", "EstimateRows", "EstimateIO", "EstimateCPU", "AvgRowSize",
                   "TotalSubtreeCost", "OutputList", "Warnings", "Type", "Parallel", "EstimateExecutions"]
        nodes = []
        for row in rows:
            if len(row) < len(columns):
                continue
            entry = {col: row[i] for i, col in enumerate(columns)}
            if entry.get("Type") and str(entry["Type"]).upper() != "PLAN_ROW":
                continue
            nodes.append(entry)

        node_map = {int(n["NodeId"]): n for n in nodes if n.get("NodeId") is not None}
        children_map = {nid: [] for nid in node_map.keys()}
        root_id = None
        for nid, n in node_map.items():
            parent = n.get("Parent")
            if parent is None or int(parent) == 0:
                root_id = nid if root_id is None else root_id
            else:
                parent = int(parent)
                children_map.setdefault(parent, []).append(nid)
        if root_id is None and node_map:
            root_id = min(node_map.keys())

        def build_node(node_id: int) -> PlanNode:
            info = node_map[node_id]
            plan_info = {
                "PhysicalOp": info.get("PhysicalOp"),
                "LogicalOp": info.get("LogicalOp"),
                "Argument": info.get("Argument"),
                "DefinedValues": info.get("DefinedValues"),
                "EstimateRows": info.get("EstimateRows"),
                "Rows": info.get("Rows"),
            }
            top_limit = self._extract_top_limit(plan_info.get("Argument", ""))
            if top_limit is not None:
                plan_info["limit"] = top_limit
            self._annotate_join_or_agg(plan_info.get("PhysicalOp", ""), plan_info.get("LogicalOp", ""), plan_info, None)
            self._annotate_object_from_argument(plan_info)
            operator_name = self._decide_operator_name(plan_info.get("PhysicalOp", ""), plan_info.get("LogicalOp", ""))
            operator = self.create_empty_operator(operator_name, node_id)
            operator.fill(plan_info, DBMSType.SQLServer)

            estimated_cardinality = self._safe_int(info.get("EstimateRows"))
            exact_cardinality = self._safe_int(info.get("Rows"))
            if exact_cardinality is None:
                exact_cardinality = estimated_cardinality

            children_ids = children_map.get(node_id, [])
            children_nodes = [build_node(cid) for cid in children_ids]

            system_representation = info if not self.include_system_representation else info.copy()

            # Skip non-relational parallel wrappers
            phys_lower = str(plan_info.get("PhysicalOp", "")).lower()
            if phys_lower.startswith("parallelism") and len(children_nodes) == 1:
                return children_nodes[0]

            if not children_nodes:
                return LeafNode(operator, estimated_cardinality, exact_cardinality, system_representation=system_representation)
            return InnerNode(operator, estimated_cardinality, exact_cardinality, children_nodes, system_representation=system_representation)

        if root_id is None:
            raise ValueError("No root node identified in SQL Server plan rows")
        return build_node(root_id)

    def _extract_children(self, relop_elem: ET.Element):
        children = []
        children_elem = relop_elem.find("Children")
        if children_elem is not None:
            children.extend(list(children_elem.findall("RelOp")))
        else:
            for child in relop_elem.findall("RelOp"):
                children.append(child)
        return children

    def _extract_operator_id(self, relop_elem: ET.Element):
        node_id = relop_elem.attrib.get("NodeId")
        if node_id is not None:
            try:
                return int(node_id)
            except Exception:
                return node_id
        current = self.op_counter
        self.op_counter += 1
        return current

    def _decide_operator_name(self, physical: str, logical: str) -> str:
        phys = physical.lower()
        logical_lower = logical.lower()
        if phys.startswith("hash match"):
            if "aggregate" in logical_lower:
                return "Hash Aggregate"
            return "Hash Match"
        if phys.startswith("stream aggregate"):
            return "Stream Aggregate"
        if phys.startswith("merge join"):
            return "Merge Join"
        if phys.startswith("nested loops"):
            return "Nested Loops"
        if phys.startswith("top"):
            return "Top"
        if phys.startswith("concat"):
            return "Concatenation"
        if phys.startswith("filter"):
            return "Filter"
        if phys.startswith("sort"):
            return "Sort"
        if phys.startswith("compute"):
            return "Compute Scalar"
        if phys.startswith("table scan") or phys.startswith("index scan") or phys.startswith("index seek") or phys.startswith("clustered index"):
            return physical
        return physical or logical

    def _annotate_join_or_agg(self, physical: str, logical: str, plan_info: dict, relop_elem: ET.Element):
        phys_lower = physical.lower()
        logical_lower = logical.lower()
        if "join" in logical_lower or "join" in phys_lower:
            join_type = None
            if "left anti" in logical_lower:
                join_type = "leftanti"
            elif "right anti" in logical_lower:
                join_type = "rightanti"
            elif "left semi" in logical_lower:
                join_type = "leftsemi"
            elif "right semi" in logical_lower:
                join_type = "rightsemi"
            elif "inner" in logical_lower:
                join_type = "inner"
            elif "left outer" in logical_lower:
                join_type = "leftouter"
            elif "right outer" in logical_lower:
                join_type = "rightouter"
            elif "full outer" in logical_lower:
                join_type = "fullouter"
            elif "cross" in logical_lower:
                join_type = "cross"
            elif "semi" in logical_lower:
                join_type = "semi"
            elif "anti" in logical_lower:
                join_type = "anti"
            plan_info["join_type"] = join_type
            if "hash" in phys_lower:
                plan_info["join_method"] = "hash"
            elif "merge" in phys_lower:
                plan_info["join_method"] = "merge"
            elif "loop" in phys_lower:
                plan_info["join_method"] = "nl"
            elif "adaptive" in phys_lower:
                plan_info["join_method"] = "adaptive"
        if "aggregate" in logical_lower or "aggregate" in phys_lower:
            if "hash" in phys_lower:
                plan_info["agg_method"] = "hash"
            elif "stream" in phys_lower:
                plan_info["agg_method"] = "stream"

    def _annotate_object(self, relop_elem: ET.Element, plan_info: dict):
        for tag in ["IndexScan", "IndexSeek", "TableScan", "ClusteredIndexScan", "ClusteredIndexSeek"]:
            child = relop_elem.find(tag)
            if child is not None:
                obj = child.find("Object")
                if obj is not None:
                    plan_info["object"] = obj.attrib
                break

    def _annotate_object_from_argument(self, plan_info: dict):
        argument = plan_info.get("Argument")
        if not isinstance(argument, str):
            return
        match = re.search(r"OBJECT:\(\[([^]]+)\]\.\[([^]]+)\]\.\[([^]]+)\]", argument)
        if match:
            plan_info["object"] = {
                "Database": match.group(1),
                "Schema": match.group(2),
                "Table": match.group(3)
            }
            return
        match = re.search(r"OBJECT:([^,]+)", argument)
        if match:
            plan_info["object"] = match.group(1).strip()

    def _extract_top_limit(self, argument: str):
        if not isinstance(argument, str):
            return None
        match = re.search(r"TOP\s+(\d+)", argument, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                return None
        return None

    def _safe_int(self, value):
        try:
            if value is None:
                return None
            return int(float(value))
        except Exception:
            return None
