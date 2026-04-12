// QueryBrew (c) 2025
import { useState, useEffect, useMemo, createContext, useContext } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import IconButton from '@mui/material/IconButton';
import Collapse from '@mui/material/Collapse';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import Chip from '@mui/material/Chip';
import Tooltip from '@mui/material/Tooltip';
import Breadcrumbs from '@mui/material/Breadcrumbs';
import Link from '@mui/material/Link';

// Context to track which node is being hovered
interface HoverContextType {
   hoveredNodeId: number | null;
   setHoveredNode: (id: number | null, node?: PlanNode) => void;
   highlightedChildren: Set<number>;
}

const HoverContext = createContext<HoverContextType>({
   hoveredNodeId: null,
   setHoveredNode: () => {},
   highlightedChildren: new Set(),
});

interface PlanNode {
   _label: string;
   _attrs: {
      operator_id?: number;
      estimated_cardinality?: number;
      exact_cardinality?: number;
      table_name?: string;
      table_size?: number;
      type?: string;
      method?: string;
      limit?: number;
      name?: string;
      system_representation?: string;
      [key: string]: unknown;
   };
   _children: PlanNode[];
}

interface QueryPlanData {
   queryText?: string;
   queryPlan?: PlanNode;
}

interface QueryPlanTreeProps {
   plan: QueryPlanData | object;
}

interface PlanNodeViewProps {
   node: PlanNode;
   depth: number;
   nodeId: number;
   coutByNode: Map<PlanNode, number>;
   prefix?: string; // Accumulated prefix for tree lines (│ or spaces from ancestors)
   connector?: string; // The connector character for this node (├─▶ or └─▶)
   isParentHovered?: boolean; // Whether the parent node is currently hovered
   pathToNode: PlanNode[]; // Path from original root to this node (inclusive)
   onSelectAsRoot: (path: PlanNode[]) => void;
}

// Helper function to collect direct children node IDs only
function collectDirectChildIds(node: PlanNode, ids: Set<number>): void {
   if (node._children) {
      node._children.forEach((child) => {
         const childId = child._attrs?.operator_id;
         if (childId !== undefined) {
            ids.add(childId);
         }
      });
   }
}

function isScanNode(node: PlanNode): boolean {
   return node._label.toLowerCase().includes('scan');
}

function isSyntheticResultNode(node: PlanNode): boolean {
   const label = node._label.toLowerCase();
   return label === 'result';
}

function buildCoutMap(root: PlanNode | undefined): Map<PlanNode, number> {
   const result = new Map<PlanNode, number>();
   if (!root) return result;

   const visit = (node: PlanNode): number => {
      const ownCard = typeof node._attrs?.exact_cardinality === 'number' ? node._attrs.exact_cardinality : 0;
      if (isScanNode(node)) {
         result.set(node, ownCard);
         return ownCard;
      }

      const childCout = (node._children || []).reduce((acc, child) => acc + visit(child), 0);
      const cout = isSyntheticResultNode(node) ? childCout : ownCard + childCout;
      result.set(node, cout);
      return cout;
   };

   visit(root);
   return result;
}

function formatMetric(value: number): string {
   return Number.isInteger(value)
      ? value.toLocaleString()
      : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

/**
 * Renders a single node in the query plan tree
 */
function PlanNodeView({
   node,
   depth,
   nodeId,
   coutByNode,
   prefix = '',
   connector = '',
   isParentHovered = false,
   pathToNode,
   onSelectAsRoot,
}: PlanNodeViewProps) {
   const [expanded, setExpanded] = useState(true);
   const { hoveredNodeId, setHoveredNode, highlightedChildren } = useContext(HoverContext);
   const hasChildren = node._children && node._children.length > 0;

   // Check if this node should be highlighted (it's a descendant of the hovered node)
   const isHighlighted = highlightedChildren.has(nodeId);
   const isCurrentHover = hoveredNodeId === nodeId;

   const attrs = node._attrs || {};
   const estimatedCard = attrs.estimated_cardinality;
   const exactCard = attrs.exact_cardinality;
   const nodeCout = coutByNode.get(node);
   const tableName = attrs.table_name;
   const tableSize = attrs.table_size;
   const joinType = attrs.type;
   const joinMethod = attrs.method;
   const limit = attrs.limit;
   const operatorName = attrs.name;

   // Build display label - use name attribute for CustomOperator
   const displayLabel =
      node._label === 'CustomOperator' && operatorName ? operatorName : node._label;

   // Calculate accuracy indicator
   const getCardinalityColor = () => {
      if (estimatedCard === undefined || exactCard === undefined || estimatedCard === 0) return 'default';
      const ratio = estimatedCard / exactCard;
      if (ratio >= 0.5 && ratio <= 2) return 'success';
      if (ratio >= 0.1 && ratio <= 10) return 'warning';
      return 'error';
   };

   // Build detail chips (excluding tableName and joinMethod which are now in the title)
   const details: React.ReactNode[] = [];

   if (tableSize !== undefined) {
      details.push(
         <Tooltip key="tableSize" title="Table Size">
            <Chip
               label={`${tableSize.toLocaleString()} rows`}
               size="small"
               variant="outlined"
               sx={{ height: 20, fontSize: '0.7rem' }}
            />
         </Tooltip>
      );
   }

   if (joinType) {
      details.push(
         <Chip
            key="joinType"
            label={joinType}
            size="small"
            color="info"
            variant="outlined"
            sx={{ height: 20, fontSize: '0.7rem' }}
         />
      );
   }

   if (limit !== undefined) {
      details.push(
         <Chip
            key="limit"
            label={`LIMIT ${limit}`}
            size="small"
            variant="outlined"
            sx={{ height: 20, fontSize: '0.7rem' }}
         />
      );
   }

   // Determine connector color based on hover state
   const connectorColor = isParentHovered
      ? 'primary.light'
      : isHighlighted
        ? 'primary.main'
        : 'text.disabled';

   return (
      <Box>
         {/* Row with prefix, connector, and node content */}
         <Box sx={{ display: 'flex', alignItems: 'flex-start' }}>
            {/* Prefix (vertical lines from ancestors) */}
            {prefix && (
               <Typography
                  component="span"
                  sx={{
                     fontFamily: 'monospace',
                     fontSize: '14px',
                     lineHeight: '28px',
                     color: 'text.disabled',
                     whiteSpace: 'pre',
                     userSelect: 'none',
                  }}
               >
                  {prefix}
               </Typography>
            )}
            {/* Connector for this node */}
            {connector && (
               <Typography
                  component="span"
                  sx={{
                     fontFamily: 'monospace',
                     fontSize: '14px',
                     lineHeight: '28px',
                     color: connectorColor,
                     whiteSpace: 'pre',
                     userSelect: 'none',
                     transition: 'color 0.15s ease-in-out',
                  }}
               >
                  {connector}
               </Typography>
            )}

            {/* Node content */}
            <Box
               onMouseEnter={() => {
                  setHoveredNode(nodeId, node);
               }}
               onMouseLeave={() => {
                  setHoveredNode(null);
               }}
               onDoubleClick={() => onSelectAsRoot(pathToNode)}
               sx={{
                  flex: 1,
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1,
                  py: 0.5,
                  px: 1,
                  borderRadius: 1,
                  backgroundColor: isCurrentHover
                     ? 'rgba(25, 118, 210, 0.15)'
                     : isHighlighted
                       ? 'rgba(25, 118, 210, 0.08)'
                       : '#8881',
                  mb: 0.5,
                  transition: 'all 0.15s ease-in-out',
                  borderLeft: isCurrentHover
                     ? '3px solid'
                     : isHighlighted
                       ? '2px solid'
                       : '2px solid transparent',
                  borderLeftColor: isCurrentHover
                     ? 'primary.main'
                     : isHighlighted
                       ? 'primary.light'
                       : 'transparent',
                  cursor: hasChildren ? 'pointer' : 'default',
                  '&:hover': {
                     backgroundColor: isCurrentHover ? 'rgba(25, 118, 210, 0.15)' : '#8882',
                  },
               }}
            >
               {/* Main row: label and cardinality */}
               <Box
                  sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap', flex: 1 }}
               >
                  <Typography variant="body2" sx={{ fontWeight: 600, fontFamily: 'monospace' }}>
                     {displayLabel}
                     {node._label === 'TableScan' && tableName && `: ${tableName}`}
                     {displayLabel.toLowerCase().includes('join') &&
                        joinMethod &&
                        ` (${joinMethod})`}
                  </Typography>

                  {exactCard !== undefined && (
                     <Tooltip
                        title={`Estimated: ${estimatedCard?.toLocaleString() ?? 'N/A'} | Actual: ${exactCard.toLocaleString()}`}
                     >
                        <Chip
                           label={`${exactCard.toLocaleString()} rows`}
                           size="small"
                           color={getCardinalityColor()}
                           sx={{ height: 20, fontSize: '0.7rem' }}
                        />
                     </Tooltip>
                  )}

                  {exactCard !== undefined && nodeCout !== undefined && (
                     <Tooltip
                        title="Computed recursively from exact cardinalities"
                     >
                        <Chip
                           label={`cout ${formatMetric(nodeCout)}`}
                           size="small"
                           color="secondary"
                           variant="outlined"
                           sx={{ height: 20, fontSize: '0.7rem' }}
                        />
                     </Tooltip>
                  )}

                  {details}
               </Box>

               {/* Expand/collapse button at the end */}
               {hasChildren && (
                  <IconButton
                     size="small"
                     onClick={() => setExpanded(!expanded)}
                     sx={{ p: 0, width: 24, height: 24, flexShrink: 0 }}
                  >
                     {expanded ? (
                        <ExpandMoreIcon sx={{ fontSize: 18 }} />
                     ) : (
                        <ChevronRightIcon sx={{ fontSize: 18 }} />
                     )}
                  </IconButton>
               )}
            </Box>
         </Box>

         {/* Children with tree connectors */}
         {hasChildren && (
            <Collapse in={expanded}>
               {node._children.map((child, index) => {
                  const childNodeId = child._attrs?.operator_id ?? index;
                  const isLastChild = index === node._children.length - 1;

                  // Build the connector for this child: ├─▶ or └─▶
                  const childConnector = isLastChild ? '└─▶ ' : '├─▶ ';
                  // Build the prefix for grandchildren: add │ or space based on whether this is last child
                  const childPrefix =
                     prefix + (connector ? (connector.startsWith('└') ? '    ' : '│   ') : '');

                  return (
                     <PlanNodeView
                        key={childNodeId}
                        node={child}
                        depth={depth + 1}
                        nodeId={childNodeId}
                        coutByNode={coutByNode}
                        prefix={childPrefix}
                        connector={childConnector}
                        isParentHovered={isCurrentHover}
                        pathToNode={[...pathToNode, child]}
                        onSelectAsRoot={onSelectAsRoot}
                     />
                  );
               })}
            </Collapse>
         )}
      </Box>
   );
}

/**
 * Component for displaying a query plan as an interactive tree
 */
export default function QueryPlanTree({ plan }: QueryPlanTreeProps) {
   // Parse plan only when the prop changes to avoid new object references every render
   const parsedPlan = useMemo(() => {
      if (typeof plan === 'string') {
         try {
            return JSON.parse(plan);
         } catch {
            return plan;
         }
      }
      return plan;
   }, [plan]);

   // Extract root node from parsed plan with stable reference across renders
   const rootNode = useMemo(() => {
      const planData = parsedPlan as QueryPlanData;
      if (planData && typeof planData === 'object' && 'queryPlan' in planData) {
         return planData.queryPlan as PlanNode;
      }
      if (planData && typeof planData === 'object' && '_label' in planData) {
         return planData as unknown as PlanNode;
      }
      return undefined;
   }, [parsedPlan]);

   // State for hover highlighting and current root path
   const [hoveredNodeId, setHoveredNodeId] = useState<number | null>(null);
   const [highlightedChildren, setHighlightedChildren] = useState<Set<number>>(new Set());
   const [currentPath, setCurrentPath] = useState<PlanNode[]>(rootNode ? [rootNode] : []);

   // Wrapper to update both hoveredNodeId and collect highlighted direct children
   const handleSetHoveredNodeId = (id: number | null, node?: PlanNode) => {
      setHoveredNodeId(id);
      if (id === null || !node) {
         setHighlightedChildren(new Set());
      } else {
         const directChildren = new Set<number>();
         collectDirectChildIds(node, directChildren);
         setHighlightedChildren(directChildren);
      }
   };
   // Reset current path when the plan root changes
   useEffect(() => {
      setCurrentPath(rootNode ? [rootNode] : []);
   }, [rootNode]);

   const currentRoot = currentPath[currentPath.length - 1] || rootNode;
   const rootNodeId = currentRoot?._attrs?.operator_id ?? -1;
   const coutByNode = useMemo(() => buildCoutMap(rootNode), [rootNode]);

   if (!currentRoot || !currentRoot._label) {
      const fallbackContent =
         typeof parsedPlan === 'string' ? parsedPlan : JSON.stringify(parsedPlan, null, 2);
      return (
         <Box
            sx={{
               p: 2,
               fontFamily: 'monospace',
               fontSize: '0.85rem',
               whiteSpace: 'pre-wrap',
               overflow: 'auto',
            }}
         >
            {fallbackContent}
         </Box>
      );
   }

   const getNodeBreadcrumbLabel = (node: PlanNode) => {
      const baseLabel =
         node._label === 'CustomOperator' && node._attrs?.name ? node._attrs.name : node._label;
      const tableName = node._attrs?.table_name;
      if (node._label === 'TableScan' && tableName) {
         return `${baseLabel}: ${tableName}`;
      }
      return baseLabel;
   };

   const handleBreadcrumbClick = (index: number) => {
      setCurrentPath((prev) => prev.slice(0, index + 1));
   };

   return (
      <HoverContext.Provider
         value={{
            hoveredNodeId,
            setHoveredNode: handleSetHoveredNodeId,
            highlightedChildren,
         }}
      >
         <Box
            sx={{
               p: 1,
               pt: 0.5,
               overflow: 'auto',
               display: 'flex',
               flexDirection: 'column',
               gap: 1,
            }}
         >
            {currentPath.length > 1 && (
               <Breadcrumbs
                  aria-label="Plan path"
                  maxItems={6}
                  itemsBeforeCollapse={2}
                  itemsAfterCollapse={2}
                  sx={{ mb: 0.5 }}
               >
                  {currentPath.map((node, index) => {
                     const label = getNodeBreadcrumbLabel(node);
                     const isLast = index === currentPath.length - 1;
                     return isLast ? (
                        <Typography key={index} color="text.primary" sx={{ fontSize: '0.9rem' }}>
                           {label}
                        </Typography>
                     ) : (
                        <Link
                           key={index}
                           component="button"
                           underline="hover"
                           color="text.secondary"
                           onClick={() => handleBreadcrumbClick(index)}
                           sx={{ fontSize: '0.9rem' }}
                        >
                           {label}
                        </Link>
                     );
                  })}
               </Breadcrumbs>
            )}
            <PlanNodeView
               node={currentRoot}
               depth={0}
               nodeId={rootNodeId}
               coutByNode={coutByNode}
               pathToNode={currentPath}
               onSelectAsRoot={setCurrentPath}
            />
         </Box>
      </HoverContext.Provider>
   );
}
