// QueryBrew (c) 2025
import { useCallback, useLayoutEffect, useMemo, useRef, useState } from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import Tooltip from '@mui/material/Tooltip';
import IconButton from '@mui/material/IconButton';
import Paper from '@mui/material/Paper';
import Divider from '@mui/material/Divider';
import AddIcon from '@mui/icons-material/Add';
import RemoveIcon from '@mui/icons-material/Remove';
import FitScreenIcon from '@mui/icons-material/FitScreen';
import CenterFocusStrongIcon from '@mui/icons-material/CenterFocusStrong';
import { useTheme, Theme } from '@mui/material/styles';

interface PlanNode {
   _label: string;
   _attrs: {
      operator_id?: number;
      estimated_cardinality?: number;
      exact_cardinality?: number;
      table_name?: string;
      type?: string;
      method?: string;
      limit?: number;
      name?: string;
      [key: string]: unknown;
   };
   _children: PlanNode[];
}

interface QueryPlanData {
   queryText?: string;
   queryPlan?: PlanNode;
}

interface QueryPlanGraphProps {
   plan: QueryPlanData | object;
}

// Vertical gap reserved between a node and its children for the connector edges.
const LEVEL_GAP = 56;
// Horizontal gap between sibling subtrees.
const SIBLING_GAP = 32;

const MIN_SCALE = 0.2;
const MAX_SCALE = 2;

type Accuracy = 'good' | 'fair' | 'poor' | 'unknown';

interface Edge {
   path: string; // SVG path data
   labelX: number;
   labelY: number;
   rows: number;
   exact?: number;
   estimated?: number;
   accuracy: Accuracy;
}

function isScanNode(node: PlanNode): boolean {
   return node._label.toLowerCase().includes('scan');
}

// Compact row count: rounds and scales with K / M / B / T (e.g. 5,916,591 -> "5.9M").
function formatRows(value: number): string {
   const abs = Math.abs(value);
   const scaled = (n: number, suffix: string) => {
      const rounded = Math.round(n * 10) / 10;
      // Drop a trailing ".0" so e.g. 4M shows as "4M" not "4.0M".
      return `${Number.isInteger(rounded) ? rounded : rounded.toFixed(1)}${suffix}`;
   };
   if (abs >= 1e12) return scaled(value / 1e12, 'T');
   if (abs >= 1e9) return scaled(value / 1e9, 'B');
   if (abs >= 1e6) return scaled(value / 1e6, 'M');
   if (abs >= 1e3) return scaled(value / 1e3, 'K');
   return `${Math.round(value)}`;
}

// Rows flowing out of a node (and thus along the edge to its parent).
function outputRows(node: PlanNode): number | undefined {
   const exact = node._attrs?.exact_cardinality;
   if (typeof exact === 'number') return exact;
   const estimated = node._attrs?.estimated_cardinality;
   return typeof estimated === 'number' ? estimated : undefined;
}

// How close the optimizer's estimate was to the actual row count.
function cardinalityAccuracy(exact?: number, estimated?: number): Accuracy {
   if (exact === undefined || estimated === undefined || exact === 0) return 'unknown';
   const ratio = estimated / exact;
   if (ratio >= 0.5 && ratio <= 2) return 'good';
   if (ratio >= 0.1 && ratio <= 10) return 'fair';
   return 'poor';
}

function accuracyColor(theme: Theme, accuracy: Accuracy): string {
   switch (accuracy) {
      case 'good':
         return theme.palette.success.main;
      case 'fair':
         return theme.palette.warning.main;
      case 'poor':
         return theme.palette.error.main;
      default:
         return theme.palette.divider;
   }
}

function nodeLabel(node: PlanNode): string {
   const base = node._label === 'CustomOperator' && node._attrs?.name ? node._attrs.name : node._label;
   if (node._label === 'TableScan' && node._attrs?.table_name) {
      return `${base}: ${node._attrs.table_name}`;
   }
   if (base.toLowerCase().includes('join') && node._attrs?.method) {
      return `${base} (${node._attrs.method})`;
   }
   return base;
}

/**
 * Renders one subtree (node on top, children laid out in a row below it).
 * Each node is given a ref so edges can be measured and drawn afterwards.
 */
function Subtree({
   node,
   path,
   registerRef,
}: {
   node: PlanNode;
   path: string;
   registerRef: (key: string, el: HTMLDivElement | null) => void;
}) {
   const theme = useTheme();
   const children = node._children || [];
   const scan = isScanNode(node);
   const joinType = node._attrs?.type;
   const limit = node._attrs?.limit;
   const tableName = node._label === 'TableScan' ? node._attrs?.table_name : undefined;

   const accent = scan ? theme.palette.success.main : theme.palette.primary.main;

   return (
      <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', flexShrink: 0 }}>
         <Box
            ref={(el: HTMLDivElement | null) => registerRef(path, el)}
            sx={{
               position: 'relative',
               zIndex: 1,
               px: 1.25,
               py: 0.75,
               borderRadius: 1.5,
               border: '1px solid',
               borderColor: accent,
               borderTopWidth: 3,
               backgroundColor: theme.palette.background.paper,
               boxShadow: 1,
               display: 'flex',
               flexDirection: 'column',
               alignItems: 'center',
               gap: 0.25,
               whiteSpace: 'nowrap',
            }}
         >
            {tableName ? (
               <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                  <Typography variant="body2" sx={{ fontWeight: 600, fontFamily: 'monospace' }}>
                     {nodeLabel({ ...node, _attrs: { ...node._attrs, table_name: undefined } })}
                  </Typography>
                  <Typography
                     variant="body2"
                     sx={{ fontWeight: 600, fontFamily: 'monospace', color: 'success.main' }}
                  >
                     {tableName}
                  </Typography>
               </Box>
            ) : (
               <Typography variant="body2" sx={{ fontWeight: 600, fontFamily: 'monospace' }}>
                  {nodeLabel(node)}
               </Typography>
            )}
            {(joinType || limit !== undefined) && (
               <Box sx={{ display: 'flex', gap: 0.5 }}>
                  {joinType && (
                     <Chip
                        label={joinType}
                        size="small"
                        color="info"
                        variant="outlined"
                        sx={{ height: 18, fontSize: '0.65rem' }}
                     />
                  )}
                  {limit !== undefined && (
                     <Chip
                        label={`LIMIT ${limit}`}
                        size="small"
                        variant="outlined"
                        sx={{ height: 18, fontSize: '0.65rem' }}
                     />
                  )}
               </Box>
            )}
         </Box>

         {children.length > 0 && (
            <Box
               sx={{
                  display: 'flex',
                  flexDirection: 'row',
                  alignItems: 'flex-start',
                  justifyContent: 'center',
                  gap: `${SIBLING_GAP}px`,
                  mt: `${LEVEL_GAP}px`,
               }}
            >
               {children.map((child, index) => (
                  <Subtree
                     key={child._attrs?.operator_id ?? index}
                     node={child}
                     path={`${path}.${index}`}
                     registerRef={registerRef}
                  />
               ))}
            </Box>
         )}
      </Box>
   );
}

/**
 * Displays a query plan as a top-down tree: the root operator is at the top and
 * table scans are at the bottom. The number of rows produced by each operator is
 * drawn on the edge connecting it to its parent. Supports zoom, fit-to-screen and
 * drag-to-pan, and colours each edge by how accurate the cardinality estimate was.
 */
export default function QueryPlanGraph({ plan }: QueryPlanGraphProps) {
   const theme = useTheme();

   const rootNode = useMemo(() => {
      const parsed = typeof plan === 'string' ? safeParse(plan) : plan;
      const planData = parsed as QueryPlanData;
      if (planData && typeof planData === 'object' && 'queryPlan' in planData) {
         return planData.queryPlan as PlanNode;
      }
      if (planData && typeof planData === 'object' && '_label' in planData) {
         return planData as unknown as PlanNode;
      }
      return undefined;
   }, [plan]);

   const viewportRef = useRef<HTMLDivElement>(null);
   const contentRef = useRef<HTMLDivElement>(null);
   const nodeRefs = useRef<Map<string, HTMLDivElement>>(new Map());
   const [edges, setEdges] = useState<Edge[]>([]);
   const [size, setSize] = useState({ width: 0, height: 0 });
   // Image-style transform: pan offset (x, y) and zoom (scale).
   const [view, setView] = useState({ x: 0, y: 0, scale: 1 });
   const [dragging, setDragging] = useState(false);
   const scale = view.scale;

   const registerRef = useCallback((key: string, el: HTMLDivElement | null) => {
      if (el) nodeRefs.current.set(key, el);
      else nodeRefs.current.delete(key);
   }, []);

   // Walk the tree to build (parent -> child) edges keyed by the same paths used in render.
   const edgeSpecs = useMemo(() => {
      const specs: {
         parent: string;
         child: string;
         rows: number | undefined;
         exact?: number;
         estimated?: number;
      }[] = [];
      const visit = (node: PlanNode, path: string) => {
         (node._children || []).forEach((child, index) => {
            const childPath = `${path}.${index}`;
            specs.push({
               parent: path,
               child: childPath,
               rows: outputRows(child),
               exact: child._attrs?.exact_cardinality,
               estimated: child._attrs?.estimated_cardinality,
            });
            visit(child, childPath);
         });
      };
      if (rootNode) visit(rootNode, 'r');
      return specs;
   }, [rootNode]);

   // Measure node positions with offset* (layout coordinates, unaffected by the
   // CSS zoom transform) so edges stay correct at any scale.
   const recompute = useCallback(() => {
      const content = contentRef.current;
      if (!content) return;
      const next: Edge[] = [];
      for (const spec of edgeSpecs) {
         const parentEl = nodeRefs.current.get(spec.parent);
         const childEl = nodeRefs.current.get(spec.child);
         if (!parentEl || !childEl) continue;
         const x1 = parentEl.offsetLeft + parentEl.offsetWidth / 2;
         const y1 = parentEl.offsetTop + parentEl.offsetHeight;
         const x2 = childEl.offsetLeft + childEl.offsetWidth / 2;
         const y2 = childEl.offsetTop;
         const midY = (y1 + y2) / 2;
         next.push({
            path: `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`,
            labelX: (x1 + x2) / 2,
            labelY: midY,
            rows: spec.rows ?? NaN,
            exact: spec.exact,
            estimated: spec.estimated,
            accuracy: cardinalityAccuracy(spec.exact, spec.estimated),
         });
      }
      setEdges(next);
      setSize({ width: content.offsetWidth, height: content.offsetHeight });
   }, [edgeSpecs]);

   const clampScale = (s: number) => Math.min(MAX_SCALE, Math.max(MIN_SCALE, s));

   // Horizontal centre of the root node within the (unscaled) content layout.
   const rootCenterX = useCallback(() => {
      const rootEl = nodeRefs.current.get('r');
      return rootEl ? rootEl.offsetLeft + rootEl.offsetWidth / 2 : 0;
   }, []);

   // Place the root node in the top-middle of the viewport at the given scale.
   const centerRoot = useCallback(
      (s: number) => {
         const viewport = viewportRef.current;
         if (!viewport) return;
         setView({ scale: s, x: viewport.clientWidth / 2 - rootCenterX() * s, y: 24 });
      },
      [rootCenterX]
   );

   const resetView = useCallback(() => centerRoot(1), [centerRoot]);

   // Scale the plan so it fits inside the current viewport, root centred at top.
   const fitToScreen = useCallback(() => {
      const viewport = viewportRef.current;
      if (!viewport || size.width === 0 || size.height === 0) return;
      const padding = 32;
      const fit = clampScale(
         Math.min(
            (viewport.clientWidth - padding) / size.width,
            (viewport.clientHeight - padding) / size.height
         )
      );
      centerRoot(fit);
   }, [size, centerRoot]);

   // Zoom keeping the given viewport point (default: centre) anchored.
   const zoomAt = useCallback((factor: number, px?: number, py?: number) => {
      const viewport = viewportRef.current;
      if (!viewport) return;
      const cx = px ?? viewport.clientWidth / 2;
      const cy = py ?? viewport.clientHeight / 2;
      setView((v) => {
         const next = clampScale(v.scale * factor);
         const ratio = next / v.scale;
         return { scale: next, x: cx - (cx - v.x) * ratio, y: cy - (cy - v.y) * ratio };
      });
   }, []);

   const zoomBy = useCallback((factor: number) => zoomAt(factor), [zoomAt]);

   useLayoutEffect(() => {
      recompute();
      const content = contentRef.current;
      if (!content || typeof ResizeObserver === 'undefined') return;
      const observer = new ResizeObserver(() => recompute());
      observer.observe(content);
      return () => observer.disconnect();
   }, [recompute]);

   // Centre the root once the plan has been laid out (and again for a new plan).
   const didInitRef = useRef(false);
   useLayoutEffect(() => {
      didInitRef.current = false;
   }, [rootNode]);
   useLayoutEffect(() => {
      if (didInitRef.current || size.width === 0) return;
      didInitRef.current = true;
      resetView();
   }, [size, resetView]);

   // Plain wheel zooms toward the cursor. Attached natively so preventDefault works
   // (React's onWheel is passive and cannot block the page from scrolling).
   useLayoutEffect(() => {
      const viewport = viewportRef.current;
      if (!viewport) return;
      const handler = (e: WheelEvent) => {
         e.preventDefault();
         const rect = viewport.getBoundingClientRect();
         zoomAt(e.deltaY < 0 ? 1.1 : 1 / 1.1, e.clientX - rect.left, e.clientY - rect.top);
      };
      viewport.addEventListener('wheel', handler, { passive: false });
      return () => viewport.removeEventListener('wheel', handler);
   }, [zoomAt]);

   // Drag anywhere to pan, like moving an image.
   const panState = useRef<{ x: number; y: number; vx: number; vy: number } | null>(null);
   const onPointerDown = (e: React.PointerEvent) => {
      panState.current = { x: e.clientX, y: e.clientY, vx: view.x, vy: view.y };
      setDragging(true);
      viewportRef.current?.setPointerCapture(e.pointerId);
   };
   const onPointerMove = (e: React.PointerEvent) => {
      const start = panState.current;
      if (!start) return;
      setView((v) => ({
         ...v,
         x: start.vx + (e.clientX - start.x),
         y: start.vy + (e.clientY - start.y),
      }));
   };
   const endPan = (e: React.PointerEvent) => {
      panState.current = null;
      setDragging(false);
      viewportRef.current?.releasePointerCapture(e.pointerId);
   };

   if (!rootNode || !rootNode._label) {
      return (
         <Box sx={{ p: 2 }}>
            <Typography variant="body2" color="text.secondary">
               No query plan available.
            </Typography>
         </Box>
      );
   }

   const legendItems: { label: string; accuracy: Accuracy }[] = [
      { label: 'within 2×', accuracy: 'good' },
      { label: 'within 10×', accuracy: 'fair' },
      { label: 'off by >10×', accuracy: 'poor' },
   ];

   return (
      <Box sx={{ position: 'relative', width: '100%', height: '100%', overflow: 'hidden' }}>
         {/* Zoom / fit controls */}
         <Paper
            elevation={3}
            sx={{
               position: 'absolute',
               top: 8,
               right: 8,
               zIndex: 2,
               display: 'flex',
               alignItems: 'center',
            }}
         >
            <Tooltip title="Zoom out">
               <IconButton size="small" aria-label="Zoom out" onClick={() => zoomBy(1 / 1.2)}>
                  <RemoveIcon fontSize="small" />
               </IconButton>
            </Tooltip>
            <Tooltip title="Reset to 100%">
               <Box
                  component="button"
                  onClick={resetView}
                  sx={{
                     border: 'none',
                     background: 'none',
                     cursor: 'pointer',
                     color: 'text.secondary',
                     fontSize: '0.72rem',
                     fontFamily: 'monospace',
                     width: 44,
                  }}
               >
                  {Math.round(scale * 100)}%
               </Box>
            </Tooltip>
            <Tooltip title="Zoom in">
               <IconButton size="small" aria-label="Zoom in" onClick={() => zoomBy(1.2)}>
                  <AddIcon fontSize="small" />
               </IconButton>
            </Tooltip>
            <Divider orientation="vertical" flexItem sx={{ mx: 0.25 }} />
            <Tooltip title="Fit to screen">
               <IconButton size="small" aria-label="Fit to screen" onClick={fitToScreen}>
                  <FitScreenIcon fontSize="small" />
               </IconButton>
            </Tooltip>
            <Tooltip title="Actual size (100%), centred">
               <IconButton size="small" aria-label="Actual size" onClick={resetView}>
                  <CenterFocusStrongIcon fontSize="small" />
               </IconButton>
            </Tooltip>
         </Paper>

         {/* Estimation-accuracy legend */}
         <Paper
            elevation={3}
            sx={{
               position: 'absolute',
               bottom: 8,
               left: 8,
               zIndex: 2,
               px: 1,
               py: 0.5,
               display: 'flex',
               alignItems: 'center',
               gap: 1.25,
            }}
         >
            <Typography variant="caption" color="text.secondary">
               Estimate
            </Typography>
            {legendItems.map((item) => (
               <Box key={item.accuracy} sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                  <Box
                     sx={{
                        width: 14,
                        height: 3,
                        borderRadius: 1,
                        backgroundColor: accuracyColor(theme, item.accuracy),
                     }}
                  />
                  <Typography variant="caption" color="text.secondary">
                     {item.label}
                  </Typography>
               </Box>
            ))}
         </Paper>

         {/* Pannable / zoomable viewport (no scrollbars; drag to pan, wheel to zoom) */}
         <Box
            ref={viewportRef}
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={endPan}
            onPointerCancel={endPan}
            sx={{
               width: '100%',
               height: '100%',
               overflow: 'hidden',
               touchAction: 'none',
               userSelect: 'none',
               cursor: dragging ? 'grabbing' : 'grab',
            }}
         >
            <Box
               ref={contentRef}
               sx={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  display: 'inline-flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  transform: `translate(${view.x}px, ${view.y}px) scale(${scale})`,
                  transformOrigin: 'top left',
               }}
            >
                  {/* Edge layer drawn behind the nodes */}
                  <Box
                     component="svg"
                     sx={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        pointerEvents: 'none',
                        zIndex: 0,
                     }}
                     width={size.width}
                     height={size.height}
                  >
                     {edges.map((edge, index) => (
                        <path
                           key={index}
                           d={edge.path}
                           fill="none"
                           stroke={accuracyColor(theme, edge.accuracy)}
                           strokeWidth={1.5}
                           strokeOpacity={edge.accuracy === 'unknown' ? 1 : 0.85}
                        />
                     ))}
                  </Box>

                  {/* Edge row-count labels */}
                  {edges.map((edge, index) =>
                     Number.isNaN(edge.rows) ? null : (
                        <Tooltip
                           key={index}
                           title={
                              <>
                                 Exact:{' '}
                                 {edge.exact !== undefined ? edge.exact.toLocaleString() : 'N/A'}
                                 <br />
                                 Estimated:{' '}
                                 {edge.estimated !== undefined
                                    ? edge.estimated.toLocaleString()
                                    : 'N/A'}
                              </>
                           }
                        >
                           <Box
                              sx={{
                                 position: 'absolute',
                                 left: edge.labelX,
                                 top: edge.labelY,
                                 transform: 'translate(-50%, -50%)',
                                 zIndex: 1,
                                 px: 0.5,
                                 borderRadius: 0.75,
                                 border: '1px solid',
                                 borderColor: accuracyColor(theme, edge.accuracy),
                                 backgroundColor: theme.palette.background.paper,
                                 fontFamily: 'monospace',
                                 fontSize: '0.7rem',
                                 color: 'text.secondary',
                                 whiteSpace: 'nowrap',
                                 cursor: 'default',
                              }}
                           >
                              {formatRows(edge.rows)}
                           </Box>
                        </Tooltip>
                     )
                  )}

                  <Subtree node={rootNode} path="r" registerRef={registerRef} />
            </Box>
         </Box>
      </Box>
   );
}

function safeParse(value: string): unknown {
   try {
      return JSON.parse(value);
   } catch {
      return value;
   }
}
