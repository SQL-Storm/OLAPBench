// QueryBrew (c) 2025
import { useState, useEffect, useRef } from 'react';
import Box from '@mui/material/Box';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import TableChartIcon from '@mui/icons-material/TableChart';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import SchemaIcon from '@mui/icons-material/Schema';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import CircularProgress from '@mui/material/CircularProgress';
import QueryPlanTree from './QueryPlanTree';
import QueryPlanGraph from './QueryPlanGraph';
import { QueryResponse, PlanResponse } from '../Api';
import { PlanViewMode } from '../types';

// Auto-hiding scrollbar: hidden by default, thin on hover (table + plan views).
const hiddenScrollbarSx = {
   flex: 1,
   overflow: 'auto',
   scrollbarWidth: 'none',
   msOverflowStyle: 'none',
   '&::-webkit-scrollbar': {
      width: 0,
      height: 0,
   },
   '&:hover': {
      scrollbarWidth: 'thin',
      msOverflowStyle: 'auto',
   },
   '&:hover::-webkit-scrollbar': {
      width: 8,
      height: 8,
   },
   '&:hover::-webkit-scrollbar-thumb': {
      backgroundColor: 'rgba(136, 136, 136, 0.8)',
      borderRadius: 8,
   },
   '&:hover::-webkit-scrollbar-track': {
      backgroundColor: 'transparent',
   },
} as const;

const headerBg = (theme: { palette: { mode: string } }) =>
   theme.palette.mode === 'dark' ? '#222222' : '#efefef';

function isNumericValue(cell: unknown): boolean {
   return typeof cell === 'number' || typeof cell === 'bigint';
}

/** Render a raw result cell to a display string (objects/arrays become JSON). */
function formatCellValue(cell: unknown): string {
   if (cell === null || cell === undefined) return '';
   if (typeof cell === 'object') {
      try {
         return JSON.stringify(cell);
      } catch {
         return String(cell);
      }
   }
   return String(cell);
}

interface QueryResultViewProps {
   queryResult: QueryResponse | null;
   queryPlan?: PlanResponse | null;
   isLoadingPlan?: boolean;
   onFetchPlan?: () => void;
   viewMode?: PlanViewMode;
   onViewModeChange?: (mode: PlanViewMode) => void;
}

/**
 * Component for displaying query execution results in a table or query plan
 */
export default function QueryResultView({
   queryResult,
   queryPlan,
   isLoadingPlan,
   onFetchPlan,
   viewMode,
   onViewModeChange,
}: QueryResultViewProps) {
   const [localViewMode, setLocalViewMode] = useState<PlanViewMode>('table');
   const [copied, setCopied] = useState(false);
   const prevQueryResultRef = useRef(queryResult);
   const activeViewMode = viewMode ?? localViewMode;
   const isPlanView = activeViewMode === 'plan' || activeViewMode === 'graph';

   // Track the last query result to detect changes without forcing a view reset
   useEffect(() => {
      prevQueryResultRef.current = queryResult;
   }, [queryResult]);

   // If the user is in plan view, automatically fetch the plan for the current result when needed
   useEffect(() => {
      if (
         isPlanView &&
         onFetchPlan &&
         queryResult &&
         queryResult.status === 'success' &&
         !queryPlan &&
         !isLoadingPlan
      ) {
         onFetchPlan();
      }
   }, [isPlanView, queryPlan, onFetchPlan, queryResult, isLoadingPlan]);

   const handleViewModeChange = (
      _event: React.MouseEvent<HTMLElement>,
      newMode: PlanViewMode | null
   ) => {
      if (newMode !== null) {
         if (viewMode !== undefined) {
            onViewModeChange?.(newMode);
         } else {
            setLocalViewMode(newMode);
         }
         // Fetch plan on first click if not already loaded
         if ((newMode === 'plan' || newMode === 'graph') && !queryPlan && onFetchPlan) {
            onFetchPlan();
         }
      }
   };

   if (!queryResult) {
      return (
         <Box
            sx={{
               height: '100%',
               display: 'flex',
               alignItems: 'center',
               justifyContent: 'center',
               color: '#888',
            }}
         >
            <Typography variant="body2">Run a query to see results</Typography>
         </Box>
      );
   }

   const isSuccess = queryResult.status === 'success';
   const isRunning = queryResult.status === 'running';
   const statusColor = isSuccess ? 'success' : isRunning ? 'info' : 'error';

   // Generate column headers (use provided columns or generate generic ones)
   const rows = queryResult.result || [];
   const columns =
      queryResult.columns || (rows.length > 0 ? rows[0].map((_, i) => `Column ${i + 1}`) : []);

   // Copy the result set to the clipboard as tab-separated values (header + rows).
   const handleCopyResults = async () => {
      const header = columns.join('\t');
      const body = rows.map((row) => row.map(formatCellValue).join('\t')).join('\n');
      try {
         await navigator.clipboard.writeText(body ? `${header}\n${body}` : header);
         setCopied(true);
         window.setTimeout(() => setCopied(false), 1500);
      } catch {
         // Clipboard access can be denied (e.g. insecure context); ignore silently.
      }
   };

   // A column is numeric (and thus right-aligned) when every non-null value in it is a number.
   const numericColumns = columns.map((_, colIdx) => {
      let sawValue = false;
      for (const row of rows) {
         const value = row[colIdx];
         if (value === null || value === undefined) continue;
         sawValue = true;
         if (!isNumericValue(value)) return false;
      }
      return sawValue;
   });

   // Fixed minimal width for the row-number gutter, sized to the largest row number so it
   // never absorbs leftover space (the data columns take that via table-layout: fixed).
   const gutterWidth = `calc(${String(rows.length).length}ch + 12px)`;

   // Show loading state
   if (isRunning) {
      return (
         <Box
            sx={{
               height: '100%',
               display: 'flex',
               alignItems: 'center',
               justifyContent: 'center',
               color: '#888',
            }}
         >
            <Typography variant="body2">Running query...</Typography>
         </Box>
      );
   }

   return (
      <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
         {/* Status bar */}
         <Box
            sx={{
               p: 1,
               borderBottom: '0.5px solid #8888',
               backgroundColor: '#8881',
            }}
         >
            <Stack direction="row" spacing={2} alignItems="center">
               <Chip
                  label={queryResult.status.toUpperCase()}
                  color={statusColor}
                  size="small"
                  variant="outlined"
               />
               {queryResult.runtime_ms !== undefined && (
                  <Typography variant="body2" color="text.secondary">
                     Runtime: {queryResult.runtime_ms.toFixed(2)} ms
                  </Typography>
               )}
               {queryResult.rows !== undefined && (
                  <Typography variant="body2" color="text.secondary">
                     Rows: {queryResult.rows}
                  </Typography>
               )}
               {queryResult.error && (
                  <Typography variant="body2" color="error">
                     {queryResult.error}
                  </Typography>
               )}
               <Box sx={{ flexGrow: 1 }} />
               {isSuccess && rows.length > 0 && activeViewMode === 'table' && (
                  <Tooltip title={copied ? 'Copied!' : 'Copy results (TSV)'}>
                     <IconButton
                        size="small"
                        aria-label="Copy results to clipboard"
                        onClick={handleCopyResults}
                     >
                        {copied ? (
                           <CheckIcon sx={{ fontSize: 18 }} color="success" />
                        ) : (
                           <ContentCopyIcon sx={{ fontSize: 18 }} />
                        )}
                     </IconButton>
                  </Tooltip>
               )}
               {isSuccess && (
                  <ToggleButtonGroup
                     value={activeViewMode}
                     exclusive
                     onChange={handleViewModeChange}
                     size="small"
                     aria-label="Result view mode"
                     sx={{ height: 28 }}
                  >
                     <ToggleButton value="table" sx={{ px: 1, py: 0.5 }}>
                        <TableChartIcon sx={{ fontSize: 18, mr: 0.5 }} />
                        <Typography variant="caption">Results</Typography>
                     </ToggleButton>
                     <ToggleButton value="plan" sx={{ px: 1, py: 0.5 }}>
                        <AccountTreeIcon sx={{ fontSize: 18, mr: 0.5 }} />
                        <Typography variant="caption">Plan</Typography>
                     </ToggleButton>
                     <ToggleButton value="graph" sx={{ px: 1, py: 0.5 }}>
                        <SchemaIcon sx={{ fontSize: 18, mr: 0.5 }} />
                        <Typography variant="caption">Graph</Typography>
                     </ToggleButton>
                  </ToggleButtonGroup>
               )}
            </Stack>
         </Box>

         {/* Content area - Table or Plan view */}
         {activeViewMode === 'table' ? (
            // Table view
            isSuccess && rows.length > 0 ? (
               <TableContainer component={Paper} sx={hiddenScrollbarSx}>
                  <Table
                     stickyHeader
                     size="small"
                     sx={{ tableLayout: 'fixed', width: '100%', minWidth: 650 }}
                  >
                     <TableHead>
                        <TableRow>
                           <TableCell
                              sx={{
                                 width: gutterWidth,
                                 pl: 0.5,
                                 pr: 0.5,
                                 fontWeight: 'bold',
                                 textAlign: 'right',
                                 color: 'text.disabled',
                                 userSelect: 'none',
                                 whiteSpace: 'nowrap',
                                 backgroundColor: headerBg,
                              }}
                           >
                              #
                           </TableCell>
                           {columns.map((col, index) => (
                              <TableCell
                                 key={index}
                                 align={numericColumns[index] ? 'right' : 'left'}
                                 sx={{
                                    fontWeight: 'bold',
                                    backgroundColor: headerBg,
                                    whiteSpace: 'nowrap',
                                 }}
                              >
                                 {col}
                              </TableCell>
                           ))}
                        </TableRow>
                     </TableHead>
                     <TableBody>
                        {rows.map((row, rowIndex) => (
                           <TableRow
                              key={rowIndex}
                              hover
                              sx={{ '&:nth-of-type(odd)': { backgroundColor: '#8881' } }}
                           >
                              <TableCell
                                 sx={{
                                    width: gutterWidth,
                                    pl: 0.5,
                                    pr: 0.5,
                                    textAlign: 'right',
                                    color: 'text.disabled',
                                    fontFamily: 'monospace',
                                    fontSize: '0.72rem',
                                    userSelect: 'none',
                                    whiteSpace: 'nowrap',
                                 }}
                              >
                                 {rowIndex + 1}
                              </TableCell>
                              {row.map((cell, cellIndex) => {
                                 const isNull = cell === null || cell === undefined;
                                 const display = formatCellValue(cell);
                                 return (
                                    <TableCell
                                       key={cellIndex}
                                       align={numericColumns[cellIndex] ? 'right' : 'left'}
                                       title={isNull ? 'NULL' : display}
                                       sx={{
                                          whiteSpace: 'nowrap',
                                          overflow: 'hidden',
                                          textOverflow: 'ellipsis',
                                          fontFamily: 'monospace',
                                          fontSize: '0.8rem',
                                          fontVariantNumeric: 'tabular-nums',
                                          userSelect: 'text',
                                       }}
                                    >
                                       {isNull ? (
                                          <Typography
                                             component="span"
                                             sx={{
                                                color: '#888',
                                                fontStyle: 'italic',
                                                fontSize: 'inherit',
                                             }}
                                          >
                                             NULL
                                          </Typography>
                                       ) : (
                                          display
                                       )}
                                    </TableCell>
                                 );
                              })}
                           </TableRow>
                        ))}
                     </TableBody>
                  </Table>
               </TableContainer>
            ) : isSuccess ? (
               <Box
                  sx={{
                     flex: 1,
                     display: 'flex',
                     alignItems: 'center',
                     justifyContent: 'center',
                     color: '#888',
                  }}
               >
                  <Typography variant="body2">
                     Query executed successfully (no rows returned)
                  </Typography>
               </Box>
            ) : null
         ) : (
            // Plan view
            <Box sx={{ ...hiddenScrollbarSx, p: 0.5 }}>
               {isLoadingPlan ? (
                  <Box
                     sx={{
                        height: '100%',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                     }}
                  >
                     <CircularProgress size={24} sx={{ mr: 1 }} />
                     <Typography variant="body2" color="text.secondary">
                        Loading query plan...
                     </Typography>
                  </Box>
               ) : queryPlan?.status === 'error' ? (
                  <Typography color="error">{queryPlan.error}</Typography>
               ) : queryPlan?.query_plan ? (
                  activeViewMode === 'graph' ? (
                     <QueryPlanGraph plan={queryPlan.query_plan} />
                  ) : (
                     <QueryPlanTree plan={queryPlan.query_plan} />
                  )
               ) : (
                  <Typography variant="body2" color="text.secondary">
                     No query plan available. Run a query first.
                  </Typography>
               )}
            </Box>
         )}
      </Box>
   );
}
