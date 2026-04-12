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
import TableChartIcon from '@mui/icons-material/TableChart';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import CircularProgress from '@mui/material/CircularProgress';
import QueryPlanTree from './QueryPlanTree';

export interface QueryResult {
   status: string;
   runtime_ms?: number;
   server_time_ms?: number;
   rows?: number;
   columns?: string[];
   result?: unknown[][];
   error?: string;
}

export interface PlanResult {
   status: string;
   query_plan?: object;
   error?: string;
}

interface QueryResultViewProps {
   queryResult: QueryResult | null;
   queryPlan?: PlanResult | null;
   isLoadingPlan?: boolean;
   onFetchPlan?: () => void;
   viewMode?: 'table' | 'plan';
   onViewModeChange?: (mode: 'table' | 'plan') => void;
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
   const [localViewMode, setLocalViewMode] = useState<'table' | 'plan'>('table');
   const prevQueryResultRef = useRef(queryResult);
   const activeViewMode = viewMode ?? localViewMode;

   // Track the last query result to detect changes without forcing a view reset
   useEffect(() => {
      prevQueryResultRef.current = queryResult;
   }, [queryResult]);

   // If the user is in plan view, automatically fetch the plan for the current result when needed
   useEffect(() => {
      if (
         activeViewMode === 'plan' &&
         onFetchPlan &&
         queryResult &&
         queryResult.status === 'success' &&
         !queryPlan &&
         !isLoadingPlan
      ) {
         onFetchPlan();
      }
   }, [activeViewMode, queryPlan, onFetchPlan, queryResult, isLoadingPlan]);

   const handleViewModeChange = (
      _event: React.MouseEvent<HTMLElement>,
      newMode: 'table' | 'plan' | null
   ) => {
      if (newMode !== null) {
         if (viewMode !== undefined) {
            onViewModeChange?.(newMode);
         } else {
            setLocalViewMode(newMode);
         }
         // Fetch plan on first click if not already loaded
         if (newMode === 'plan' && !queryPlan && onFetchPlan) {
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
   const columns =
      queryResult.columns ||
      (queryResult.result && queryResult.result.length > 0
         ? queryResult.result[0].map((_, i) => `Column ${i + 1}`)
         : []);

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
               {isSuccess && (
                  <ToggleButtonGroup
                     value={activeViewMode}
                     exclusive
                     onChange={handleViewModeChange}
                     size="small"
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
                  </ToggleButtonGroup>
               )}
            </Stack>
         </Box>

         {/* Content area - Table or Plan view */}
         {activeViewMode === 'table' ? (
            // Table view
            isSuccess && queryResult.result && queryResult.result.length > 0 ? (
               <TableContainer
                  component={Paper}
                  sx={{
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
                  }}
               >
                  <Table stickyHeader size="small" sx={{ minWidth: 650 }}>
                     <TableHead>
                        <TableRow>
                           {columns.map((col, index) => (
                              <TableCell
                                 key={index}
                                 sx={{
                                    fontWeight: 'bold',
                                    backgroundColor: (theme) =>
                                       theme.palette.mode === 'dark' ? '#222222' : '#efefef',
                                    whiteSpace: 'nowrap',
                                 }}
                              >
                                 {col}
                              </TableCell>
                           ))}
                        </TableRow>
                     </TableHead>
                     <TableBody>
                        {queryResult.result.map((row, rowIndex) => (
                           <TableRow
                              key={rowIndex}
                              sx={{ '&:nth-of-type(odd)': { backgroundColor: '#8881' } }}
                           >
                              {row.map((cell, cellIndex) => (
                                 <TableCell
                                    key={cellIndex}
                                    sx={{
                                       whiteSpace: 'nowrap',
                                       maxWidth: 300,
                                       overflow: 'hidden',
                                       textOverflow: 'ellipsis',
                                    }}
                                 >
                                    {cell === null ? (
                                       <Typography
                                          component="span"
                                          sx={{ color: '#888', fontStyle: 'italic' }}
                                       >
                                          NULL
                                       </Typography>
                                    ) : (
                                       String(cell)
                                    )}
                                 </TableCell>
                              ))}
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
            <Box
               sx={{
                  flex: 1,
                  overflow: 'auto',
                  p: 0.5,
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
               }}
            >
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
                  <QueryPlanTree plan={queryPlan.query_plan} />
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
