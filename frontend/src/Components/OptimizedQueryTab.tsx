// QueryBrew (c) 2025
import { useState, useEffect, useRef } from 'react';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import ButtonGroup from '@mui/material/ButtonGroup';
import IconButton from '@mui/material/IconButton';
import Stack from '@mui/material/Stack';
import Tooltip from '@mui/material/Tooltip';
import PlayArrow from '@mui/icons-material/PlayArrow';
import AutoFixHigh from '@mui/icons-material/AutoFixHigh';
import CodeIcon from '@mui/icons-material/Code';
import UndoIcon from '@mui/icons-material/Undo';
import AutoModeIcon from '@mui/icons-material/AutoMode';
import MonacoEditor from '@monaco-editor/react';
import { useTheme } from '@mui/material/styles';
import Dropdown from './Dropdown';
import ResizablePanels from './ResizablePanels';
import QueryResultView from './QueryResultView';
import { ActiveDbms, QueryResponse, PlanResponse, getQueryPlan } from '../Api';
import { PlanViewMode } from '../types';
import { safeFormatSQL } from '../utils/sqlFormat';
import { MONACO_SQL_OPTIONS, addFormatSqlAction } from '../utils/monaco';
import { errorMessage } from '../utils/errors';

interface OptimizedQueryTabProps {
   id: string;
   result: string;
   editedValue?: string;
   queryResult?: QueryResponse | null;
   onClose?: () => void;
   onRun: (dbms: string) => Promise<{ success: boolean; query: string }>;
   onOptimize: (dbms: string) => void;
   onEditValue: (value: string) => void;
   onDbmsChange: (dbms: string) => void;
   isLoading: boolean;
   showCloseButton: boolean;
   initialDbms?: string;
   activeDbms: ActiveDbms[];
   activeDataset: string;
   hostname: string;
   port: string;
   timeout: number;
   viewMode?: PlanViewMode;
   onViewModeChange: (mode: PlanViewMode) => void;
   queryPlan?: PlanResponse | null;
   onPlanFetched: (plan: PlanResponse | null) => void;
   autoRunEnabled: boolean;
   onToggleAutoRun: (enabled: boolean) => void;
   autoOptimize: boolean;
   onToggleAutoOptimize: (enabled: boolean) => void;
   originalQuery?: string;
   optimizedQuery?: string | null;
   onRevertOptimizedQuery: () => void;
}

/**
 * Individual optimized query view with its own database selector and run/optimize buttons
 */
export default function OptimizedQueryTab({
   result,
   editedValue,
   queryResult,
   onClose,
   onRun,
   onOptimize,
   onEditValue,
   onDbmsChange,
   isLoading,
   showCloseButton,
   initialDbms = '',
   activeDbms,
   activeDataset,
   hostname,
   port,
   timeout,
   viewMode,
   onViewModeChange,
   queryPlan,
   onPlanFetched,
   autoRunEnabled,
   onToggleAutoRun,
   autoOptimize,
   onToggleAutoOptimize,
   originalQuery,
   optimizedQuery,
   onRevertOptimizedQuery,
}: OptimizedQueryTabProps) {
   const theme = useTheme();
   const containerRef = useRef<HTMLDivElement>(null);
   const prevResultRef = useRef(result);
   const [isCompact, setIsCompact] = useState(false);
   const [dbms, setDbms] = useState(initialDbms || (activeDbms.length > 0 ? activeDbms[0].id : ''));
   const [isLoadingPlan, setIsLoadingPlan] = useState(false);
   const prevQueryResultRef = useRef(queryResult);
   const autoRunDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

   // Keep a ref to onRun that's always current
   const onRunRef = useRef(onRun);
   useEffect(() => {
      onRunRef.current = onRun;
   });

   // Keep a ref to onDbmsChange that's always current
   const onDbmsChangeRef = useRef(onDbmsChange);
   useEffect(() => {
      onDbmsChangeRef.current = onDbmsChange;
   });

   // Keep a ref to dbms that's always current
   const dbmsRef = useRef(dbms);
   useEffect(() => {
      dbmsRef.current = dbms;
   }, [dbms]);

   const autoRunQueriesRef = useRef(autoRunEnabled);
   useEffect(() => {
      autoRunQueriesRef.current = autoRunEnabled;
   }, [autoRunEnabled]);

   // Keep a ref to activeDbms that's always current (used by Monaco keyboard shortcuts)
   const activeDbmsRef = useRef(activeDbms);
   useEffect(() => {
      activeDbmsRef.current = activeDbms;
   }, [activeDbms]);

   // Keep a ref to the current query text (used by DBMS switching shortcuts)
   const queryTextRef = useRef((editedValue !== undefined ? editedValue : result) || '');
   useEffect(() => {
      queryTextRef.current = (editedValue !== undefined ? editedValue : result) || '';
   }, [editedValue, result]);

   // Clear cached plan when a new query is run
   useEffect(() => {
      if (queryResult !== prevQueryResultRef.current) {
         onPlanFetched(null);
         prevQueryResultRef.current = queryResult;
      }
   }, [queryResult, onPlanFetched]);

   // Handle fetching query plan
   const handleFetchPlan = async () => {
      const selectedDbms = activeDbms.find((d) => d.id === dbms);
      if (!selectedDbms) return;

      const queryToAnalyze = editedValue || result || '';
      if (!queryToAnalyze.trim()) return;

      setIsLoadingPlan(true);
      try {
         const planResponse = await getQueryPlan(
            queryToAnalyze,
            selectedDbms.title,
            hostname,
            port,
            timeout,
            activeDataset || undefined
         );
         onPlanFetched(planResponse);
      } catch (e) {
         onPlanFetched({ status: 'error', error: errorMessage(e, 'Failed to fetch query plan') });
      } finally {
         setIsLoadingPlan(false);
      }
   };

   // Handle dbms change and notify parent
   const handleDbmsChange = (newDbms: string) => {
      setDbms(newDbms);
      onDbmsChange(newDbms);
      const shouldRun = autoRunQueriesRef.current && queryTextRef.current.trim();
      if (shouldRun) {
         // Allow parent state (e.g., query override/cached optimization) to update before running.
         setTimeout(() => onRunRef.current(newDbms), 0);
      }
   };

   const cycleDbms = (direction: -1 | 1) => {
      const availableDbms = activeDbmsRef.current;
      if (availableDbms.length === 0) return;

      const currentDbms = dbmsRef.current;
      const currentIndex = availableDbms.findIndex((d) => d.id === currentDbms);
      const safeIndex = currentIndex >= 0 ? currentIndex : 0;
      const nextIndex = (safeIndex + direction + availableDbms.length) % availableDbms.length;
      const nextDbms = availableDbms[nextIndex]?.id;
      if (!nextDbms || nextDbms === currentDbms) return;

      setDbms(nextDbms);
      onDbmsChangeRef.current(nextDbms);
      const shouldRun = autoRunQueriesRef.current && queryTextRef.current.trim();
      if (shouldRun) {
         // Allow parent state (e.g., query override/cached optimization) to update before running.
         setTimeout(() => onRunRef.current(nextDbms), 0);
      }
   };

   // Sync dbms when activeDbms becomes available and current dbms is empty
   useEffect(() => {
      if (!dbms && activeDbms.length > 0) {
         const firstDbms = activeDbms[0].id;
         setDbms(firstDbms);
         onDbmsChange(firstDbms);
      }
   }, [activeDbms]); // eslint-disable-line react-hooks/exhaustive-deps

   // Use ResizeObserver to detect container width changes
   useEffect(() => {
      const container = containerRef.current;
      if (!container) return;

      const observer = new ResizeObserver((entries) => {
         for (const entry of entries) {
            setIsCompact(entry.contentRect.width < 500);
         }
      });

      observer.observe(container);
      return () => observer.disconnect();
   }, []);

   // Use editedValue if available, otherwise use result
   const displayValue = editedValue !== undefined ? editedValue : result;

   // Keep a ref to the latest server result so the edit handler can tell genuine
   // user edits apart from programmatic updates.
   const resultRef = useRef(result);
   useEffect(() => {
      resultRef.current = result;
   }, [result]);

   // Auto-run is debounced and triggered only by genuine user edits in the editor.
   // DBMS switches are handled (and run) explicitly in handleDbmsChange/cycleDbms,
   // so they must not also schedule a run here — otherwise the query runs twice.
   const handleValueChange = (value: string | undefined) => {
      if (value === undefined) return;
      onEditValue(value);

      if (autoRunDebounceRef.current) {
         clearTimeout(autoRunDebounceRef.current);
         autoRunDebounceRef.current = null;
      }
      const hasUserEdits = value !== resultRef.current;
      if (autoRunQueriesRef.current && hasUserEdits && dbmsRef.current && value.trim()) {
         autoRunDebounceRef.current = setTimeout(() => {
            onRunRef.current(dbmsRef.current);
         }, 350);
      }
   };

   // Cancel any pending auto-run when the panel unmounts.
   useEffect(
      () => () => {
         if (autoRunDebounceRef.current) {
            clearTimeout(autoRunDebounceRef.current);
            autoRunDebounceRef.current = null;
         }
      },
      []
   );

   // Update edited value when result changes from server (e.g., after optimization)
   // Only sync if result actually changed (not on initial mount)
   useEffect(() => {
      if (prevResultRef.current !== result) {
         onEditValue(result);
         prevResultRef.current = result;
      }
   }, [result]); // eslint-disable-line react-hooks/exhaustive-deps

   const handleRun = () => {
      onRun(dbms);
   };

   const handleOptimize = () => {
      onOptimize(dbms);
   };

   const handleFormatClick = () => {
      const formatted = safeFormatSQL(displayValue, 'format button');
      if (formatted !== displayValue) {
         handleValueChange(formatted);
      }
   };

   const canRevertOptimization = Boolean(optimizedQuery) && Boolean(originalQuery);

   return (
      <Box
         ref={containerRef}
         sx={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column' }}
      >
         <Box
            sx={{
               height: '48px',
               minHeight: '48px',
               maxHeight: '48px',
               flexShrink: 0,
               backgroundColor: '#8882',
               borderBottom: '0.5px solid #8888',
            }}
         >
            <Stack
               direction="row"
               spacing={2}
               sx={{ height: '100%', pl: 2, pr: 2, alignItems: 'center' }}
            >
               <Dropdown value={dbms} setValue={handleDbmsChange} options={activeDbms} />
               <Box sx={{ flexGrow: 1 }} />
               {canRevertOptimization && (
                  <Tooltip title="Revert to original query">
                     <IconButton size="small" onClick={onRevertOptimizedQuery}>
                        <UndoIcon sx={{ fontSize: 18 }} />
                     </IconButton>
                  </Tooltip>
               )}
               <Tooltip title="Format SQL (Ctrl+Shift+F)">
                  <IconButton size="small" onClick={handleFormatClick}>
                     <CodeIcon sx={{ fontSize: 18 }} />
                  </IconButton>
               </Tooltip>
               <ButtonGroup variant="contained" size="small" disableElevation>
                  <Tooltip title="Run Query">
                     <Button
                        sx={{
                           height: '32px',
                           minWidth: isCompact ? '40px' : undefined,
                           fontSize: '0.875rem',
                           lineHeight: 1,
                        }}
                        color="success"
                        onClick={handleRun}
                        disabled={isLoading}
                        startIcon={!isCompact ? <PlayArrow /> : undefined}
                     >
                        {isCompact ? <PlayArrow /> : isLoading ? 'Running...' : 'Run'}
                     </Button>
                  </Tooltip>
                  <Tooltip
                     title={
                        autoRunEnabled
                           ? 'Auto-run enabled (runs on DB change/query select)'
                           : 'Auto-run disabled'
                     }
                  >
                     <Button
                        sx={{ height: '32px', minWidth: isCompact ? '36px' : '40px', px: 1 }}
                        color={autoRunEnabled ? 'success' : 'inherit'}
                        onClick={() => onToggleAutoRun(!autoRunEnabled)}
                        aria-pressed={autoRunEnabled}
                        aria-label="Toggle auto-run"
                     >
                        <AutoModeIcon sx={{ fontSize: 18 }} />
                     </Button>
                  </Tooltip>
               </ButtonGroup>
               <ButtonGroup variant="contained" size="small" disableElevation>
                  <Tooltip title="Optimize Query">
                     <Button
                        sx={{
                           height: '32px',
                           minWidth: isCompact ? '40px' : undefined,
                           fontSize: '0.875rem',
                           lineHeight: 1,
                        }}
                        color="warning"
                        onClick={handleOptimize}
                        disabled={isLoading}
                        startIcon={!isCompact ? <AutoFixHigh sx={{ fontSize: 18 }} /> : undefined}
                     >
                        {isCompact ? <AutoFixHigh sx={{ fontSize: 18 }} /> : 'Optimize'}
                     </Button>
                  </Tooltip>
                  <Tooltip
                     title={
                        autoOptimize
                           ? 'Auto-optimize enabled (optimize before run)'
                           : 'Auto-optimize disabled'
                     }
                  >
                     <Button
                        sx={{ height: '32px', minWidth: isCompact ? '36px' : '40px', px: 1 }}
                        color={autoOptimize ? 'warning' : 'inherit'}
                        onClick={() => onToggleAutoOptimize(!autoOptimize)}
                        aria-pressed={autoOptimize}
                        aria-label="Toggle auto-optimize"
                     >
                        <AutoModeIcon sx={{ fontSize: 18 }} />
                     </Button>
                  </Tooltip>
               </ButtonGroup>
               {showCloseButton && onClose && (
                  <IconButton
                     onClick={onClose}
                     size="small"
                     sx={{
                        width: '32px',
                        height: '32px',
                        color: '#888',
                     }}
                  >
                     ✕
                  </IconButton>
               )}
            </Stack>
         </Box>
         <Box sx={{ flex: 1, minHeight: 0 }}>
            <ResizablePanels direction="vertical" initialSize={60} minSize={20}>
               <MonacoEditor
                  height="100%"
                  width="100%"
                  defaultLanguage="sql"
                  value={displayValue}
                  onChange={handleValueChange}
                  onMount={(editor, monaco) => {
                     // Add Ctrl+Enter keybinding to run query
                     editor.addAction({
                        id: 'run-query',
                        label: 'Run Query',
                        keybindings: [
                           monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                           monaco.KeyMod.Alt | monaco.KeyCode.Enter,
                        ],
                        run: () => {
                           // Use refs to get current values to avoid stale closures
                           const currentDbms = dbmsRef.current;
                           if (currentDbms) {
                              onRunRef.current(currentDbms);
                           }
                        },
                     });

                     // Alt+Left / Alt+Right to cycle through DBMS instances
                     editor.addAction({
                        id: 'prev-dbms',
                        label: 'Previous DBMS',
                        keybindings: [monaco.KeyMod.Alt | monaco.KeyCode.LeftArrow],
                        run: () => {
                           cycleDbms(-1);
                        },
                     });
                     editor.addAction({
                        id: 'next-dbms',
                        label: 'Next DBMS',
                        keybindings: [monaco.KeyMod.Alt | monaco.KeyCode.RightArrow],
                        run: () => {
                           cycleDbms(1);
                        },
                     });

                     // Ctrl+Shift+F to format SQL
                     addFormatSqlAction(editor, monaco, handleValueChange);
                  }}
                  theme={theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'}
                  options={MONACO_SQL_OPTIONS}
               />
               <QueryResultView
                  queryResult={queryResult || null}
                  queryPlan={queryPlan}
                  isLoadingPlan={isLoadingPlan}
                  onFetchPlan={handleFetchPlan}
                  viewMode={viewMode}
                  onViewModeChange={onViewModeChange}
               />
            </ResizablePanels>
         </Box>
      </Box>
   );
}
