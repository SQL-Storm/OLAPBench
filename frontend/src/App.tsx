// QueryBrew (c) 2025
import { useState, useCallback, useMemo } from 'react';
import Box from '@mui/material/Box';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import TitleBar from './Components/TitleBar';
import QueryView from './Components/QueryView';
import SchemaView from './Components/SchemaView';
import MultiOutputView from './Components/MultiOutputView';
import ErrorBoundary from './Components/ErrorBoundary';
import ResizablePanels from './Components/ResizablePanels';
import { checkHealth, getDataset, Query, ActiveDbms, type BenchmarkInfo } from './Api';
import { LAYOUT } from './constants';
import { safeFormatSQL } from './utils/sqlFormat';
import { buildDbmsWithIds } from './utils/dbms';
import { parseDatasetOverrides, resolveQueryForDbms } from './utils/queries';
import { errorMessage } from './utils/errors';
import { useOutputs } from './hooks/useOutputs';

const SCHEMA_PLACEHOLDER = '-- Connect to server to load schema';

/**
 * Main application component
 * Renders a split-view SQL query optimizer interface with:
 * - Title bar with connection controls
 * - Schema view (read-only)
 * - Query input view with benchmark query selector
 * - Multiple optimized query output views
 */
export default function App() {
   const [darkMode, setDarkMode] = useState(true);
   const [hostname, setHostname] = useState('https://querybrew.db.cit.tum.de/api');
   const [port, setPort] = useState('443');
   const [isConnected, setIsConnected] = useState(false);
   const [schema, setSchema] = useState(SCHEMA_PLACEHOLDER);
   const [queries, setQueries] = useState<Query[]>([]);
   const [queryOverrides, setQueryOverrides] = useState<Record<string, Record<string, string>>>({});
   const [timeoutInput, setTimeoutInput] = useState('5');
   const [resultLimit, setResultLimit] = useState('50');
   const [benchmarkName, setBenchmarkName] = useState('');
   const [connectionError, setConnectionError] = useState<string | null>(null);
   const [query, setQuery] = useState('');
   const [currentQueryIndex, setCurrentQueryIndex] = useState<number | null>(null);
   const [activeDbms, setActiveDbms] = useState<ActiveDbms[]>([]);
   const [datasets, setDatasets] = useState<BenchmarkInfo[]>([]);
   const [activeDataset, setActiveDataset] = useState('');

   const theme = useMemo(
      () => createTheme({ palette: { mode: darkMode ? 'dark' : 'light' } }),
      [darkMode]
   );
   const toggleDarkMode = useCallback(() => setDarkMode((prev) => !prev), []);

   const parsedTimeout = useMemo(() => {
      const parsed = Number(timeoutInput);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
   }, [timeoutInput]);
   const parsedResultLimit = useMemo(() => {
      const parsed = Number(resultLimit);
      return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 50;
   }, [resultLimit]);

   const resolveForDbms = useCallback(
      (dbmsId: string, baseSql: string, queryName?: string) =>
         resolveQueryForDbms({ queries, activeDbms, queryOverrides }, dbmsId, baseSql, queryName),
      [queries, activeDbms, queryOverrides]
   );

   const outputs = useOutputs({
      hostname,
      port,
      activeDbms,
      activeDataset,
      timeout: parsedTimeout,
      resultLimit: parsedResultLimit,
      query,
      resolveForDbms,
   });

   const loadDataset = useCallback(
      async (datasetName: string) => {
         const datasetResponse = await getDataset(hostname, port, datasetName);
         setSchema(safeFormatSQL(datasetResponse.schema, 'schema'));
         setQueryOverrides(parseDatasetOverrides(datasetResponse));
         setQueries(datasetResponse.queries);
         setBenchmarkName(datasetResponse.benchmark);
         return datasetResponse.queries;
      },
      [hostname, port]
   );

   const handleDatasetChange = useCallback(
      async (datasetName: string) => {
         setActiveDataset(datasetName);
         setQuery('');
         setCurrentQueryIndex(null);
         // Switch the active DBMS list to the selected dataset's systems
         const benchmark = datasets.find((b) => b.name === datasetName);
         setActiveDbms(buildDbmsWithIds(benchmark?.systems));
         try {
            const loadedQueries = await loadDataset(datasetName);
            if (loadedQueries.length > 0) {
               setQuery(loadedQueries[0].sql);
               setCurrentQueryIndex(0);
            }
         } catch (e) {
            setConnectionError(errorMessage(e, 'Failed to load dataset'));
         }
      },
      [loadDataset, datasets]
   );

   const handleConnect = useCallback(async () => {
      setConnectionError(null);
      try {
         const healthResponse = await checkHealth(hostname, port);
         setDatasets(healthResponse.benchmarks);

         // Use the first benchmark's systems as the active dataset/DBMS
         const firstBenchmark = healthResponse.benchmarks[0];
         const datasetName = firstBenchmark?.name ?? '';
         setActiveDataset(datasetName);
         setActiveDbms(buildDbmsWithIds(firstBenchmark?.systems));

         const loadedQueries = await loadDataset(datasetName);
         setIsConnected(true);

         if (loadedQueries.length > 0 && !query) {
            setQuery(loadedQueries[0].sql);
            setCurrentQueryIndex(0);
         }
      } catch (e) {
         console.error('Connection failed:', e);
         setConnectionError(errorMessage(e, 'Failed to connect to server'));
         setIsConnected(false);
      }
   }, [hostname, port, loadDataset, query]);

   const handleDisconnect = useCallback(() => {
      setIsConnected(false);
      setSchema(SCHEMA_PLACEHOLDER);
      setQueries([]);
      setQueryOverrides({});
      setBenchmarkName('');
      setQuery('');
      setCurrentQueryIndex(null);
      setConnectionError(null);
      setDatasets([]);
      setActiveDataset('');
      setActiveDbms([]);
   }, []);

   const handleHostnameChange = useCallback(
      (newHostname: string) => {
         setHostname(newHostname);
         if (isConnected) handleDisconnect();
      },
      [isConnected, handleDisconnect]
   );

   const handlePortChange = useCallback(
      (newPort: string) => {
         setPort(newPort);
         if (isConnected) handleDisconnect();
      },
      [isConnected, handleDisconnect]
   );

   return (
      <ThemeProvider theme={theme}>
         <CssBaseline />
         <ErrorBoundary>
            <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
               <Box sx={{ height: LAYOUT.TITLE_BAR_HEIGHT, borderBottom: LAYOUT.BORDER_STYLE }}>
                  <TitleBar
                     handleConnect={handleConnect}
                     isLoading={outputs.isLoading}
                     isConnected={isConnected}
                     hostname={hostname}
                     setHostname={handleHostnameChange}
                     port={port}
                     setPort={handlePortChange}
                     timeout={timeoutInput}
                     onTimeoutChange={setTimeoutInput}
                     resultLimit={resultLimit}
                     setResultLimit={setResultLimit}
                     onAddOutput={outputs.addOutput}
                     darkMode={darkMode}
                     toggleDarkMode={toggleDarkMode}
                  />
               </Box>

               <Box sx={{ flex: 1, overflow: 'hidden' }}>
                  <ResizablePanels direction="horizontal" initialSize={50} minSize={20}>
                     <ResizablePanels direction="vertical" initialSize={50} minSize={20}>
                        <SchemaView
                           schema={
                              connectionError
                                 ? `-- Connection Error:\n-- ${connectionError}\n\n${SCHEMA_PLACEHOLDER}`
                                 : schema
                           }
                           datasets={datasets.map((b) => b.name)}
                           activeDataset={activeDataset}
                           onDatasetChange={handleDatasetChange}
                        />
                        <QueryView
                           query={query}
                           setQuery={setQuery}
                           queries={queries}
                           benchmarkName={benchmarkName}
                           onRunAll={outputs.runAll}
                           onAutoRunAll={outputs.autoRunAll}
                           isLoading={outputs.isLoading}
                           currentQueryIndex={currentQueryIndex}
                           setCurrentQueryIndex={setCurrentQueryIndex}
                           autoRunEnabled={outputs.outputs.some(
                              (out) => out.autoRunEnabled !== false
                           )}
                        />
                     </ResizablePanels>
                     <MultiOutputView
                        outputs={outputs.outputs}
                        onRun={outputs.run}
                        onOptimize={outputs.optimize}
                        onRevertOptimizedQuery={outputs.revertOptimizedQuery}
                        onCloseOutput={outputs.closeOutput}
                        onEditOutput={outputs.editOutput}
                        onDbmsChange={outputs.dbmsChange}
                        onViewModeChange={outputs.viewModeChange}
                        onPlanFetched={outputs.planFetched}
                        onToggleAutoRun={outputs.toggleAutoRun}
                        onToggleAutoOptimize={outputs.toggleAutoOptimize}
                        isLoading={outputs.isLoading}
                        activeDbms={activeDbms}
                        activeDataset={activeDataset}
                        hostname={hostname}
                        port={port}
                        timeout={parsedTimeout}
                     />
                  </ResizablePanels>
               </Box>
            </Box>
         </ErrorBoundary>
      </ThemeProvider>
   );
}
