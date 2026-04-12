// QueryBrew (c) 2025
import { useState, useCallback, useEffect, useMemo } from 'react';
import Box from '@mui/material/Box';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import TitleBar from './Components/TitleBar';
import QueryView from './Components/QueryView';
import SchemaView from './Components/SchemaView';
import MultiOutputView from './Components/MultiOutputView';
import ErrorBoundary from './Components/ErrorBoundary';
import ResizablePanels from './Components/ResizablePanels';
import {
   checkHealth,
   getDataset,
   Query,
   optimize,
   runQuery,
   getQueryPlan,
   ActiveDbms,
   type BenchmarkInfo,
   QueryResponse,
} from './Api';
import { LAYOUT } from './constants';
import { PlanResult } from './Components/QueryResultView';
import { safeFormatSQL } from './utils/sqlFormat';

interface OutputResult {
   id: string;
   result: string;
   error: string | null;
   dbms: string;
   editedValue?: string; // User's edited version of the result
   originalQuery?: string; // Unoptimized query text (used for optimization)
   optimizedQuery?: string | null; // Cached optimized query for originalQuery+DBMS
   queryResult?: QueryResponse | null; // Result from running the query
   viewMode?: 'table' | 'plan';
   queryPlan?: PlanResult | null;
   autoRunEnabled?: boolean;
   autoOptimize?: boolean;
}

/**
 * Main application component
 * Renders a split-view SQL query optimizer interface with:
 * - Title bar with connection controls
 * - Schema view (read-only)
 * - Query input view with benchmark query selector
 * - Multiple optimized query output views
 */
export default function App() {
   const [darkMode, setDarkMode] = useState<boolean>(true);
   const [hostname, setHostname] = useState<string>('localhost');
   const [port, setPort] = useState<string>('5000');
   const [isConnected, setIsConnected] = useState<boolean>(false);
   const [schema, setSchema] = useState<string>('-- Connect to server to load schema');
   const [queries, setQueries] = useState<Query[]>([]);
   const [queryOverrides, setQueryOverrides] = useState<Record<string, Record<string, string>>>(
      {}
   );
   const [timeout, setTimeout] = useState<string>('5');
   const [resultLimit, setResultLimit] = useState<string>('50');
   const [benchmarkName, setBenchmarkName] = useState<string>('');
   const [connectionError, setConnectionError] = useState<string | null>(null);
   const [query, setQuery] = useState<string>('');
   const [currentQueryIndex, setCurrentQueryIndex] = useState<number | null>(null);
   const [activeDbms, setActiveDbms] = useState<ActiveDbms[]>([]);
   const [datasets, setDatasets] = useState<BenchmarkInfo[]>([]);
   const [activeDataset, setActiveDataset] = useState<string>('');
   const [outputs, setOutputs] = useState<OutputResult[]>([
      {
         id: '1',
         result: '-- Your optimized query will be displayed here',
         error: null,
         dbms: '',
         originalQuery: '-- Your optimized query will be displayed here',
         optimizedQuery: null,
         viewMode: 'table',
         queryPlan: null,
         autoRunEnabled: true,
         autoOptimize: false,
      },
   ]);

   // Create theme based on mode
   const theme = useMemo(
      () =>
         createTheme({
            palette: {
               mode: darkMode ? 'dark' : 'light',
            },
         }),
      [darkMode]
   );

   const toggleDarkMode = useCallback(() => {
      setDarkMode((prev) => !prev);
   }, []);
   const [isLoading, setIsLoading] = useState<boolean>(false);
   const parsedTimeout = useMemo(() => {
      const parsed = Number(timeout);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : 5;
   }, [timeout]);
   const parsedResultLimit = useMemo(() => {
      const parsed = Number(resultLimit);
      return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 50;
   }, [resultLimit]);

   const shouldApplyOverridesFor = useCallback(
      (baseSql: string, queryName?: string) => {
         const targetName = queryName || queries.find((q) => q.sql === baseSql)?.name;
         if (!targetName) return false;
         const matchingQuery = queries.find((q) => q.name === targetName);
         if (!matchingQuery) return false;
         return matchingQuery.sql === baseSql;
      },
      [queries]
   );

   const resolveQueryForDbms = useCallback(
      (dbmsId: string, baseSql: string, queryName?: string) => {
         if (!shouldApplyOverridesFor(baseSql, queryName)) {
            return baseSql;
         }

         const dbms = activeDbms.find((d) => d.id === dbmsId);
         if (!dbms) return baseSql;

         const targetName = queryName || queries.find((q) => q.sql === baseSql)?.name;
         if (!targetName) return baseSql;

         const overrides = queryOverrides[targetName];
         if (!overrides) return baseSql;

         return overrides[dbms.name] || overrides[dbms.title] || overrides[dbms.id] || baseSql;
      },
      [activeDbms, queryOverrides, shouldApplyOverridesFor, queries]
   );

   // Update all output views when query changes (e.g., when selecting a new query)
   useEffect(() => {
      if (query) {
         setOutputs((prev) =>
            prev.map((out) => ({
               ...out,
               result: resolveQueryForDbms(out.dbms, query),
               editedValue: resolveQueryForDbms(out.dbms, query),
               originalQuery: resolveQueryForDbms(out.dbms, query),
               optimizedQuery: null,
               queryPlan: null,
               queryResult: out.autoRunEnabled === false ? null : out.queryResult,
            }))
         );
      }
   }, [query, resolveQueryForDbms]);

   const loadDataset = useCallback(
      async (datasetName: string) => {
         const datasetResponse = await getDataset(hostname, port, datasetName);
         console.log('Dataset:', datasetResponse);

         const formattedSchema = safeFormatSQL(datasetResponse.schema, 'schema');
         setSchema(formattedSchema);

         const overridesFromQueries = datasetResponse.queries.reduce(
            (acc, q) => {
               const overrides: Record<string, string> = {};
               Object.entries(q).forEach(([key, value]) => {
                  if (key === 'name' || key === 'sql') return;
                  if (typeof value === 'string' && value.trim()) {
                     overrides[key] = value;
                  }
               });
               if (Object.keys(overrides).length > 0) {
                  acc[q.name] = overrides;
               }
               return acc;
            },
            {} as Record<string, Record<string, string>>
         );

         const overridesFromLegacy = (datasetResponse.query_overrides || []).reduce(
            (acc, override) => {
               const dbmsKey =
                  (override as any)?.dbms ||
                  (override as any)?.system ||
                  (override as any)?.target_dbms ||
                  (override as any)?.target;
               if (!override?.name || !dbmsKey || !override?.sql) {
                  return acc;
               }
               if (!acc[override.name]) {
                  acc[override.name] = {};
               }
               acc[override.name][dbmsKey] = override.sql;
               return acc;
            },
            {} as Record<string, Record<string, string>>
         );

         const mergedOverrides = { ...overridesFromQueries };
         for (const [name, overrides] of Object.entries(overridesFromLegacy)) {
            mergedOverrides[name] = { ...(mergedOverrides[name] || {}), ...overrides };
         }

         setQueryOverrides(mergedOverrides);
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
         setDatasets((prev) => {
            const benchmark = prev.find((b) => b.name === datasetName);
            const nameCounts: Record<string, number> = {};
            const dbmsWithIds = (benchmark?.systems ?? []).map((sys) => {
               nameCounts[sys.name] = (nameCounts[sys.name] || 0) + 1;
               const count = nameCounts[sys.name];
               return {
                  title: sys.title,
                  name: sys.name,
                  id: count === 1 ? sys.name : `${sys.name}-${count}`,
               };
            });
            setActiveDbms(dbmsWithIds);
            return prev;
         });
         try {
            const loadedQueries = await loadDataset(datasetName);
            if (loadedQueries.length > 0) {
               setQuery(loadedQueries[0].sql);
               setCurrentQueryIndex(0);
            }
         } catch (e: any) {
            setConnectionError(e?.message || 'Failed to load dataset');
         }
      },
      [loadDataset]
   );

   const handleConnect = useCallback(async () => {
      setConnectionError(null);
      try {
         // Check server health
         const healthResponse = await checkHealth(hostname, port);
         console.log('Server health:', healthResponse);

         setDatasets(healthResponse.benchmarks);

         // Use the first benchmark's systems as active DBMS, or all systems of selected dataset
         const firstBenchmark = healthResponse.benchmarks[0];
         const datasetName = firstBenchmark?.name ?? '';
         setActiveDataset(datasetName);

         // Build activeDbms from first dataset's systems
         const nameCounts: Record<string, number> = {};
         const dbmsWithIds = (firstBenchmark?.systems ?? []).map((sys) => {
            nameCounts[sys.name] = (nameCounts[sys.name] || 0) + 1;
            const count = nameCounts[sys.name];
            return {
               title: sys.title,
               name: sys.name,
               id: count === 1 ? sys.name : `${sys.name}-${count}`,
            };
         });
         setActiveDbms(dbmsWithIds);

         const loadedQueries = await loadDataset(datasetName);
         setIsConnected(true);

         if (loadedQueries.length > 0 && !query) {
            setQuery(loadedQueries[0].sql);
            setCurrentQueryIndex(0);
         }
      } catch (e: any) {
         console.error('Connection failed:', e);
         const errorMsg = e?.message || 'Failed to connect to server';
         setConnectionError(errorMsg);
         setIsConnected(false);
      }
   }, [hostname, port, loadDataset, query]);

   const handleDisconnect = useCallback(() => {
      setIsConnected(false);
      setSchema('-- Connect to server to load schema');
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

   const handleRun = useCallback(
      async (
         outputId: string,
         dbmsId: string,
         queryOverride?: string
      ): Promise<{ success: boolean; query: string }> => {
         // Look up the actual database name from the ID
         const selectedDbms = activeDbms.find((d) => d.id === dbmsId);
         if (!selectedDbms) {
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          queryResult: {
                             status: 'error',
                             error: 'Please select a target database',
                          },
                       }
                     : out
               )
            );
            return { success: false, query: '' };
         }

         setIsLoading(true);

         // Update the specific output to show loading state in queryResult
         setOutputs((prev) =>
            prev.map((out) =>
               out.id === outputId
                  ? {
                       ...out,
                       error: null,
                       dbms: dbmsId,
                       queryResult: { status: 'running' },
                       queryPlan: null,
                    }
                  : out
            )
         );

         // Get the query to run (prefer override from optimizer result)
         const output = outputs.find((o) => o.id === outputId);
         const baseQuery =
            queryOverride ||
            output?.originalQuery ||
            output?.editedValue ||
            output?.result ||
            '';
         let queryToRun = queryOverride || output?.editedValue || baseQuery || '';

         if (!queryToRun.trim() || queryToRun.startsWith('--')) {
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          queryResult: { status: 'error', error: 'No valid query to run' },
                       }
                     : out
               )
            );
            setIsLoading(false);
            return { success: false, query: queryToRun };
         }

         try {
            const shouldAutoOptimize = Boolean(output?.autoOptimize);
            const normalizedBaseQuery = baseQuery.trim();

            if (shouldAutoOptimize) {
               const canReuseOptimized =
                  Boolean(output?.optimizedQuery) &&
                  (output?.originalQuery || '').trim() === normalizedBaseQuery;

               if (canReuseOptimized) {
                  queryToRun = output?.optimizedQuery || queryToRun;
               } else {
                  const optimizeResponse = await optimize(
                     baseQuery,
                     selectedDbms.name,
                     hostname,
                     port,
                     activeDataset || undefined
                  );

                  if (optimizeResponse.status !== 'success') {
                     throw new Error(optimizeResponse.error || 'Optimization failed');
                  }

                  const optimizedQueryText = optimizeResponse.optimized_query;

                  queryToRun = optimizedQueryText;

                  setOutputs((prev) =>
                     prev.map((out) =>
                        out.id === outputId
                           ? {
                                ...out,
                                result: optimizedQueryText,
                                editedValue: optimizedQueryText,
                                error: null,
                                originalQuery: baseQuery,
                                optimizedQuery: optimizedQueryText,
                             }
                           : out
                     )
                  );
               }
            }

            const response = await runQuery(
               queryToRun,
               selectedDbms.title,
               hostname,
               port,
               parsedTimeout,
               parsedResultLimit,
               activeDataset || undefined
            );

            // Store the query result for display in the result view
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? { ...out, error: null, dbms: dbmsId, queryResult: response }
                     : out
               )
            );

            const success = response.status === 'success';
            if (!success) {
               throw new Error(response.error || 'Query execution failed');
            }
            return { success: true, query: queryToRun };
         } catch (e: any) {
            console.error('Query execution failed:', e);
            const errorMessage =
               e?.message || 'Failed to execute query. Please check your connection and try again.';
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          error: errorMessage,
                          queryResult: { status: 'error', error: errorMessage },
                       }
                     : out
               )
            );
            return { success: false, query: queryToRun };
         } finally {
            setIsLoading(false);
         }
      },
      [outputs, hostname, port, activeDbms, activeDataset, parsedTimeout, parsedResultLimit]
   );

   const fetchPlanForOutput = useCallback(
      async (outputId: string, dbmsId: string, sql: string) => {
         const dbms = activeDbms.find((d) => d.id === dbmsId);
         const trimmedSql = (sql || '').trim();
         if (!dbms || !trimmedSql) return;

         try {
            const planResponse = await getQueryPlan(
               trimmedSql,
               dbms.title,
               hostname,
               port,
               parsedTimeout,
               activeDataset || undefined
            );
            setOutputs((prev) =>
               prev.map((out) => (out.id === outputId ? { ...out, queryPlan: planResponse } : out))
            );
         } catch (e: any) {
            const errorMessage = e?.message || 'Failed to fetch query plan';
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? { ...out, queryPlan: { status: 'error', error: errorMessage } }
                     : out
               )
            );
         }
      },
      [activeDbms, hostname, port, parsedTimeout, activeDataset]
   );

   const handleOptimize = useCallback(
      async (outputId: string, dbmsId: string) => {
         // Look up the actual database name from the ID
         const selectedDbms = activeDbms.find((d) => d.id === dbmsId);
         if (!selectedDbms) {
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          queryResult: {
                             status: 'error',
                             error: 'Please select a target database',
                          },
                       }
                     : out
               )
            );
            return;
         }

         setIsLoading(true);

         // Update the specific output to show loading state in queryResult
         setOutputs((prev) =>
            prev.map((out) =>
               out.id === outputId
                  ? { ...out, error: null, dbms: dbmsId, queryResult: { status: 'running' } }
                  : out
            )
         );

         try {
            const output = outputs.find((o) => o.id === outputId);
            const baseQueryText =
               output?.originalQuery || output?.editedValue || output?.result || query;

            if (!baseQueryText.trim() || baseQueryText.startsWith('--')) {
               throw new Error('No valid query to optimize');
            }

            const response = await optimize(
               baseQueryText,
               selectedDbms.name,
               hostname,
               port,
               activeDataset || undefined
            );

            if (response.status === 'success') {
               const optimizedQueryText = response.optimized_query;
               setOutputs((prev) =>
                  prev.map((out) =>
                     out.id === outputId
                        ? {
                             ...out,
                             result: optimizedQueryText,
                             editedValue: optimizedQueryText,
                             error: null,
                             dbms: dbmsId,
                             originalQuery: baseQueryText,
                             optimizedQuery: optimizedQueryText,
                          }
                        : out
                  )
               );

               const runResponse = await runQuery(
                  optimizedQueryText,
                  selectedDbms.title,
                  hostname,
                  port,
                  parsedTimeout,
                  parsedResultLimit,
                  activeDataset || undefined
               );

               setOutputs((prev) =>
                  prev.map((out) =>
                     out.id === outputId
                        ? { ...out, error: null, dbms: dbmsId, queryResult: runResponse }
                        : out
                  )
               );

               if (runResponse.status !== 'success') {
                  throw new Error(runResponse.error || 'Query execution failed');
               }
            } else {
               throw new Error(response.error || 'Optimization failed');
            }
         } catch (e: any) {
            console.error('Optimization failed:', e);
            const errorMessage =
               e?.message ||
               'Failed to optimize query. Please check your connection and try again.';
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          queryResult: { status: 'error', error: errorMessage },
                       }
                     : out
               )
            );
         } finally {
            setIsLoading(false);
         }
      },
      [query, hostname, port, activeDbms, activeDataset, outputs, parsedTimeout, parsedResultLimit]
   );

   const handleRunAll = useCallback(
      async (queryOverride?: string) => {
         const baseQueryText = queryOverride ?? query;
         const trimmedQuery = baseQueryText.trim();
         if (!trimmedQuery) return;

         // Immediately propagate a new query to all outputs so the run uses the updated text
         if (queryOverride !== undefined) {
            setOutputs((prev) =>
               prev.map((out) => ({
                  ...out,
                  result: resolveQueryForDbms(out.dbms, baseQueryText),
                  editedValue: resolveQueryForDbms(out.dbms, baseQueryText),
                  originalQuery: resolveQueryForDbms(out.dbms, baseQueryText),
                  optimizedQuery: null,
                  queryPlan: null,
               }))
            );
         }

         // Run the query (and then plan) sequentially per output
         for (const output of outputs) {
            if (!output.dbms) continue;
            const overrideForRun =
               queryOverride !== undefined
                  ? resolveQueryForDbms(output.dbms, baseQueryText)
                  : undefined;
            const { success, query: usedQuery } = await handleRun(
               output.id,
               output.dbms,
               overrideForRun
            );
            if (success && output.viewMode === 'plan') {
               await fetchPlanForOutput(output.id, output.dbms, usedQuery);
            }
         }
      },
      [outputs, handleRun, fetchPlanForOutput, query, resolveQueryForDbms]
   );

   const handleAutoRunAll = useCallback(
      async (queryOverride?: string) => {
         const baseQueryText = queryOverride ?? query;
         const trimmedQuery = baseQueryText.trim();
         if (!trimmedQuery) return;

         // Immediately propagate a new query to all outputs so the run uses the updated text
         if (queryOverride !== undefined) {
            setOutputs((prev) =>
               prev.map((out) => ({
                  ...out,
                  result: resolveQueryForDbms(out.dbms, baseQueryText),
                  editedValue: resolveQueryForDbms(out.dbms, baseQueryText),
                  originalQuery: resolveQueryForDbms(out.dbms, baseQueryText),
                  optimizedQuery: null,
                  queryPlan: null,
                  queryResult: out.autoRunEnabled === false ? null : out.queryResult,
               }))
            );
         }

         // Run the query only in outputs where auto-run is enabled
         for (const output of outputs) {
            if (output.dbms && output.autoRunEnabled !== false) {
               const overrideForRun =
                  queryOverride !== undefined
                     ? resolveQueryForDbms(output.dbms, baseQueryText)
                     : undefined;
               const { success, query: usedQuery } = await handleRun(
                  output.id,
                  output.dbms,
                  overrideForRun
               );
               if (success && output.viewMode === 'plan') {
                  await fetchPlanForOutput(output.id, output.dbms, usedQuery);
               }
            }
         }
      },
      [outputs, handleRun, fetchPlanForOutput, query, resolveQueryForDbms]
   );

   const handleAddOutput = useCallback(() => {
      const newId = String(Math.max(...outputs.map((o) => parseInt(o.id))) + 1);

      // Choose the next database in the list (with wrap-around)
      let nextDbms = activeDbms.length > 0 ? activeDbms[0].id : '';
      if (activeDbms.length > 0 && outputs.length > 0) {
         const lastOutput = outputs[outputs.length - 1];
         const lastDbmsIndex = activeDbms.findIndex((db) => db.id === lastOutput.dbms);
         const nextIndex = (lastDbmsIndex + 1) % activeDbms.length;
         nextDbms = activeDbms[nextIndex].id;
      }

      const baseQuery = query || '-- Your optimized query will be displayed here';
      const resolvedQuery = resolveQueryForDbms(nextDbms, baseQuery);

      setOutputs((prev) => [
         ...prev,
         {
            id: newId,
            result: resolvedQuery,
            error: null,
            dbms: nextDbms,
            editedValue: resolvedQuery,
            originalQuery: resolvedQuery,
            optimizedQuery: null,
            viewMode: 'table',
            queryPlan: null,
            autoRunEnabled: true,
            autoOptimize: false,
         },
      ]);
   }, [outputs, query, activeDbms, resolveQueryForDbms]);

   const handleCloseOutput = useCallback((outputId: string) => {
      setOutputs((prev) => prev.filter((out) => out.id !== outputId));
   }, []);

   const handleEditOutput = useCallback((outputId: string, editedValue: string) => {
      setOutputs((prev) =>
         prev.map((out) => {
            if (out.id !== outputId) return out;

            // `OptimizedQueryTab` calls `onEditValue(result)` when `result` changes; treat that as a sync,
            // not a user edit that should invalidate cached optimization.
            if (editedValue === out.result) {
               return { ...out, editedValue };
            }

            return {
               ...out,
               editedValue,
               originalQuery: editedValue,
               optimizedQuery: null,
               queryPlan: null,
            };
         })
      );
   }, []);

   const handleDbmsChange = useCallback(
      (outputId: string, dbms: string) => {
         setOutputs((prev) =>
            prev.map((out) => {
               if (out.id !== outputId) return out;

               const previousResolved = resolveQueryForDbms(out.dbms, query);
               const nextResolved = resolveQueryForDbms(dbms, query);
               const currentBase =
                  out.originalQuery || out.editedValue || out.result || previousResolved;
               const shouldUpdateQuery = currentBase === previousResolved;

               return {
                  ...out,
                  dbms,
                  queryResult: out.autoRunEnabled === false ? null : out.queryResult,
                  queryPlan: out.autoRunEnabled === false ? null : out.queryPlan,
                  optimizedQuery: null,
                  ...(shouldUpdateQuery
                     ? {
                          result: nextResolved,
                          editedValue: nextResolved,
                          originalQuery: nextResolved,
                          queryPlan: null,
                       }
                     : {}),
               };
            })
         );
      },
      [query, resolveQueryForDbms]
   );

   const handleViewModeChange = useCallback((outputId: string, mode: 'table' | 'plan') => {
      setOutputs((prev) =>
         prev.map((out) => (out.id === outputId ? { ...out, viewMode: mode } : out))
      );
   }, []);

   const handlePlanFetched = useCallback((outputId: string, plan: PlanResult | null) => {
      setOutputs((prev) =>
         prev.map((out) => (out.id === outputId ? { ...out, queryPlan: plan } : out))
      );
   }, []);

   const handleRevertOptimizedQuery = useCallback((outputId: string) => {
      setOutputs((prev) =>
         prev.map((out) => {
            if (out.id !== outputId) return out;
            if (!out.optimizedQuery) return out;
            const originalQuery = out.originalQuery || out.editedValue || out.result;
            return {
               ...out,
               result: originalQuery,
               editedValue: originalQuery,
               optimizedQuery: null,
               queryPlan: null,
            };
         })
      );
   }, []);

   const handleToggleAutoRun = useCallback((outputId: string, enabled: boolean) => {
      setOutputs((prev) =>
         prev.map((out) =>
            out.id === outputId
               ? {
                    ...out,
                    autoRunEnabled: enabled,
                    // Preserve the current results when auto-run is disabled so the view stays populated
                    queryResult: out.queryResult,
                    queryPlan: out.queryPlan,
                 }
               : out
         )
      );
   }, []);

   const handleToggleAutoOptimize = useCallback((outputId: string, enabled: boolean) => {
      setOutputs((prev) =>
         prev.map((out) =>
            out.id === outputId
               ? {
                    ...out,
                    autoOptimize: enabled,
                 }
               : out
         )
      );
   }, []);

   const handleHostnameChange = useCallback(
      (newHostname: string) => {
         setHostname(newHostname);
         if (isConnected) {
            handleDisconnect();
         }
      },
      [isConnected, handleDisconnect]
   );

   const handlePortChange = useCallback(
      (newPort: string) => {
         setPort(newPort);
         if (isConnected) {
            handleDisconnect();
         }
      },
      [isConnected, handleDisconnect]
   );

   return (
      <ThemeProvider theme={theme}>
         <CssBaseline />
         <ErrorBoundary>
            <Box
               sx={{
                  height: '100vh',
                  display: 'flex',
                  flexDirection: 'column',
               }}
            >
               <Box sx={{ height: LAYOUT.TITLE_BAR_HEIGHT, borderBottom: LAYOUT.BORDER_STYLE }}>
                  <TitleBar
                     handleConnect={handleConnect}
                     isLoading={isLoading}
                     isConnected={isConnected}
                     hostname={hostname}
                     setHostname={handleHostnameChange}
                     port={port}
                     setPort={handlePortChange}
                     timeout={timeout}
                     setTimeout={setTimeout}
                     resultLimit={resultLimit}
                     setResultLimit={setResultLimit}
                     onAddOutput={handleAddOutput}
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
                                 ? `-- Connection Error:\n-- ${connectionError}\n\n-- Connect to server to load schema`
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
                           onRunAll={handleRunAll}
                           onAutoRunAll={handleAutoRunAll}
                           isLoading={isLoading}
                           currentQueryIndex={currentQueryIndex}
                           setCurrentQueryIndex={setCurrentQueryIndex}
                           autoRunEnabled={outputs.some((out) => out.autoRunEnabled !== false)}
                        />
                     </ResizablePanels>
                     <MultiOutputView
                        outputs={outputs}
                        onRun={handleRun}
                        onOptimize={handleOptimize}
                        onRevertOptimizedQuery={handleRevertOptimizedQuery}
                        onCloseOutput={handleCloseOutput}
                        onEditOutput={handleEditOutput}
                        onDbmsChange={handleDbmsChange}
                        onViewModeChange={handleViewModeChange}
                        onPlanFetched={handlePlanFetched}
                        onToggleAutoRun={handleToggleAutoRun}
                        onToggleAutoOptimize={handleToggleAutoOptimize}
                        isLoading={isLoading}
                        activeDbms={activeDbms}
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
