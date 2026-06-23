// QueryBrew (c) 2025
import { useCallback, useEffect, useState } from 'react';
import { ActiveDbms, PlanResponse, getQueryPlan, optimize, runQuery } from '../Api';
import { OutputResult, PlanViewMode } from '../types';
import { propagateQueryToOutputs } from '../utils/queries';
import { errorMessage } from '../utils/errors';

const DEFAULT_OUTPUT = '-- Your optimized query will be displayed here';

export interface UseOutputsContext {
   hostname: string;
   port: string;
   activeDbms: ActiveDbms[];
   activeDataset: string;
   timeout: number; // parsed timeout in seconds
   resultLimit: number; // parsed row limit
   query: string;
   /** Resolve the SQL for a DBMS, applying per-DBMS overrides for unedited benchmark queries. */
   resolveForDbms: (dbmsId: string, baseSql: string, queryName?: string) => string;
}

/**
 * Owns the optimized-query output panels and all of their behavior: editing, running,
 * optimizing, plan fetching, per-output toggles, and propagating a newly selected query
 * to every panel. Connection/dataset context is supplied via {@link UseOutputsContext}.
 */
export function useOutputs(ctx: UseOutputsContext) {
   const {
      hostname,
      port,
      activeDbms,
      activeDataset,
      timeout,
      resultLimit,
      query,
      resolveForDbms,
   } = ctx;

   const [isLoading, setIsLoading] = useState(false);
   const [outputs, setOutputs] = useState<OutputResult[]>([
      {
         id: '1',
         result: DEFAULT_OUTPUT,
         error: null,
         dbms: '',
         originalQuery: DEFAULT_OUTPUT,
         optimizedQuery: null,
         viewMode: 'table',
         queryPlan: null,
         autoRunEnabled: true,
         autoOptimize: false,
      },
   ]);

   // Update all output views when the selected query changes.
   useEffect(() => {
      if (query) {
         setOutputs((prev) => propagateQueryToOutputs(prev, resolveForDbms, query, true));
      }
   }, [query, resolveForDbms]);

   const run = useCallback(
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
            queryOverride || output?.originalQuery || output?.editedValue || output?.result || '';
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
               timeout,
               resultLimit,
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
         } catch (e) {
            console.error('Query execution failed:', e);
            const message = errorMessage(
               e,
               'Failed to execute query. Please check your connection and try again.'
            );
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          error: message,
                          queryResult: { status: 'error', error: message },
                       }
                     : out
               )
            );
            return { success: false, query: queryToRun };
         } finally {
            setIsLoading(false);
         }
      },
      [outputs, hostname, port, activeDbms, activeDataset, timeout, resultLimit]
   );

   const fetchPlan = useCallback(
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
               timeout,
               activeDataset || undefined
            );
            setOutputs((prev) =>
               prev.map((out) => (out.id === outputId ? { ...out, queryPlan: planResponse } : out))
            );
         } catch (e) {
            const message = errorMessage(e, 'Failed to fetch query plan');
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? { ...out, queryPlan: { status: 'error', error: message } }
                     : out
               )
            );
         }
      },
      [activeDbms, hostname, port, timeout, activeDataset]
   );

   const optimizeOutput = useCallback(
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
                  timeout,
                  resultLimit,
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
         } catch (e) {
            console.error('Optimization failed:', e);
            const message = errorMessage(
               e,
               'Failed to optimize query. Please check your connection and try again.'
            );
            setOutputs((prev) =>
               prev.map((out) =>
                  out.id === outputId
                     ? {
                          ...out,
                          queryResult: { status: 'error', error: message },
                       }
                     : out
               )
            );
         } finally {
            setIsLoading(false);
         }
      },
      [query, hostname, port, activeDbms, activeDataset, outputs, timeout, resultLimit]
   );

   // Run the query (and then plan) sequentially per output. When `autoOnly` is set, only
   // outputs with auto-run enabled run, and their results are reset before running.
   const runAllOutputs = useCallback(
      async (queryOverride: string | undefined, autoOnly: boolean) => {
         const baseQueryText = queryOverride ?? query;
         if (!baseQueryText.trim()) return;

         // Immediately propagate a new query to all outputs so the run uses the updated text
         if (queryOverride !== undefined) {
            setOutputs((prev) =>
               propagateQueryToOutputs(prev, resolveForDbms, baseQueryText, autoOnly)
            );
         }

         for (const output of outputs) {
            if (!output.dbms) continue;
            if (autoOnly && output.autoRunEnabled === false) continue;
            const overrideForRun =
               queryOverride !== undefined ? resolveForDbms(output.dbms, baseQueryText) : undefined;
            const { success, query: usedQuery } = await run(output.id, output.dbms, overrideForRun);
            if (success && (output.viewMode === 'plan' || output.viewMode === 'graph')) {
               await fetchPlan(output.id, output.dbms, usedQuery);
            }
         }
      },
      [outputs, run, fetchPlan, query, resolveForDbms]
   );

   const runAll = useCallback(
      (queryOverride?: string) => runAllOutputs(queryOverride, false),
      [runAllOutputs]
   );
   const autoRunAll = useCallback(
      (queryOverride?: string) => runAllOutputs(queryOverride, true),
      [runAllOutputs]
   );

   const addOutput = useCallback(() => {
      const newId = String(Math.max(...outputs.map((o) => parseInt(o.id))) + 1);

      // Choose the next database in the list (with wrap-around)
      let nextDbms = activeDbms.length > 0 ? activeDbms[0].id : '';
      if (activeDbms.length > 0 && outputs.length > 0) {
         const lastOutput = outputs[outputs.length - 1];
         const lastDbmsIndex = activeDbms.findIndex((db) => db.id === lastOutput.dbms);
         const nextIndex = (lastDbmsIndex + 1) % activeDbms.length;
         nextDbms = activeDbms[nextIndex].id;
      }

      const baseQuery = query || DEFAULT_OUTPUT;
      const resolvedQuery = resolveForDbms(nextDbms, baseQuery);

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
   }, [outputs, query, activeDbms, resolveForDbms]);

   const closeOutput = useCallback((outputId: string) => {
      setOutputs((prev) => prev.filter((out) => out.id !== outputId));
   }, []);

   const editOutput = useCallback((outputId: string, editedValue: string) => {
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

   const dbmsChange = useCallback(
      (outputId: string, dbms: string) => {
         setOutputs((prev) =>
            prev.map((out) => {
               if (out.id !== outputId) return out;

               const previousResolved = resolveForDbms(out.dbms, query);
               const nextResolved = resolveForDbms(dbms, query);
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
      [query, resolveForDbms]
   );

   const viewModeChange = useCallback((outputId: string, mode: PlanViewMode) => {
      setOutputs((prev) =>
         prev.map((out) => (out.id === outputId ? { ...out, viewMode: mode } : out))
      );
   }, []);

   const planFetched = useCallback((outputId: string, plan: PlanResponse | null) => {
      setOutputs((prev) =>
         prev.map((out) => (out.id === outputId ? { ...out, queryPlan: plan } : out))
      );
   }, []);

   const revertOptimizedQuery = useCallback((outputId: string) => {
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

   const toggleAutoRun = useCallback((outputId: string, enabled: boolean) => {
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

   const toggleAutoOptimize = useCallback((outputId: string, enabled: boolean) => {
      setOutputs((prev) =>
         prev.map((out) => (out.id === outputId ? { ...out, autoOptimize: enabled } : out))
      );
   }, []);

   return {
      outputs,
      isLoading,
      run,
      optimize: optimizeOutput,
      runAll,
      autoRunAll,
      addOutput,
      closeOutput,
      editOutput,
      dbmsChange,
      viewModeChange,
      planFetched,
      revertOptimizedQuery,
      toggleAutoRun,
      toggleAutoOptimize,
   };
}
