// QueryBrew (c) 2025
import { ActiveDbms, DatasetResponse, Query } from '../Api';
import { OutputResult } from '../types';

export interface QueryResolutionContext {
   queries: Query[];
   activeDbms: ActiveDbms[];
   queryOverrides: Record<string, Record<string, string>>;
}

/** Whether per-DBMS overrides should apply to `baseSql` (i.e. it is an unedited benchmark query). */
function shouldApplyOverridesFor(queries: Query[], baseSql: string, queryName?: string): boolean {
   const targetName = queryName || queries.find((q) => q.sql === baseSql)?.name;
   if (!targetName) return false;
   const matchingQuery = queries.find((q) => q.name === targetName);
   if (!matchingQuery) return false;
   return matchingQuery.sql === baseSql;
}

/**
 * Resolve the SQL to use for a given DBMS, applying a per-DBMS override when `baseSql` is
 * an unedited benchmark query that has one. Falls back to `baseSql` otherwise.
 */
export function resolveQueryForDbms(
   ctx: QueryResolutionContext,
   dbmsId: string,
   baseSql: string,
   queryName?: string
): string {
   const { queries, activeDbms, queryOverrides } = ctx;
   if (!shouldApplyOverridesFor(queries, baseSql, queryName)) {
      return baseSql;
   }

   const dbms = activeDbms.find((d) => d.id === dbmsId);
   if (!dbms) return baseSql;

   const targetName = queryName || queries.find((q) => q.sql === baseSql)?.name;
   if (!targetName) return baseSql;

   const overrides = queryOverrides[targetName];
   if (!overrides) return baseSql;

   return overrides[dbms.name] || overrides[dbms.title] || overrides[dbms.id] || baseSql;
}

/** Shape of a legacy `query_overrides` entry, which may key the DBMS under several names. */
type LegacyOverride = {
   name?: string;
   sql?: string;
   dbms?: string;
   system?: string;
   target_dbms?: string;
   target?: string;
};

/**
 * Build the `{ queryName: { dbmsKey: sql } }` override map from a dataset response,
 * merging per-query override fields with the legacy `query_overrides` array.
 */
export function parseDatasetOverrides(
   datasetResponse: DatasetResponse
): Record<string, Record<string, string>> {
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

   const overridesFromLegacy = ((datasetResponse.query_overrides as LegacyOverride[]) || []).reduce(
      (acc, override) => {
         const dbmsKey =
            override.dbms || override.system || override.target_dbms || override.target;
         if (!override.name || !dbmsKey || !override.sql) {
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

   const merged = { ...overridesFromQueries };
   for (const [name, overrides] of Object.entries(overridesFromLegacy)) {
      merged[name] = { ...(merged[name] || {}), ...overrides };
   }
   return merged;
}

/**
 * Re-derive every output's query text from a new base query, resolving per-DBMS overrides
 * and clearing the cached optimization/plan. When `resetAutoRunResults` is set, results are
 * also cleared for outputs that have auto-run disabled (matching the editor/auto-run flows).
 */
export function propagateQueryToOutputs(
   outputs: OutputResult[],
   resolve: (dbmsId: string, baseSql: string) => string,
   baseText: string,
   resetAutoRunResults: boolean
): OutputResult[] {
   return outputs.map((out) => ({
      ...out,
      result: resolve(out.dbms, baseText),
      editedValue: resolve(out.dbms, baseText),
      originalQuery: resolve(out.dbms, baseText),
      optimizedQuery: null,
      queryPlan: null,
      ...(resetAutoRunResults
         ? { queryResult: out.autoRunEnabled === false ? null : out.queryResult }
         : {}),
   }));
}
