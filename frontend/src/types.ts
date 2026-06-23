// QueryBrew (c) 2025
import { QueryResponse, PlanResponse } from './Api';

/**
 * Result panel view modes: tabular results, the indented plan tree, or the
 * top-down plan graph.
 */
export type PlanViewMode = 'table' | 'plan' | 'graph';

/**
 * State for a single optimized-query output panel. Shared between App (owner) and
 * the views that render it (MultiOutputView, OptimizedQueryTab).
 */
export interface OutputResult {
   id: string;
   result: string;
   error: string | null;
   dbms: string;
   editedValue?: string; // User's edited version of the result
   originalQuery?: string; // Unoptimized query text (used for optimization)
   optimizedQuery?: string | null; // Cached optimized query for originalQuery+DBMS
   queryResult?: QueryResponse | null; // Result from running the query
   viewMode?: PlanViewMode;
   queryPlan?: PlanResponse | null;
   autoRunEnabled?: boolean;
   autoOptimize?: boolean;
}
