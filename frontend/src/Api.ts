// QueryBrew (c) 2025

export interface OptimizeResponse {
   status: string;
   optimized_query: string;
   error?: string;
}

export interface QueryResponse {
   status: string;
   runtime_ms?: number;
   server_time_ms?: number;
   rows?: number;
   columns?: string[];
   result?: any[][];
   error?: string;
   query_plan?: object;
}

export interface ActiveDbms {
   id: string; // Unique identifier (e.g., "duckdb-1", "duckdb-2")
   title: string;
   name: string;
   planner_only?: boolean;
   statistics_status?: string;
   statistics_available?: boolean;
}

export interface BenchmarkInfo {
   name: string;
   fullname: string;
   systems: {
      title: string;
      name: string;
      planner_only?: boolean;
      statistics_status?: string;
      statistics_available?: boolean;
   }[];
   optimizer: string | null;
}

export interface HealthResponse {
   status: string;
   benchmarks: BenchmarkInfo[];
}

export interface Query {
   name: string;
   sql: string;
   // Additional properties may include DB-specific overrides keyed by dbms name/title/id
   [dbms: string]: string;
}

export interface QueryOverride {
   name: string;
   dbms: string;
   sql: string;
}

export interface DatasetResponse {
   status: string;
   benchmark: string;
   schema: string;
   queries: Query[];
   query_overrides?: QueryOverride[]; // Legacy field for backwards compatibility
}

/**
 * Check server health and get available DBMS instances
 */
export async function checkHealth(hostname: string, port: string): Promise<HealthResponse> {
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/health`, {
         method: 'GET',
         headers: {
            'Content-Type': 'application/json',
         },
      });

      if (!response.ok) {
         throw new Error(`Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error('Unable to connect to server. Please ensure the server is running.');
      }
      throw error;
   }
}

/**
 * Get dataset schema and queries from server
 */
export async function getDataset(
   hostname: string,
   port: string,
   dataset?: string
): Promise<DatasetResponse> {
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/dataset`, {
         method: 'POST',
         headers: {
            'Content-Type': 'application/json',
         },
         body: JSON.stringify(dataset ? { dataset } : {}),
      });

      if (!response.ok) {
         throw new Error(`Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error('Unable to connect to server. Please ensure the server is running.');
      }
      throw error;
   }
}

/**
 * Optimize SQL query via API
 */
export async function optimize(
   query: string,
   dbms: string,
   hostname: string,
   port: string,
   dataset?: string
): Promise<OptimizeResponse> {
   const body: Record<string, string> = { query, dbms };
   if (dataset) body.dataset = dataset;
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/optimize`, {
         method: 'POST',
         headers: {
            'Content-Type': 'application/json',
         },
         body: JSON.stringify(body),
      });

      if (!response.ok) {
         const errorData = await response.json().catch(() => ({}));
         throw new Error(errorData.error || `Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error(
            'Unable to connect to optimization service. Please ensure the server is running.'
         );
      }
      throw error;
   }
}

/**
 * Execute SQL query via API
 */
export async function runQuery(
   query: string,
   dbms: string,
   hostname: string,
   port: string,
   timeout = 5,
   resultLimit = 50,
   dataset?: string
): Promise<QueryResponse> {
   const body: Record<string, unknown> = {
      query,
      dbms,
      timeout,
      fetch_result: true,
      fetch_result_limit: resultLimit,
   };
   if (dataset) body.dataset = dataset;
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/query`, {
         method: 'POST',
         headers: {
            'Content-Type': 'application/json',
         },
         body: JSON.stringify(body),
      });

      if (!response.ok) {
         const errorData = await response.json().catch(() => ({}));
         throw new Error(errorData.error || `Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error(
            'Unable to connect to query service. Please ensure the server is running.'
         );
      }
      throw error;
   }
}

export interface PlanResponse {
   status: string;
   query_plan?: object;
   error?: string;
}

export interface PlannerStatisticsResponse {
   status: string;
   target_dbms: string;
   target_dbms_name?: string;
   target_version?: string;
   optimizer?: string | null;
   dialect?: string | null;
   collection_method?: string | null;
   statistics?: string;
   statsql_call?: string | null;
   statsql_query?: string | null;
   statsql_error?: string | null;
   statjson_call?: string | null;
   statjson_error?: string | null;
   cache?: {
      hit?: boolean;
      path?: string;
      fingerprint?: string;
   };
   error?: string;
}

/**
 * Get query plan via API
 */
export async function getQueryPlan(
   query: string,
   dbms: string,
   hostname: string,
   port: string,
   timeout = 5,
   dataset?: string
): Promise<PlanResponse> {
   const body: Record<string, unknown> = { query, dbms, timeout };
   if (dataset) body.dataset = dataset;
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/plan`, {
         method: 'POST',
         headers: {
            'Content-Type': 'application/json',
         },
         body: JSON.stringify(body),
      });

      if (!response.ok) {
         const errorData = await response.json().catch(() => ({}));
         throw new Error(errorData.error || `Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error('Unable to connect to plan service. Please ensure the server is running.');
      }
      throw error;
   }
}

/**
 * Get cached Umbra planner statistics for a target DBMS
 */
export async function getPlannerStatistics(
   targetDbms: string,
   hostname: string,
   port: string,
   dataset?: string
): Promise<PlannerStatisticsResponse> {
   const body: Record<string, string> = { target_dbms: targetDbms };
   if (dataset) body.dataset = dataset;
   const apiUrl = `http://${hostname}:${port}`;

   try {
      const response = await fetch(`${apiUrl}/planner/statistics`, {
         method: 'POST',
         headers: {
            'Content-Type': 'application/json',
         },
         body: JSON.stringify(body),
      });

      if (!response.ok) {
         const errorData = await response.json().catch(() => ({}));
         throw new Error(errorData.error || `Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error: any) {
      if (error.name === 'TypeError' && error.message.includes('fetch')) {
         throw new Error(
            'Unable to connect to planner statistics service. Please ensure the server is running.'
         );
      }
      throw error;
   }
}
