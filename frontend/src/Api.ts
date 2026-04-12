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
}

export interface BenchmarkInfo {
   name: string;
   fullname: string;
   systems: { title: string; name: string }[];
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
