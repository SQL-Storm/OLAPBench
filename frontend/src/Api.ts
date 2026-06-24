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
   result?: unknown[][];
   error?: string;
   query_plan?: object;
}

export interface PlanResponse {
   status: string;
   query_plan?: object;
   error?: string;
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

interface RequestOptions {
   method?: 'GET' | 'POST';
   body?: unknown;
   serviceLabel: string; // Used in the "unable to connect" message
   parseError?: boolean; // Parse `error` from the response body on non-OK status
}

/** A `fetch` rejection (vs. an HTTP error) — i.e. the server could not be reached. */
function isFetchFailure(error: unknown): boolean {
   return error instanceof TypeError && error.message.includes('fetch');
}

/**
 * Build the API base URL. Priority:
 *   1. REACT_APP_API_URL (build-time override)
 *   2. a hostname that already includes a scheme (e.g. "https://host/api") — used verbatim
 *   3. otherwise http://hostname:port
 */
function apiBase(hostname: string, port: string): string {
   if (process.env.REACT_APP_API_URL) return process.env.REACT_APP_API_URL;
   if (/^https?:\/\//i.test(hostname)) return hostname.replace(/\/$/, '');
   return `http://${hostname}:${port}`;
}

/**
 * Shared JSON request helper. Builds the URL, sets headers, surfaces HTTP errors,
 * and maps connection failures to a friendly, service-specific message.
 */
async function request<T>(
   hostname: string,
   port: string,
   path: string,
   { method = 'POST', body, serviceLabel, parseError = false }: RequestOptions
): Promise<T> {
   try {
      const response = await fetch(`${apiBase(hostname, port)}${path}`, {
         method,
         headers: { 'Content-Type': 'application/json' },
         ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      });

      if (!response.ok) {
         if (parseError) {
            const errorData = (await response.json().catch(() => ({}))) as { error?: string };
            throw new Error(errorData.error || `Server responded with status ${response.status}`);
         }
         throw new Error(`Server responded with status ${response.status}`);
      }

      return await response.json();
   } catch (error) {
      if (isFetchFailure(error)) {
         throw new Error(
            `Unable to connect to ${serviceLabel}. Please ensure the server is running.`
         );
      }
      throw error;
   }
}

/**
 * Check server health and get available DBMS instances
 */
export function checkHealth(hostname: string, port: string): Promise<HealthResponse> {
   return request(hostname, port, '/health', { method: 'GET', serviceLabel: 'server' });
}

/**
 * Get dataset schema and queries from server
 */
export function getDataset(
   hostname: string,
   port: string,
   dataset?: string
): Promise<DatasetResponse> {
   return request(hostname, port, '/dataset', {
      body: dataset ? { dataset } : {},
      serviceLabel: 'server',
   });
}

/**
 * Optimize SQL query via API
 */
export function optimize(
   query: string,
   dbms: string,
   hostname: string,
   port: string,
   dataset?: string
): Promise<OptimizeResponse> {
   const body: Record<string, string> = { query, dbms };
   if (dataset) body.dataset = dataset;
   return request(hostname, port, '/optimize', {
      body,
      serviceLabel: 'optimization service',
      parseError: true,
   });
}

/**
 * Execute SQL query via API
 */
export function runQuery(
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
   return request(hostname, port, '/query', {
      body,
      serviceLabel: 'query service',
      parseError: true,
   });
}

/**
 * Get query plan via API
 */
export function getQueryPlan(
   query: string,
   dbms: string,
   hostname: string,
   port: string,
   timeout = 5,
   dataset?: string
): Promise<PlanResponse> {
   const body: Record<string, unknown> = { query, dbms, timeout };
   if (dataset) body.dataset = dataset;
   return request(hostname, port, '/plan', {
      body,
      serviceLabel: 'plan service',
      parseError: true,
   });
}
