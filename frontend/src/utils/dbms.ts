// QueryBrew (c) 2025
import { ActiveDbms } from '../Api';

/**
 * Build the active-DBMS list from a benchmark's systems, assigning each a unique `id`.
 * Duplicate system names are disambiguated with a numeric suffix (e.g. `duckdb`,
 * `duckdb-2`, `duckdb-3`).
 */
export function buildDbmsWithIds(systems: { title: string; name: string }[] = []): ActiveDbms[] {
   const nameCounts: Record<string, number> = {};
   return systems.map((sys) => {
      nameCounts[sys.name] = (nameCounts[sys.name] || 0) + 1;
      const count = nameCounts[sys.name];
      return {
         title: sys.title,
         name: sys.name,
         id: count === 1 ? sys.name : `${sys.name}-${count}`,
      };
   });
}
