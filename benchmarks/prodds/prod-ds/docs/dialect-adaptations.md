# Dialect Adaptations

This document describes how Prod-DS handles SQL dialect differences across the
four evaluation engines: DuckDB, PostgreSQL, CedarDB, and MonetDB.

## Overview

TPC-DS query templates use a dialect-abstraction layer via `dsqgen` template
macros (e.g., `__LIMITA`, `__LIMITB`, `__LIMITC`). Prod-DS inherits this
mechanism and extends it with engine-specific query variants to resolve parse
and dialect incompatibilities.

Despite this abstraction, many generated queries require per-engine syntax
adjustments due to differences in SQL parser strictness, function availability,
CTE handling, and type-casting behavior. These adjustments are tracked as
**dialect variants** and are mapped through a centralized configuration file.

## Dialect Template Files

Each supported engine has a dialect template that defines `dsqgen` macro values:

### DuckDB (`query_templates/duckdb.tpl`)

```sql
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = " limit %d";
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM]
             + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM]
           + " using template " + [_TEMPLATE];
```

### PostgreSQL (`query_templates/postgres.tpl`)

```sql
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = " limit %d";
define _BEGIN = "-- start query " + [_QUERY] + " in stream " + [_STREAM]
             + " using template " + [_TEMPLATE];
define _END = "-- end query " + [_QUERY] + " in stream " + [_STREAM]
           + " using template " + [_TEMPLATE];
```

Both DuckDB and PostgreSQL use identical `LIMIT` syntax. The ANSI template
(`query_templates/ansi.tpl`) is also available for engines that require standard
SQL syntax.

## Query Variant Mapping

When a canonical query cannot be parsed or executed by a specific engine, a
variant SQL file is provided. The mapping is maintained in:

- **`experiments/queries/query_mapping.yaml`** -- machine-readable mapping
  configuration.
- **`experiments/queries/dialect_parse_variant_mapping.md`** -- human-readable
  export of the mapping.

### Mapping Structure

For each engine and suite combination, the mapping specifies:

- **Canonical root**: the directory containing the original generated queries
  (e.g., `$QUERY_DIR/prodds/sf100/str10`).
- **Variant root**: the directory containing engine-specific rewrites
  (e.g., `experiments/queries/duckdb/prodds_sf100_str10`).
- **Fix class**: the reason for the variant, classified as `PARSE_OR_DIALECT`.

### Variant Counts by Engine

The following table summarizes the number of queries requiring per-engine dialect
variants (Prod-DS SF100 STR10 and TPC-DS SF100):

| Engine     | Prod-DS SF100 STR10 | TPC-DS SF100 |
|------------|--------------------:|-------------:|
| CedarDB    |                  45 |           76 |
| DuckDB     |                  48 |           84 |
| MonetDB    |                  54 |           70 |
| PostgreSQL |                  48 |           70 |

CedarDB requires the fewest Prod-DS variants but the most TPC-DS variants.
DuckDB requires the most TPC-DS variants due to stricter parsing of certain
legacy SQL constructs.

## Common Dialect Issues

The following categories of syntax differences are addressed by the variant
system:

1. **CTE scoping rules** -- Some engines require CTEs to be explicitly referenced
   or do not support recursive CTE inlining in the same way.

2. **Type-casting syntax** -- Differences in implicit vs. explicit casting
   (e.g., `CAST(x AS INTEGER)` vs. `x::int`).

3. **Window function syntax** -- Variations in `ROWS BETWEEN` frame
   specifications, `QUALIFY` support, and named window definitions.

4. **String function names** -- E.g., `SUBSTR` vs. `SUBSTRING`, `STRPOS` vs.
   `POSITION`.

5. **`ROLLUP`/`GROUPING SETS` support** -- Not all engines support the full
   SQL:1999 grouping-set syntax.

6. **`LIMIT`/`OFFSET` placement** -- While standardized via template macros,
   some complex subquery patterns require engine-specific placement.

7. **Date/time arithmetic** -- Differences in `INTERVAL` syntax and date function
   availability.

## Extended Query Templates

Beyond dialect macros, Prod-DS provides 92 extended query templates
(`query_templates/*_ext.tpl`) that introduce string-oriented operators, adjusted
aggregations, and modified usage patterns. These templates are activated based on
the stringification level (see [stringification-levels.md](stringification-levels.md)).

## How to Add a New Dialect

To add support for a new engine:

1. **Create a dialect template** in `query_templates/<engine>.tpl` defining the
   `dsqgen` macros (`__LIMITA`, `__LIMITB`, `__LIMITC`, `_BEGIN`, `_END`).

2. **Generate canonical queries** using the new dialect template:
   ```bash
   python3 -m workload.dsqgen.generate_queries \
       --dialect <engine> \
       --scale-factor 10 \
       --stringification-level 10
   ```

3. **Identify parse/dialect failures** by running the generated queries against
   the target engine and classifying errors using the protocol error taxonomy
   (`PARSE`, `DIALECT`, `FAILURE`, `OOM`, `UNKNOWN`).

4. **Create variant SQL files** under `experiments/queries/<engine>/<suite>/` for
   each query that requires adaptation. Minimize changes: only modify the specific
   syntax that causes the parse or dialect error.

5. **Register variants** in `experiments/queries/query_mapping.yaml` with the
   appropriate fix class (`PARSE_OR_DIALECT`).

6. **Validate** by re-running the full query set with the mapping active and
   confirming that all previously failing queries now execute successfully.

## Implementation References

- **`query_templates/duckdb.tpl`** -- DuckDB dialect macros.
- **`query_templates/postgres.tpl`** -- PostgreSQL dialect macros (with TPC
  legal notice).
- **`query_templates/ansi.tpl`** -- ANSI SQL dialect macros.
- **`experiments/queries/query_mapping.yaml`** -- centralized variant mapping.
- **`experiments/queries/dialect_parse_variant_mapping.md`** -- human-readable
  mapping export with per-engine, per-suite detail.
