# Appendix C: Column Recast Justification

This appendix describes the rationale, selection criteria, and categorization of
the 131 columns chosen for stringification in Prod-DS.

## Motivation

Large-scale workload studies consistently report that columns which TPC-DS defines
as integers -- surrogate keys, foreign keys, and coded identifiers -- are stored
as strings in production schemas. Variable-length strings are the dominant column
type in production, commonly encoding surrogate keys (UUIDs, ISBNs) and values
that could be typed as integers or booleans (e.g., "Y"/"N", "0"/"1").

In contrast, TPC-DS defines comparable domains as integers. To close this gap,
Prod-DS introduces a schema-level stringification pass that recasts a curated set
of categorical and identifier-like attributes from integer to `varchar`.

## Candidate Selection

### Scope

We identify every TPC-DS column whose base type is numeric but could plausibly be
a text type in a production schema (e.g., `varchar(32)`). This yields **131
candidates across 24 tables**.

### Inclusion rules

A column is included if it acts as an identifier or categorical code:

- Surrogate keys (`_sk` suffix columns)
- Foreign keys referencing dimension tables
- Classification and categorical codes (`_id` suffix columns for brand, class,
  category, manager, etc.)

### Exclusion rules

The following column types are excluded from recasting:

- **Additive numeric measures** (prices, quantities, profits) -- recasting would
  be semantically incorrect since they serve as aggregation operands.
- **Temporal columns** (`d_year`, `t_hour`, etc.) -- used as range-predicate
  targets and aggregation operands.

## Semantic Categories

The 131 recast candidates are grouped by semantic category:

| Category             | Count | Examples                 |
|----------------------|------:|--------------------------|
| Surrogate key        |    55 | `s_store_sk`             |
| Demographic key      |    38 | `c_customer_sk`          |
| Time key             |    27 | `d_date_sk`              |
| Geographic key       |     7 | `ca_address_sk`          |
| Classification code  |     4 | `i_brand_id`             |
| Categorical code     |     7 | `i_manager_id`           |
| **Total**            |   131 |                          |

## Usage Statistics

Of the 131 recast candidates:

- **66%** appear as equi-join keys across the TPC-DS query templates.
- **74%** appear in filter predicates.
- **74%** appear in `GROUP BY` clauses.

The recasting therefore directly affects the operators most relevant to
string-processing coverage: joins, filters, aggregations, and ordering.

## Selection Order

Candidates enter the recast set in DDL order as the stringification level
increases:

1. **STR 2--4**: Dimension-table primary keys and item classification codes are
   recast first.
2. **STR 5--7**: Demographic and geographic keys are progressively added.
3. **STR 8--10**: The three largest fact tables (`store_sales`, `catalog_sales`,
   `web_sales`) are deferred to these levels so that the I/O impact of
   stringification is concentrated at high levels.

The full per-column list is included in the artifact and can be inspected via the
`explain-stringification` CLI command.

## Implementation Reference

- **`workload/dsdgen/stringify.py`** -- contains the curated column lists,
  custom prefix mappings (e.g., `BRAND_`, `TKT_`, `ORD_`), and domain-suffix
  prefix rules that ensure PK/FK columns sharing the same domain stringify to
  the same textual key space across tables.
- The **`explain-stringification`** CLI subcommand prints the full column
  selection for any given `STR` level, including the recast order, prefix
  assignments, and affected query templates.
