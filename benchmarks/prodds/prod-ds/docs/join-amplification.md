# Appendix A: Join-Graph Amplification

This appendix describes the join-amplification mechanism used by the Prod-DS join
generator to scale join graphs toward production-level sizes without enlarging the
TPC-DS schema.

## Motivation

Production analytical workloads frequently contain queries with tens to hundreds
of joins, driven by denormalization patterns, LOD (Level of Detail) calculations,
and filtered segment replicas. TPC-DS queries, by contrast, rarely exceed 10--15
joins. The Prod-DS join generator bridges this gap by synthesizing queries whose
join counts are parameterized by a syntactic target `J*`.

## Window Function vs. Aggregate-and-Rejoin

A common source of join amplification in production SQL is the rewriting of window
functions into aggregate-and-rejoin CTE patterns. The two formulations below are
semantically equivalent, but the CTE variant forces the optimizer to plan
additional joins:

```sql
-- (a) Window function
SELECT sale_id, category, net,
  SUM(net) OVER (PARTITION BY category)
    AS sum_by_cat
FROM sales;

-- (b) Aggregate then rejoin
WITH base AS (
  SELECT sale_id, category, net
  FROM sales
),
sum_by_cat AS (
  SELECT category, SUM(net) AS sum_by_cat
  FROM base
  GROUP BY category
)
SELECT b.sale_id, b.category, b.net,
       s.sum_by_cat
FROM base b
LEFT JOIN sum_by_cat s          -- rejoin triggers amplification
  ON s.category = b.category;
```

When a query optimizer inlines the CTE, each aggregate-and-rejoin block replicates
the base join block. The `LEFT JOIN` on line 13 of variant (b) marks the rejoin
point that triggers this amplification.

## Growth Model

The join generator uses a two-level growth model.

### Base block with LOD replicas

Starting from a wide base join block of width `b` (the number of dimension-table
joins in the denormalized fact view), adding `k` LOD-style aggregate-and-rejoin
blocks produces:

```
J_0(k) = b + k * (b + 1)
```

Each LOD block re-references the base block (contributing `b` joins) plus one
rejoin (`+1`), and this is repeated `k` times.

### Filtered segment replicas

Purely vertical scaling (increasing `k`) produces deep but narrow join trees.
To force the optimizer to handle wide, multi-branch join shapes, the generator
additionally adds `m` filtered segment replicas that rejoin the base, creating
lateral fan-out:

```
J(k, m) = (m + 1) * J_0(k) + m
```

Each of the `m` segment replicas duplicates the entire LOD-augmented structure
(`J_0(k)` joins) and adds one rejoin to the base (`+1` per replica), while the
original copy contributes `J_0(k)` as well. The final `+m` accounts for the
segment-to-base rejoin of each replica.

### Solver

Given a syntactic join target `J*`, the generator solves for construction
parameters `(k, m)` that minimize `|J(k, m) - J*|`. Ties are broken by
preferring smaller SQL expansion (fewer replicated blocks) and lower `m`.

## Concrete Example on TPC-DS

Consider the `store_sales` fact table denormalized over its ten foreign keys:

| Parameter | Value | Description |
|-----------|------:|-------------|
| `b`       |    10 | Base view width (dimension joins) |
| `k`       |     2 | LOD aggregate-and-rejoin blocks |
| `J_0(2)`  |    32 | `10 + 2 * (10 + 1) = 32` |
| `m`       |     1 | Filtered segment replicas |
| `J(2,1)`  |    65 | `(1 + 1) * 32 + 1 = 65` |

With only 25 tables in the schema, a single generated query produces 65 syntactic
joins. Because each replicated block references different dimension columns for
its predicates and projections, the optimizer cannot recognize copies as identical
subqueries and must plan each independently.

## Implementation Reference

The growth model is implemented in:

- **`workload/dsqgen/generate_join_query.py`** -- contains the functions
  `_base_augmented_join_count()` and `_effective_join_count()` that compute
  `J_0(k)` and `J(k, m)` respectively, as well as the best-fit solver
  `solve_km_for_target_prefer_k()`.

The solver searches all `(k, m)` combinations within configured bounds and
selects the pair that minimizes absolute error to the target, with tie-breaking
that prefers compact SQL output:

```python
def _base_augmented_join_count(b_eff: int, k: int) -> int:
    return b_eff + k * (b_eff + 1)

def _effective_join_count(b_eff: int, k: int, m: int) -> int:
    return (m + 1) * _base_augmented_join_count(b_eff, k) + m
```

Generated queries include a header comment with metadata (target joins, chosen
`k` and `m`, strategy, and expected effective joins) for traceability.
