# Join Generator Fix (Canonical Path)

## Problem

Historically, the join generator used a CTE-sharing shape plus an empirical join model (`2*b + k + m`), which capped effective complexity and produced label-vs-effective mismatch.

## Canonical Fix

- `workload/dsqgen/generate_join_query.py` now follows the paper model:
  - `J0(k) = b + k*(b+1)`
  - `J(k,m) = (m+1)*J0(k) + m`
- Generator output now inlines base blocks into LOD/segment branches (no shared `base` CTE reuse for these branches).
- Calibration source-of-truth remains `workload/config/returns.yml::target_overrides` for canonical levels.

## Regenerate Canonical Queries

```bash
python3 tools/regenerate_join_scaling_queries.py \
  --levels 1,2,4,8,16,32,64,128,256,512,1024,2048
```

This writes:

- `$QUERY_DIR/prodds/generators/join/join_<J>.sql`

and validates each file header:

- `STRATEGY=target_override`
- `chosen k=<...> m=<...>` matches `target_overrides`

## Guardrails

- `wrap_dsqgen.py` now fails if canonical scaling targets are generated without `STRATEGY=target_override`.
- `tools/run_e3_join_scaling.py` and `tools/run_e7_deep_dive.py` now fail fast when `join_<J>.sql` headers do not match `target_overrides`.
- `tools/validate_join_counts.py` can be used for closed-loop EXPLAIN-based validation.

This prevents silent drift between calibration outputs and timed experiments.
