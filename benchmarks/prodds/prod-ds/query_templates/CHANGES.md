# Query Template Changes vs. upstream `prod-ds-kit`

This document lists every modification made to the `query_templates/*.tpl`
files in this directory compared to their counterparts in the upstream
[`szlangini/prod-ds-kit`](https://github.com/szlangini/prod-ds-kit)
repository. Changes were necessary because Umbra (the primary engine
targeted for our benchmarking) does not support several SQL features
used by the original templates:

* `GROUP BY ROLLUP (...)` (and grouping sets) **in combination with
  distinct aggregates** such as `count(distinct …)` — plain `ROLLUP`
  on its own is fine, but mixing it with a distinct aggregate in the
  same `SELECT` is not. The fix is to drop one side of the
  combination: in some queries the `ROLLUP` is removed and a flat
  `GROUP BY` is used, in others the distinct aggregate is replaced
  with a non-distinct one.
* `GROUPING(col)` indicator — removed together with the corresponding
  `ROLLUP`.

In addition, several templates contained latent semantic bugs (mixing
aggregates with non-grouped columns, ordering by columns not in the
`SELECT DISTINCT` projection, division by zero, missing CTE columns,
misplaced clauses). Those were fixed at the same time so the queries
return well-defined results across the engines we benchmark.

The list below shows only the `*_ext.tpl` templates that differ from
upstream; every other template is byte-identical.

---

## query9_ext.tpl

**Reason:** syntax fix.

The upstream template placed two additional projection columns
(`any_value(r_reason_desc)`, `count(distinct r_reason_id)`) *after* the
`FROM reason WHERE r_reason_sk = 1` clause, which is not valid SQL —
once the parser sees `FROM`, no further `SELECT`-list items may
follow. The clauses were reordered so the new projection columns
appear at the end of the `SELECT` list (before `FROM`).

```diff
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > [RC.5]
           then (select avg([AGGCTHEN])
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg([AGGCELSE])
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
- from reason
- where r_reason_sk = 1
-        ,any_value(r_reason_desc) as any_reason_desc
-        ,count(distinct r_reason_id) as distinct_reason_id_count
+        ,any_value(r_reason_desc) as any_reason_desc
+        ,count(distinct r_reason_id) as distinct_reason_id_count
+ from reason
+ where r_reason_sk = 1
[_LIMITC];
```

---

## query11_ext.tpl

**Reason:** Umbra rejects mixing aggregate functions with `OVER ()`
against non-grouped columns in the same `SELECT`, and the upstream
template was relying on this non-standard behaviour to compute
"per-result-set" aggregates without an explicit `GROUP BY`.

* The window-aggregate expressions
  `max(...) over ()`, `min(...) over ()`, `any_value(...) over ()`
  were rewritten as plain aggregates (no `OVER` clause).
* An explicit `GROUP BY t_s_secyear.customer_id,
  t_s_secyear.customer_first_name, t_s_secyear.customer_last_name,
  [SELECTONE]` was added, replacing the original
  `ORDER BY t_s_secyear.year_total desc, t_w_secyear.year_total desc,
  t_s_secyear.dyear desc`.
* The ordering columns from the original template are no longer
  computed in the projection (`year_total`, `dyear`) once the query is
  a properly-grouped aggregation, so the `ORDER BY` was dropped.

```diff
  ) select t_s_secyear.customer_id
          ,t_s_secyear.customer_first_name
          ,t_s_secyear.customer_last_name
          ,[SELECTONE]
          ,any_value(t_s_secyear.customer_birth_country) as any_birth_country
          ,any_value(t_s_secyear.customer_email_address) as any_email_address
          ,count(distinct t_s_secyear.customer_login) as distinct_login_count
-         ,max(t_s_secyear.customer_login) over () as max_login
-         ,min(t_s_secyear.customer_birth_country) over () as min_birth_country
-         ,any_value(t_s_secyear.customer_preferred_cust_flag) over () as any_pref_flag
+         ,max(t_s_secyear.customer_login) as max_login
+         ,min(t_s_secyear.customer_birth_country) as min_birth_country
+         ,any_value(t_s_secyear.customer_preferred_cust_flag) as any_pref_flag
  ...
- order by t_s_secyear.year_total desc
-         ,t_w_secyear.year_total desc
-         ,t_s_secyear.dyear desc ;
+ group by t_s_secyear.customer_id
+         ,t_s_secyear.customer_first_name
+         ,t_s_secyear.customer_last_name
+         ,[SELECTONE] ;
```

Note the projection columns shown before the `max(...) over ()` block:
`customer_id`, `customer_first_name`, `customer_last_name`,
`[SELECTONE]` are bare (non-aggregated) columns, while
`any_value(...)`, `count(distinct ...)` are plain aggregates. Mixing
those bare columns with plain aggregates *and* `OVER ()` window
aggregates in the same `SELECT` only works under an implicit-group-by
semantics; turning the `OVER ()` aggregates into plain ones forces the
query to be a proper grouped aggregation, which is why the
matching `GROUP BY` had to be added at the bottom.

---

## query14_ext.tpl

**Reason:** the outer aggregation projects a distinct aggregate
together with the `ROLLUP` grouping, which Umbra rejects. We keep the
distinct aggregate and drop the `ROLLUP`:

```diff
  select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
        ,any_value(i_brand) as any_brand_label
        ,any_value(i_category) as any_category_label
        ,count(distinct i_product_name) as distinct_product_name_count   --  ← distinct aggregate
  from(
        ...
  ) y
- group by rollup (channel, i_brand_id,i_class_id,i_category_id)
+ group by channel, i_brand_id,i_class_id,i_category_id
  order by channel,i_brand_id,i_class_id,i_category_id ;
```

The query now produces only the leaf-level grouping; the hierarchical
subtotals from `ROLLUP` are no longer materialised.

---

## query18_ext.tpl

**Reason:** same as query14 — the `SELECT` mixes two distinct
aggregates with `GROUP BY ROLLUP (…)`, which Umbra rejects. We keep
the distinct aggregates and drop the `ROLLUP`:

```diff
  select i_item_id,
         ca_country,
         ca_state,
         ca_county,
         avg(cast(cs_sales_price as decimal(12,2))) agg4,
         avg(cast(cs_net_profit as decimal(12,2))) agg5,
         any_value(i_item_desc) as any_item_desc,
         any_value(i_category) as any_item_category,
         count(distinct ca_city) as distinct_city_count,           --  ← distinct aggregate
         any_value(i_brand) as any_item_brand,
         max(i_product_name) as max_product_name,
         count(distinct c_email_address) as distinct_email_count,  --  ← distinct aggregate
         ...
         max(cast(d_date as timestamp)) as max_sold_ts
  from catalog_sales, customer_demographics cd1,
       customer_demographics cd2, customer, customer_address, date_dim, item
  where ...
- group by rollup (i_item_id, ca_country, ca_state, ca_county)
+ group by i_item_id, ca_country, ca_state, ca_county
```

---

## query24_ext.tpl

**Reason:** the outer `SELECT` references `any_value(c_birth_country)`,
but the upstream CTE (`ssales`) did not project `c_birth_country`
at all, which makes the query invalid:

```sql
-- outer query (unchanged) — references c_birth_country:
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
      ,any_value(c_birth_country) as any_birth_country   --  ← reads c_birth_country
      ,any_value(i_color) as any_item_color
      ,count(distinct ca_state) as distinct_state_count
from ssales
...
```

The column was added to both occurrences of the `ssales` CTE — in the
`SELECT` list and in the corresponding `GROUP BY` — so that the outer
aggregation has a column to read:

```diff
  with ssales as
  (select c_last_name
        ,c_first_name
+       ,c_birth_country
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size
        ,sum([AMOUNTONE]) netpaid
   from store_sales
       ,store_returns
       ,store
       ,item
       ,customer
       ,customer_address
   where ...
   group by c_last_name
           ,c_first_name
+          ,c_birth_country
           ,s_store_name
           ,ca_state
           ,s_state
           ,i_color
           ,i_current_price
           ,i_manager_id
           ,i_units
           ,i_size)
```

(The same addition is made in both halves of the query, since the CTE
is defined twice.)

---

## query27_ext.tpl

**Reason:** same combination problem as query14 / query18 — the
`SELECT` has two distinct aggregates alongside
`GROUP BY ROLLUP (i_item_id, s_state)`, which Umbra rejects. We keep
the distinct aggregates, drop the `ROLLUP`, and also drop the
`GROUPING(s_state)` indicator column (which would always be `0`
without `ROLLUP` and is therefore meaningless).

```diff
  select i_item_id,
-        s_state, grouping(s_state) g_state,
+        s_state,
         avg(ss_sales_price) agg4,
         any_value(i_item_desc) as any_item_desc,
         any_value(i_category) as any_item_category,
         max(i_brand) as max_item_brand,
         count(distinct i_product_name) as distinct_product_name_count,  --  ← distinct aggregate
         max(s_store_name) as max_store_name,
         count(distinct s_city) as distinct_city_count,                  --  ← distinct aggregate
         min(d_date) as min_sold_date,
         max(cast(d_date as timestamp)) as max_sold_ts
  from store_sales, customer_demographics, date_dim, store, item
  where ...
- group by rollup (i_item_id, s_state)
+ group by i_item_id, s_state
```

---

## query41_ext.tpl

**Reason:** the upstream `ORDER BY i_manufact_id desc` references a
column that is not part of the `SELECT DISTINCT (i_product_name), …`
projection. Engines that strictly enforce the "ORDER BY column must be
in the SELECT list when using `DISTINCT`" rule reject the query.

Switched the sort key to a column that *is* in the projection:

```diff
  select distinct(i_product_name)             --  ← only i_product_name is in the projection
         ,max(i_brand) over () as max_brand
         ,any_value(i_category) over () as any_category
         ,max(i_manufact) over () as max_manufact
         ,min(i_color) over () as min_color
         ,any_value(i_class) over () as any_class
  from item i1
  where ...
- order by i_manufact_id desc ;              --  ← i_manufact_id is not projected
+ order by i_product_name desc ;
```

---

## query44_ext.tpl

**Reason:** the upstream query projects three non-aggregated columns
(`asceding.rnk`, `i1.i_product_name`, `i2.i_product_name`) alongside
several aggregates (`any_value`, `max`) without a `GROUP BY`. Added an
explicit `GROUP BY` over those three columns so the query is a valid
aggregation.

```diff
  select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
                  --  ↑ three bare (non-aggregated) columns
        ,any_value(i1.i_brand) as any_best_brand        --  ← aggregate
        ,any_value(i2.i_brand) as any_worst_brand       --  ← aggregate
        ,any_value(i1.i_category) as any_best_category  --  ← aggregate
        ,any_value(i2.i_category) as any_worst_category --  ← aggregate
        ,max(i1.i_class) as max_best_class              --  ← aggregate
        ,max(i2.i_class) as max_worst_class             --  ← aggregate
  from ( ... ) asceding,
       ( ... ) descending,
       item i1,
       item i2
  where asceding.rnk = descending.rnk
    and i1.i_item_sk=asceding.item_sk
    and i2.i_item_sk=descending.item_sk
    and i1.i_category in ('Home','Electronics','Sports')
    and i2.i_category in ('Home','Electronics','Sports')
    and i1.i_brand is not null
    and i2.i_brand is not null
+ group by asceding.rnk, i1.i_product_name, i2.i_product_name   --  ← matches the three bare columns
  order by asceding.rnk ;
```

---

## query49_ext.tpl

**Reason:** the upstream template computes ratios such as
`sum(coalesce(wr_return_quantity,0)) / sum(coalesce(ws_quantity,0))`.
When the denominator sums to zero, this produces a divide-by-zero
error in strict engines. Changed the denominator's `coalesce` default
from `0` to `1` so a missing/NULL value contributes `1` instead of `0`
and the ratio is well-defined.

```diff
  select ws.ws_item_sk as item
        ,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/      -- numerator: 0 is safe
-         cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
+         cast(sum(coalesce(ws.ws_quantity,1)) as decimal(15,4) )) as return_ratio   -- denom: 1 prevents /0
        ,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/           -- numerator: 0 is safe
-         cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
+         cast(sum(coalesce(ws.ws_net_paid,1)) as decimal(15,4) )) as currency_ratio -- denom: 1 prevents /0
```

The same substitution is applied to the catalog branch
(`cs_quantity`, `cs_net_paid`) and the store branch (`ss_quantity`,
`ss_net_paid`). Numerator `coalesce(...,0)` is left unchanged.

---

## query58_ext.tpl

**Reason:** the upstream template mixes several problematic patterns:

1. `any_value((select i_category from item where ...))` — applying an
   aggregate (`any_value`, `max`, `count(distinct …)`) to a scalar
   subquery is not portable.
2. Filter predicates such as
   `and (select i_category from item where i_item_id = ss_items.item_id) in ('Home','Electronics','Sports')`
   are correlated scalar-subquery filters that are awkward to compile
   on engines that materialise scalar subqueries.
3. The projection has non-aggregated columns
   (`ss_items.item_id`, `ss_item_rev`) together with aggregates
   (`max(...)`) without a `GROUP BY`.

Changes:

* The aggregates were pushed *inside* the scalar subqueries:
  `any_value((select i_category from item …))` becomes
  `(select any_value(i_category) from item …)`, and likewise for
  `max(i_brand)` and `count(distinct i_product_name)`.
* The correlated-subquery filter predicates on `i_category` / `i_brand`
  were dropped.
* An explicit `group by ss_items.item_id, ss_item_rev` was added so
  the remaining `max(...)` is well-defined.

```diff
  ) select ss_items.item_id          --  ← bare column
       ,ss_item_rev                  --  ← bare column
-      ,any_value((select i_category from item where i_item_id = ss_items.item_id)) as any_item_category
-      ,max((select i_brand from item where i_item_id = ss_items.item_id)) as max_item_brand
-      ,count(distinct (select i_product_name from item where i_item_id = ss_items.item_id)) as distinct_product_name_count
+      ,(select any_value(i_category) from item where i_item_id = ss_items.item_id) as any_item_category
+      ,(select max(i_brand) from item where i_item_id = ss_items.item_id) as max_item_brand
+      ,(select count(distinct i_product_name) from item where i_item_id = ss_items.item_id) as distinct_product_name_count
       ,max(cast((select max(d_date) from date_dim where ...) as timestamp)) as max_week_ts   --  ← aggregate
  from ss_items,cs_items,ws_items
  where ss_items.item_id=cs_items.item_id
    and ss_items.item_id=ws_items.item_id
    and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
    and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
-   and (select i_category from item where i_item_id = ss_items.item_id) in ('Home','Electronics','Sports')
-   and (select i_brand from item where i_item_id = ss_items.item_id) is not null
+ group by ss_items.item_id, ss_item_rev    --  ← required because the two bare columns coexist with max(...)
  order by max_week_ts desc
```

---

## query60_ext.tpl

**Reason:** same as query58 — aggregates over scalar subqueries are
rewritten so the aggregate is computed *inside* the subquery.

```diff
  [_LIMITA] select [_LIMITB]
    i_item_id
  ,sum(total_sales) as total_sales
  ,count(*) as item_count
- ,any_value((select i_category from item where i_item_id = tmp1.i_item_id)) as any_item_category
- ,count(distinct (select i_product_name from item where i_item_id = tmp1.i_item_id)) as distinct_product_name_count
+ ,(select any_value(i_category) from item where i_item_id = tmp1.i_item_id) as any_item_category
+ ,(select count(distinct i_product_name) from item where i_item_id = tmp1.i_item_id) as distinct_product_name_count
  ,min((select min(d_date) from date_dim where d_year = [YEAR] and d_moy = [MONTH])) as min_sales_date
  ,max(cast((select max(d_date) from date_dim where d_year = [YEAR] and d_moy = [MONTH]) as timestamp)) as max_sales_ts
  from  ( ... ) tmp1
  group by i_item_id          --  ← already present, no change
```

No filter changes here; the outer `GROUP BY i_item_id` already exists.

---

## query68_ext.tpl

**Reason:** the upstream query mixes aggregates
(`any_value(store.s_store_name)`, `min(d_date)`, `max(cast(d_date as
timestamp))`) with non-aggregated columns (`c_last_name`,
`c_first_name`, `ca_city`, …) in the outer query, without a `GROUP
BY`.

The aggregates were moved into the inner derived table `dn` (where the
group key is already established by the existing `GROUP BY
ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city`), and the outer
query now simply selects those pre-computed columns and adds an
explicit outer `GROUP BY` over the projected non-aggregated columns.

Outer projection — convert outer aggregates into references to the
inner derived-table columns:

```diff
  [_LIMITA] select [_LIMITB] c_last_name        --  ← bare column
        ,c_first_name                           --  ← bare column
        ,ca_city                                --  ← bare column
        ,bought_city                            --  ← bare column
        ,ss_ticket_number                       --  ← bare column
        ,extended_price                         --  ← bare column
        ,dn.any_buy_potential as any_buy_potential
-       ,any_value(store.s_store_name) as any_store_name
-       ,any_value(store.s_market_desc) as any_market_desc
+       ,any_store_name as any_store_name
+       ,any_market_desc as any_market_desc
        ,count(distinct current_addr.ca_state) as distinct_current_state_count
-       ,min(d_date) as min_sold_date
-       ,max(cast(d_date as timestamp)) as max_sold_ts
+       ,min_sold_date as min_sold_date
+       ,max_sold_ts as max_sold_ts
```

The original outer aggregates `any_value(store.s_store_name)`,
`min(d_date)`, etc. cannot coexist with the bare columns above
without an outer `GROUP BY`. They are recomputed inside the inner
derived table `dn` where a `GROUP BY` is already established:

```diff
  from (select ss_ticket_number
              ,ss_customer_sk
              ,ca_city bought_city
              ,sum(ss_ext_sales_price) extended_price
              ,sum(ss_ext_list_price) list_price
              ,sum(ss_ext_tax) extended_tax
              ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
+             ,any_value(store.s_store_name) as any_store_name
+             ,any_value(store.s_market_desc) as any_market_desc
+             ,min(d_date) as min_sold_date
+             ,max(cast(d_date as timestamp)) as max_sold_ts
        from store_sales
            ,date_dim
            ,store
            ,household_demographics
            ,customer_address
        where ...
        group by ss_ticket_number
                ,ss_customer_sk
                ,ss_addr_sk,ca_city) dn      --  ← inner group key
```

Outer `GROUP BY` added to make the surviving outer aggregate
`count(distinct current_addr.ca_state)` valid against the bare outer
projection columns:

```diff
+ group by c_last_name
+         ,c_first_name
+         ,ca_city
+         ,bought_city
+         ,ss_ticket_number
+         ,extended_price
+         ,dn.any_buy_potential
+         ,dn.any_store_name
+         ,dn.any_market_desc
+         ,min_sold_date
+         ,max_sold_ts
  order by extended_price desc
          ,max_sold_ts desc
          ,min_sold_date desc
```

---

## query70_ext.tpl

**Reason:** same combination problem as the queries above — the
projection uses `count(distinct s_store_name)` together with
`GROUP BY ROLLUP (s_state, s_county)`, which Umbra rejects. In this
template the `ROLLUP` is load-bearing (the surrounding query uses
`GROUPING(...)` in the window-function `PARTITION BY` to differentiate
hierarchy levels), so the *other* side of the combination was
removed: `count(distinct s_store_name)` was replaced with
`any_value(s_store_name)`.

```diff
  [_LIMITA] select [_LIMITB]
     sum(ss_net_profit) as total_sum
    ,s_state
    ,s_county
    ,grouping(s_state)+grouping(s_county) as lochierarchy   --  ← GROUPING() needs ROLLUP
   ,rank() over (
        partition by grouping(s_state)+grouping(s_county),  --  ← GROUPING() needs ROLLUP
        case when grouping(s_county) = 0 then s_state end
        order by sum(ss_net_profit) desc) as rank_within_parent
   ,any_value(s_company_name) as any_company_name
   ,max(s_division_name) as max_division_name
-  ,count(distinct s_store_name) as distinct_store_name_count   --  ← incompatible with ROLLUP
+  ,any_value(s_store_name) as any_store_name
   ,max(cast(d1.d_date as timestamp)) as max_sold_ts
   ,min(d1.d_date) as min_sold_date
  ...
  group by rollup(s_state,s_county)                           --  ← ROLLUP kept
  order by
   max_sold_ts desc
- ,distinct_store_name_count desc
+ ,any_store_name desc
  ,min_sold_date
```

```diff
-  ,count(distinct s_store_name) as distinct_store_name_count
+  ,any_value(s_store_name) as any_store_name
...
-  ,distinct_store_name_count desc
+  ,any_store_name desc
```

This trades the exact distinct-count for a representative value but
keeps the query shape and `ORDER BY` semantics intact.

---

## query83_ext.tpl

**Reason:** same shape of issue as query58 / query60:

* Aggregates were applied to scalar subqueries
  (`any_value((select i_category from item …))`, `max((select i_brand
  from item …))`, `count(distinct (select i_product_name from item
  …))`). They were rewritten so the aggregate is computed *inside* the
  subquery.
* The outer projection mixed non-aggregated columns
  (`sr_items.item_id`, `sr_item_qty`) with aggregates
  (`max(cast(…))`) without a `GROUP BY`. Added
  `group by sr_items.item_id, sr_item_qty`.

```diff
  [_LIMITA] select [_LIMITB] sr_items.item_id      --  ← bare column
        ,sr_item_qty                               --  ← bare column
-       ,any_value((select i_category from item where i_item_id = sr_items.item_id)) as any_item_category
-       ,max((select i_brand from item where i_item_id = sr_items.item_id)) as max_item_brand
-       ,count(distinct (select i_product_name from item where i_item_id = sr_items.item_id)) as distinct_product_name_count
+       ,(select any_value(i_category) from item where i_item_id = sr_items.item_id) as any_item_category
+       ,(select max(i_brand) from item where i_item_id = sr_items.item_id) as max_item_brand
+       ,(select count(distinct i_product_name) from item where i_item_id = sr_items.item_id) as distinct_product_name_count
        ,max(cast((select max(d_date) from date_dim where ...) as timestamp)) as max_return_ts   --  ← outer aggregate
  from sr_items
      ,cr_items
      ,wr_items
  where sr_items.item_id=cr_items.item_id
    and sr_items.item_id=wr_items.item_id
    and sr_item_qty > 0
+ group by sr_items.item_id, sr_item_qty   --  ← matches the two bare columns above
  order by max_return_ts desc
          ,sr_item_qty desc
          ,sr_items.item_id
```

---

## Summary by category

| Category | Templates affected |
|---|---|
| `ROLLUP` + distinct-aggregate combination broken up by removing `ROLLUP` (distinct aggregate kept) | `query14_ext`, `query18_ext`, `query27_ext` |
| `ROLLUP` + distinct-aggregate combination broken up by removing the distinct aggregate (`ROLLUP` kept) | `query70_ext` |
| `GROUPING(...)` indicator column removed alongside its `ROLLUP` | `query27_ext` |
| Window-aggregate `OVER ()` replaced with explicit `GROUP BY` | `query11_ext` |
| Missing `GROUP BY` added so aggregates and bare columns co-exist legally | `query44_ext`, `query58_ext`, `query68_ext`, `query83_ext` |
| Missing column added to a CTE so the outer query can reference it | `query24_ext` |
| `agg((select …))` rewritten as `(select agg(…) …)` | `query58_ext`, `query60_ext`, `query83_ext` |
| Removed correlated-scalar-subquery filter predicates | `query58_ext` |
| Aggregates pushed into a derived table | `query68_ext` |
| Divide-by-zero hardening (`coalesce(…,0)` → `coalesce(…,1)` in denominators) | `query49_ext` |
| `ORDER BY` column not in `SELECT DISTINCT` projection — switched to a projected column | `query41_ext` |
| Syntax fix: stray projection columns placed after `FROM`/`WHERE` | `query9_ext` |
