SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 1 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 2 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 3 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 4 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 5 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 6 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 7 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 8 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 9 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 10 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 11 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 12 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 13 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 14 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 15 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 16 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 17 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 18 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 19 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 20 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8;
