SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 1 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 2 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2;
