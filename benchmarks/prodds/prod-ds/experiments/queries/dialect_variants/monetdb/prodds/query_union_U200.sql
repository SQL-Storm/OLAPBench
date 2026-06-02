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
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 21 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 22 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 23 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 24 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 25 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 26 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 27 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 28 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 29 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 30 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 31 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 32 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 33 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 34 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 35 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 36 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 37 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 38 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 39 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 40 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 41 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 42 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 43 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 44 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 45 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 46 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 47 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 48 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 49 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 50 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 51 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 52 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 53 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 54 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 55 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 56 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 57 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 58 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 59 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 60 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 61 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 62 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 63 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 64 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 65 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 66 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 67 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 68 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 69 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 70 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 71 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 72 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 73 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 74 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 75 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 76 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 77 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 78 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 79 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 80 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 81 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 82 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 83 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 84 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 85 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 86 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 87 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 88 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 89 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 90 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 91 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 92 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 93 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 94 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 95 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 96 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 97 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 98 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 99 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 100 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 101 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 102 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 103 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 104 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 105 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 106 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 107 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 108 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 109 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 110 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 111 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 112 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 113 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 114 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 115 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 116 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 117 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 118 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 119 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 120 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 121 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 122 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 123 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 124 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 125 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 126 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 127 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 128 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 129 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 130 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 131 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 132 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 133 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 134 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 135 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 136 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 137 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 138 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 139 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 140 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 141 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 142 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 143 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 144 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 145 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 146 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 147 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 148 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 149 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 150 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 151 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 152 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 153 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 154 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 155 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 156 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 157 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 158 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 159 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 160 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 161 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 162 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 163 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 164 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 165 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 166 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 167 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 168 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 169 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 170 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 171 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 172 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 173 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 174 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 175 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 176 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 177 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 178 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 179 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 180 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 181 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 182 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 183 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 184 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 185 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 186 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 187 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 188 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 189 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 9
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 190 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 10
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 191 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 11
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 192 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 12
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 193 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 1
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 194 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 2
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 195 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 3
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 196 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 4
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 197 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 5
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 198 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 6
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 199 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 7
UNION ALL
SELECT ss_item_sk, ss_ext_sales_price, d_year, d_moy, s_state, 200 AS union_branch
FROM store_sales, date_dim, store
WHERE ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 1998 AND d_moy = 8;
