with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as yr
       ,sum(ss_net_paid) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
   and d_year in (1998,1999)
   and d_year in (1998,1998+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as yr
       ,sum(ws_net_paid) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
   and d_year in (1998,1999)
   and d_year in (1998,1998+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
         )
 select customer_id,
       customer_first_name,
       customer_last_name
 from year_total
 group by customer_id,
       customer_first_name,
       customer_last_name
 having max(case when sale_type = 's' and yr = 1998 then year_total end) > 0
   and max(case when sale_type = 'w' and yr = 1998 then year_total end) > 0
   and (max(case when sale_type = 'w' and yr = 1999 then year_total end) / nullif(max(case when sale_type = 'w' and yr = 1998 then year_total end), 0)) > (max(case when sale_type = 's' and yr = 1999 then year_total end) / nullif(max(case when sale_type = 's' and yr = 1998 then year_total end), 0))
 order by 2,
       1,
       3
 limit 100
;
