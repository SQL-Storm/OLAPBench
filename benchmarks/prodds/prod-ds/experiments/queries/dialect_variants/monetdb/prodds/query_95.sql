with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select
   count(distinct ws_order_number) as order_count
  ,sum(ws_ext_ship_cost) as total_shipping_cost
  ,max(web_site.web_name) as max_web_name
  ,max(web_site.web_class) as any_web_class
  ,max(ca_city) as any_city
  ,count(distinct ca_city) as distinct_city_count
  ,min(d_date) as min_ship_date
  ,max(cast(d_date as timestamp)) as max_ship_ts
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '1999-3-01' and
           cast('1999-3-01' as date) + interval '60' day
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'OR'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
 limit 50000;
