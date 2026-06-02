select substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
  ,sum(case when ((cast(regexp_replace(cast(ws_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ws_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(30 as text), '[^0-9-]', '', 'g') as bigint)) ) then 1 else 0 end)  as "30 days" 
  ,sum(case when ((cast(regexp_replace(cast(ws_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ws_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(90 as text), '[^0-9-]', '', 'g') as bigint))) and
                 ((cast(regexp_replace(cast(ws_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ws_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when ((cast(regexp_replace(cast(ws_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ws_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as ">120 days" 
  ,any_value(w_warehouse_id) as any_warehouse_id
  ,any_value(web_class) as any_web_class
  ,any_value(sm_carrier) as any_ship_carrier
  ,min(d_date) as min_ship_date
  ,max(cast(d_date as timestamp)) as max_ship_ts
from
   web_sales
  ,warehouse
  ,ship_mode
  ,web_site
  ,date_dim
where
    d_month_seq between 1178 and 1178 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
and sm_type is not null
and sm_carrier is not null
and web_name is not null
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
order by "30 days" desc
        ,"91-120 days" desc
       ,">120 days" desc
       ,max_ship_ts desc
limit 50000;
