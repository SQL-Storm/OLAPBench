select substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(30 as text), '[^0-9-]', '', 'g') as bigint)) ) then 1 else 0 end)  as "30 days" 
  ,sum(case when ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(30 as text), '[^0-9-]', '', 'g') as bigint))) and 
                 ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(60 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(60 as text), '[^0-9-]', '', 'g') as bigint))) and 
                 ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(90 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(90 as text), '[^0-9-]', '', 'g') as bigint))) and
                 ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when ((cast(regexp_replace(cast(cs_ship_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(cs_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as ">120 days" 
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between 1211 and 1211 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
limit 50000;
