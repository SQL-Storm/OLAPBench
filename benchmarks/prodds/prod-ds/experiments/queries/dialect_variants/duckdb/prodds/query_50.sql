select s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(30 as text), '[^0-9-]', '', 'g') as bigint)) ) then 1 else 0 end)  as "30 days" 
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(30 as text), '[^0-9-]', '', 'g') as bigint))) and 
                 ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(60 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(60 as text), '[^0-9-]', '', 'g') as bigint))) and 
                 ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(90 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(90 as text), '[^0-9-]', '', 'g') as bigint))) and
                 ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) <= cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when ((cast(regexp_replace(cast(sr_returned_date_sk as text), '[^0-9-]', '', 'g') as bigint) - cast(regexp_replace(cast(ss_sold_date_sk as text), '[^0-9-]', '', 'g') as bigint) > cast(regexp_replace(cast(120 as text), '[^0-9-]', '', 'g') as bigint))) then 1 else 0 end)  as ">120 days" 
from
   store_sales
  ,store_returns
  ,store
  ,date_dim d1
  ,date_dim d2
where
    d2.d_year = 2000
and d2.d_moy  = 9
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
group by
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
order by s_store_name
        ,s_company_id
        ,s_street_number
        ,s_street_name
        ,s_street_type
        ,s_suite_number
        ,s_city
        ,s_county
        ,s_state
        ,s_zip
limit 100;
