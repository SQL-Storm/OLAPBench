with customer_total_return as (select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk) select c_customer_id
      ,max(s_store_name) over () as max_store_name
      ,any_value(c_first_name) over () as any_first_name
      ,any_value(c_last_name) over () as any_last_name
      ,any_value(c_email_address) over () as any_email_address
      ,any_value(c_birth_country) over () as any_birth_country
from customer_total_return ctr1
,(select ctr_store_sk, avg(ctr_total_return)*1.2 as avg_total_return
  from customer_total_return
  group by ctr_store_sk) ctr_avg
,store
,customer
where ctr1.ctr_store_sk = ctr_avg.ctr_store_sk
and ctr1.ctr_total_return > ctr_avg.avg_total_return
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and c_preferred_cust_flag = 'Y'
and c_birth_country is not null
and ctr1.ctr_customer_sk = c_customer_sk
order by ctr_total_return desc
 limit 100