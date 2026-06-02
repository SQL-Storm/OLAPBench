select sum(ss_net_profit) as total_sum
   ,s_state
   ,s_county
   ,grouping(s_state)+grouping(s_county) as lochierarchy
  ,rank() over (
	partition by grouping(s_state)+grouping(s_county),
	case when grouping(s_county) = 0 then s_state end 
	order by sum(ss_net_profit) desc) as rank_within_parent
  ,min(s_company_name) as any_company_name
  ,max(s_division_name) as max_division_name
  ,count(distinct s_store_name) as distinct_store_name_count
  ,max(cast(d1.d_date as timestamp)) as max_sold_ts
  ,min(d1.d_date) as min_sold_date
 from
    store_sales
   ,date_dim       d1
   ,store
 where
    d1.d_month_seq between 1186 and 1186+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_market_desc is not null
 and s_state in
             ( select s_state
               from  (select s_state as s_state,
 			    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
                      from   store_sales, store, date_dim
                      where  d_month_seq between 1186 and 1186+11
			    and d_date_sk = ss_sold_date_sk
			    and s_store_sk  = ss_store_sk
                            and s_market_desc is not null
                      group by s_state
                     ) tmp1 
             )
 group by rollup(s_state,s_county)
 order by
  max_sold_ts desc
 ,distinct_store_name_count desc
 ,min_sold_date
limit 50000;
