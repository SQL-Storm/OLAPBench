with ss as
 (select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit,
         max(s_store_name) as store_name
 from store_sales,
      date_dim,
      store
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date) 
                  and (cast('1998-08-26' as date) + interval '30 days') 
       and ss_store_sk = s_store_sk
       and s_state in ('CA','WA','GA','TX')
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         sum(sr_return_amt) as returns_amt,
         sum(sr_net_loss) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date)
                  and (cast('1998-08-26' as date) + interval '30 days')
       and sr_store_sk = s_store_sk
       and s_state in ('CA','WA','GA','TX')
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit,
        max(cc_name) as call_center_name
 from catalog_sales,
      date_dim,
      call_center
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date)
                  and (cast('1998-08-26' as date) + interval '30 days')
       and cs_call_center_sk = cc_call_center_sk
       and cc_country = 'United States'
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         sum(cr_return_amount) as returns_amt,
         sum(cr_net_loss) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date)
                  and (cast('1998-08-26' as date) + interval '30 days')
 group by cr_call_center_sk
 ), 
 ws as
 ( select wp_web_page_sk,
        sum(ws_ext_sales_price) as sales,
        sum(ws_net_profit) as profit,
        max(wp_url) as page_url
 from web_sales,
      date_dim,
      web_page
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date)
                  and (cast('1998-08-26' as date) + interval '30 days')
       and ws_web_page_sk = wp_web_page_sk
       and wp_type is not null
 group by wp_web_page_sk), 
 wr as
 (select wp_web_page_sk,
        sum(wr_return_amt) as returns_amt,
        sum(wr_net_loss) as profit_loss
 from web_returns,
      date_dim,
      web_page
 where wr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-26' as date)
                  and (cast('1998-08-26' as date) + interval '30 days')
       and wr_web_page_sk = wp_web_page_sk
       and wp_type is not null
 group by wp_web_page_sk)
  select channel
        , id
        , sum(sales) as sales
        , sum(returns_amt) as returns_amt
        , sum(profit) as profit
        , any_value(label) as any_channel_label
 from 
 (select 'store channel' as channel
        , ss.s_store_sk as id
        , sales
        , coalesce(returns_amt, 0) as returns_amt
        , (profit - coalesce(profit_loss,0)) as profit
        , store_name as label
 from   ss left join sr
        on  ss.s_store_sk = sr.s_store_sk
 union all
 select 'catalog channel' as channel
        , cs_call_center_sk as id
        , sales
        , returns_amt
        , (profit - profit_loss) as profit
        , call_center_name as label
 from  cs
       , cr
 union all
 select 'web channel' as channel
        , ws.wp_web_page_sk as id
        , sales
        , coalesce(returns_amt, 0) returns_amt
        , (profit - coalesce(profit_loss,0)) as profit
        , page_url as label
 from   ws left join wr
        on  ws.wp_web_page_sk = wr.wp_web_page_sk
 ) x
 group by rollup (channel, id)
 order by sales desc
         ,returns_amt desc
         ,profit desc
limit 50000;
