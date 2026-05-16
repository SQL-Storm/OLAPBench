with ssr as
 (select s_store_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns_amt,
        sum(net_loss) as profit_loss,
        max(s_store_name) as store_name
 from
  ( select  ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from store_sales
    union all
    select sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    from store_returns
   ) salesreturns,
     date_dim,
     store
 where date_sk = d_date_sk
       and d_date between cast('1998-08-21' as date)
                  and cast('1998-08-21' as date) + interval '14' day
       and store_sk = s_store_sk
       and s_state in ('CA','WA','GA','TX')
 group by s_store_id)
,
 csr as
 (select cp_catalog_page_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns_amt,
        sum(net_loss) as profit_loss,
        max(cp_description) as catalog_page_desc
 from
  ( select  cs_catalog_page_sk as page_sk,
            cs_sold_date_sk  as date_sk,
            cs_ext_sales_price as sales_price,
            cs_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from catalog_sales
    union all
    select cr_catalog_page_sk as page_sk,
           cr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           cr_return_amount as return_amt,
           cr_net_loss as net_loss
    from catalog_returns
   ) salesreturns,
     date_dim,
     catalog_page
  where date_sk = d_date_sk
        and d_date between cast('1998-08-21' as date)
                   and cast('1998-08-21' as date) + interval '14' day
        and page_sk = cp_catalog_page_sk
        and cp_type is not null
 group by cp_catalog_page_id)
 ,
 wsr as
 (select web_site_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as returns_amt,
        sum(net_loss) as profit_loss,
        max(web_name) as web_site_name
 from
  ( select  ws_web_site_sk as wsr_web_site_sk,
            ws_sold_date_sk  as date_sk,
            ws_ext_sales_price as sales_price,
            ws_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from web_sales
    union all
    select ws_web_site_sk as wsr_web_site_sk,
           wr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           wr_return_amt as return_amt,
           wr_net_loss as net_loss
    from web_returns left outer join web_sales on
         ( wr_item_sk = ws_item_sk
           and wr_order_number = ws_order_number)
   ) salesreturns,
     date_dim,
     web_site
    where date_sk = d_date_sk
          and d_date between cast('1998-08-21' as date)
                     and cast('1998-08-21' as date) + interval '14' day
          and wsr_web_site_sk = web_site_sk
          and web_country = 'United States'
 group by web_site_id) select channel
        , id
        , sum(sales) as sales
        , sum(returns_amt) as returns_amt
        , sum(profit) as profit
        , max(label) as any_channel_label
 from
 (select 'store channel' as channel
        , 'store' || s_store_id as id
        , sales
        , returns_amt
        , (profit - profit_loss) as profit
        , store_name as label
 from   ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || cp_catalog_page_id as id
        , sales
        , returns_amt
        , (profit - profit_loss) as profit
        , catalog_page_desc as label
 from  csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , returns_amt
        , (profit - profit_loss) as profit
        , web_site_name as label
 from   wsr
 ) x
 group by rollup (channel, id)
 order by sales desc
         ,returns_amt desc
         ,profit desc;
