select substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
       ,any_value(cd1.cd_education_status) as any_education_status
       ,any_value(cd1.cd_marital_status) as any_marital_status
       ,count(distinct ca_state) as distinct_state_count
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2002
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price is not null
    )
   or
    (
     cd1.cd_marital_status = 'D'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'College' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price is not null
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '2 yr Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price is not null
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('VA', 'IL', 'TN')
     and ca_city is not null
     and ws_net_profit > 0
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('WV', 'SD', 'TX')
     and ca_city is not null
     and ws_net_profit > 0
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('CO', 'CA', 'AL')
     and ca_city is not null
     and ws_net_profit > 0 
   )
  )
group by r_reason_desc
order by avg(wr_refunded_cash) desc
        ,avg(wr_fee) desc
        ,avg(ws_quantity) desc
limit 50000;
