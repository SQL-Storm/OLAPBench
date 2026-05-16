with ss_items as
 (select i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev
 from store_sales
     ,item
     ,date_dim
where ss_item_sk = i_item_sk
 and i_brand is not null
 and i_category in ('Books','Electronics','Home')
  and d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq
                                      from date_dim
                                     where d_date = '2000-02-12'))
   and ss_sold_date_sk   = d_date_sk
 group by i_item_id),
 cs_items as
 (select i_item_id item_id
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      ,item
      ,date_dim
where cs_item_sk = i_item_sk
  and i_brand is not null
  and i_category in ('Books','Electronics','Home')
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq
                                      from date_dim
                                      where d_date = '2000-02-12'))
  and  cs_sold_date_sk = d_date_sk
 group by i_item_id),
 ws_items as
 (select i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
where ws_item_sk = i_item_sk
  and i_brand is not null
  and i_category in ('Books','Electronics','Home')
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq =(select d_week_seq
                                     from date_dim
                                     where d_date = '2000-02-12'))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id) select ss_items.item_id
      ,ss_item_rev
      ,max((select i_category from item where i_item_id = ss_items.item_id)) as any_item_category
      ,max((select i_brand from item where i_item_id = ss_items.item_id)) as max_item_brand
      ,count(distinct (select i_product_name from item where i_item_id = ss_items.item_id)) as distinct_product_name_count
      ,max(cast((select max(d_date) from date_dim where d_week_seq = (select d_week_seq from date_dim where d_date = '2000-02-12')) as timestamp)) as max_week_ts
 from ss_items,cs_items,ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and (select i_category from item where i_item_id = ss_items.item_id) in ('Home','Electronics','Sports')
   and (select i_brand from item where i_item_id = ss_items.item_id) is not null
 group by ss_items.item_id, ss_item_rev
 order by max_week_ts desc
         ,ss_item_rev desc
         ,distinct_product_name_count desc

;
