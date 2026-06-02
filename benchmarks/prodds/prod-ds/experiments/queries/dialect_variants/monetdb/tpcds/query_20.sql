with item_sales as (
  select i_item_id
        ,i_item_desc
        ,i_category
        ,i_class
        ,i_current_price
        ,sum(cs_ext_sales_price) as itemrevenue
  from	catalog_sales
      ,item
      ,date_dim
  where cs_item_sk = i_item_sk
    and i_category in ('Men', 'Home', 'Music')
    and cs_sold_date_sk = d_date_sk
  and d_date between cast('1999-03-08' as date)
  				and cast('1999-03-08' as date) + interval '30' day
  group by i_item_id
          ,i_item_desc
          ,i_category
          ,i_class
          ,i_current_price
)
select  i_item_id
       ,i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
       ,itemrevenue
       ,itemrevenue*100.0/sum(itemrevenue) over
           (partition by i_class) as revenueratio
 from item_sales
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
 limit 100
;
