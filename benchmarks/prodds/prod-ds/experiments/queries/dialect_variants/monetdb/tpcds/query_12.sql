with item_sales as (
  select i_item_id
        ,i_item_desc
        ,i_category
        ,i_class
        ,i_current_price
        ,sum(ws_ext_sales_price) as itemrevenue
  from
	web_sales
    	,item
    	,date_dim
  where
	ws_item_sk = i_item_sk
  	and i_category in ('Women', 'Children', 'Books')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('2001-02-28' as date)
				and cast('2001-02-28' as date) + interval '30' day
  group by
	i_item_id
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
order by
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
 limit 100
;
