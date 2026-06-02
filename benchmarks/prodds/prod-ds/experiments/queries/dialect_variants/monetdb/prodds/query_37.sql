select i_item_id
      ,i_item_desc
      ,i_current_price
      ,avg(inv_quantity_on_hand) as avg_inv_qty
      ,max(i_category) as any_item_category
      ,max(i_brand) as max_item_brand
      ,count(distinct i_product_name) as distinct_product_name_count
      ,max(i_class) as any_item_class
      ,min(d_date) as min_inv_date
      ,max(cast(d_date as timestamp)) as max_inv_ts
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 34 and 34 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date >= cast('1998-01-07' as date) and d_date <= cast('1998-01-07' as date) + interval '60' day
 and i_manufact_id in ('MFG_00000896','MFG_00000906','MFG_00000767','MFG_00000821')
 and i_category in ('Books','Electronics','Sports')
 and i_class is not null
 and i_color is not null
 and i_units in ('Each','Box','Case')
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by avg_inv_qty desc
         ,max_inv_ts desc
         ,min_inv_date desc;
