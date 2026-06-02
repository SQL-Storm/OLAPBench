select i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 25 and 25+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-04-09' as date) and (cast('2001-04-09' as date) + interval '60 days')
 and i_manufact_id in ('MFG_00000838','MFG_00000637','MFG_00000831','MFG_00000580')
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
limit 50000;
