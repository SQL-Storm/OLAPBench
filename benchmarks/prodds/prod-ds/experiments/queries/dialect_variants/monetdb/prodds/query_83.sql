with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('1998-06-11','1998-09-01','1998-11-12')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   i_brand is not null
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('1998-06-11','1998-09-01','1998-11-12')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   i_brand is not null
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
		where d_date in ('1998-06-11','1998-09-01','1998-11-12')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
 select sr_items.item_id
       ,sr_item_qty
       ,min((select i_category from item where i_item_id = sr_items.item_id limit 1)) as any_item_category
       ,max((select i_brand from item where i_item_id = sr_items.item_id limit 1)) as max_item_brand
       ,count(distinct (select i_product_name from item where i_item_id = sr_items.item_id limit 1)) as distinct_product_name_count
       ,max(cast((select max(d_date) from date_dim where d_week_seq in (select d_week_seq from date_dim where d_date in ('1998-06-11','1998-09-01','1998-11-12'))) as timestamp)) as max_return_ts
 from sr_items
     ,cr_items
     ,wr_items
 where sr_items.item_id=cr_items.item_id
   and sr_items.item_id=wr_items.item_id 
   and sr_item_qty > 0
 group by sr_items.item_id
        ,sr_item_qty
 order by max_return_ts desc
         ,sr_item_qty desc
         ,sr_items.item_id
limit 50000;
