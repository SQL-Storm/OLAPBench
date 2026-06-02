select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
       ,max(i1.i_brand) as any_best_brand
       ,max(i2.i_brand) as any_worst_brand
       ,max(i1.i_category) as any_best_category
       ,max(i2.i_category) as any_worst_category
       ,max(i1.i_class) as max_best_class
       ,max(i2.i_class) as max_worst_class
from (
     select *
     from (
          select item_sk,rank() over (order by rank_col asc) rnk
          from (
               select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 's00000001'
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0) V1) V11
     where rnk  < 11) asceding,
    (
     select *
     from (
          select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 's00000001'
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0) V2) V21
     where rnk  < 11) descending,
item i1,
item i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
  and i1.i_category in ('Home','Electronics','Sports')
  and i2.i_category in ('Home','Electronics','Sports')
  and i1.i_brand is not null
  and i2.i_brand is not null
group by asceding.rnk, i1.i_product_name, i2.i_product_name
order by asceding.rnk

;
