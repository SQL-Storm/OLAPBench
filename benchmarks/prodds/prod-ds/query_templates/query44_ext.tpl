--
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- 
define NULLCOLSS= text({"ss_customer_sk",1},{"ss_cdemo_sk",1},{"ss_hdemo_sk",1},{"ss_addr_sk",1},{"ss_promo_sk",1});
define STORE=random(1,rowcount("STORE"),uniform);
define _LIMIT=100;
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
       ,any_value(i1.i_brand) as any_best_brand
       ,any_value(i2.i_brand) as any_worst_brand
       ,any_value(i1.i_category) as any_best_category
       ,any_value(i2.i_category) as any_worst_category
       ,max(i1.i_class) as max_best_class
       ,max(i2.i_class) as max_worst_class
from (
     select *
     from (
          select item_sk,rank() over (order by rank_col asc) rnk
          from (
               select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                 from store_sales ss1
                 where ss_store_sk = [STORE]
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0) V1) V11
     where rnk  < 11) asceding,
    (
     select *
     from (
          select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = [STORE]
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
order by asceding.rnk ;
