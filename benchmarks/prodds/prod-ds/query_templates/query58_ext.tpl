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
 define YEAR = random(1998, 2002, uniform); 
 define SALES_DATE=date([YEAR]+"-01-01",[YEAR]+"-07-24",sales); 
 define _LIMIT=100;
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
                                     where d_date = '[SALES_DATE]'))
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
                                      where d_date = '[SALES_DATE]'))
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
                                     where d_date = '[SALES_DATE]'))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id) select ss_items.item_id
      ,ss_item_rev
      ,(select any_value(i_category) from item where i_item_id = ss_items.item_id) as any_item_category
      ,(select max(i_brand) from item where i_item_id = ss_items.item_id) as max_item_brand
      ,(select count(distinct i_product_name) from item where i_item_id = ss_items.item_id) as distinct_product_name_count
      ,max(cast((select max(d_date) from date_dim where d_week_seq = (select d_week_seq from date_dim where d_date = '[SALES_DATE]')) as timestamp)) as max_week_ts
 from ss_items,cs_items,ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id 
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
 group by ss_items.item_id, ss_item_rev
 order by max_week_ts desc
         ,ss_item_rev desc
         ,distinct_product_name_count desc ;
