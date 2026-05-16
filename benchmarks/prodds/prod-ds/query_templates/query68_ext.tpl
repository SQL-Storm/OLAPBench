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
 define DEPCNT=random(0,9,uniform);
 define YEAR = random(1998,2000,uniform);
 define VEHCNT=random(-1,4,uniform);
 define CITYNUMBER = ulist(random(1, rowcount("active_cities", "store"), uniform), 2);
 define CITY_A = distmember(cities, [CITYNUMBER.1], 1);
 define CITY_B = distmember(cities, [CITYNUMBER.2], 1);
 define _LIMIT=1000;
 
 [_LIMITA] select [_LIMITB] c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,dn.any_buy_potential as any_buy_potential
       ,any_store_name as any_store_name
       ,any_market_desc as any_market_desc
       ,count(distinct current_addr.ca_state) as distinct_current_state_count
       ,min_sold_date as min_sold_date
       ,max_sold_ts as max_sold_ts
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
             ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
             ,any_value(store.s_store_name) as any_store_name
             ,any_value(store.s_market_desc) as any_market_desc
             ,min(d_date) as min_sold_date
             ,max(cast(d_date as timestamp)) as max_sold_ts
       from store_sales
           ,date_dim
           ,store
           ,household_demographics
           ,customer_address 
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
        and store_sales.ss_store_sk = store.s_store_sk  
       and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        and date_dim.d_dom between 1 and 2 
        and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
        and date_dim.d_year in ([YEAR],[YEAR]+1,[YEAR]+2)
        and store.s_market_desc is not null
        and store.s_city in ('[CITY_A]','[CITY_B]')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer
      ,customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
   and current_addr.ca_country = 'United States'
   and current_addr.ca_state in ('CA','WA','GA','TX')
   and current_addr.ca_city is not null
   and c_birth_country in ('United States','Canada')
 group by c_last_name
         ,c_first_name
         ,ca_city
         ,bought_city
         ,ss_ticket_number
         ,extended_price
         ,dn.any_buy_potential
         ,dn.any_store_name
         ,dn.any_market_desc
         ,min_sold_date
         ,max_sold_ts
 order by extended_price desc
         ,max_sold_ts desc
         ,min_sold_date desc
 [_LIMITC];
