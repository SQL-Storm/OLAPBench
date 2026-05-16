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
 define CITYNUMBER = ulist(random(1, rowcount("active_cities", "store"), uniform),5);
 define CITY_A = distmember(cities, [CITYNUMBER.1], 1);
 define CITY_B = distmember(cities, [CITYNUMBER.2], 1);
 define CITY_C = distmember(cities, [CITYNUMBER.3], 1);
 define CITY_D = distmember(cities, [CITYNUMBER.4], 1);
 define CITY_E = distmember(cities, [CITYNUMBER.5], 1);
 define _LIMIT=100;
select c_last_name
      ,c_first_name
      ,ca_city
      ,bought_city
      ,ss_ticket_number
      ,profit
      ,max_store_name
      ,any_company_name
      ,max_bought_state
      ,distinct_store_city_count
      ,any_buy_potential
      ,min_sold_date
      ,max_sold_ts 
 from
  (select ss_ticket_number
         ,ss_customer_sk
         ,ca_city bought_city
         ,sum(ss_net_profit) profit
         ,max(store.s_store_name) as max_store_name
         ,any_value(store.s_company_name) as any_company_name
         ,max(customer_address.ca_state) as max_bought_state
         ,count(distinct store.s_city) as distinct_store_city_count
         ,any_value(household_demographics.hd_buy_potential) as any_buy_potential
         ,min(date_dim.d_date) as min_sold_date
         ,max(cast(date_dim.d_date as timestamp)) as max_sold_ts
    from store_sales,date_dim,store,household_demographics,customer_address 
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in ([YEAR],[YEAR]+1,[YEAR]+2) 
    and store.s_city in ('[CITY_A]','[CITY_B]','[CITY_C]','[CITY_D]','[CITY_E]') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
      and c_birth_country in ('United States','Canada')
      and current_addr.ca_state in ('CA','TX','NY','WA')
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number ;
