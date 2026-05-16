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

 define YEAR=random(1999,2002,uniform);
 define AGGONE= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define AGGTWO= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define AGGTHREE= text({"sum",1},{"min",1},{"max",1},{"avg",1},{"stddev_samp",1}); 
 define _LIMIT=100;
select ca_state,
  cd_gender,
  cd_marital_status,
 cd_dep_count,
  count(distinct c_customer_id) as distinct_customer_id_count,
  any_value(c_birth_country) as any_birth_country,
  any_value(ca_city) as any_city_label,
  any_value(c_email_address) as any_email_address,
  max(ca_state) as max_state_label,
  any_value(ca_county) as any_county_label
 from
  customer c,customer_address ca,customer_demographics
where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  ca_country = 'United States' and
  cd_gender in ('F','M') and
  cd_marital_status in ('M','S','D') and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = [YEAR] and
                d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = [YEAR] and
                  d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = [YEAR] and
                  d_quarter_name in ('[YEAR]Q1','[YEAR]Q2','[YEAR]Q3')))
 group by ca_state,
          ca_country,
          ca_county,
          ca_city,
          ca_zip,
          cd_gender,
          cd_marital_status,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by distinct_customer_id_count desc,
          cd_dep_count desc,
          cd_dep_employed_count desc,
          cd_dep_college_count desc ;
 
