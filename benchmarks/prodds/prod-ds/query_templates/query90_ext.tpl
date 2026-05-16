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
 define HOUR_AM = random(6,12,uniform);
 define HOUR_PM = random(13,21,uniform);
 define _LIMIT=5000;
 
[_LIMITA] select [_LIMITB] cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
     ,am_wp_url
     ,am_wp_type
     ,am_wp_access_date_sk
     ,am_buy_potential
     ,am_education_status
from ( select count(*) amc
             ,any_value(web_page.wp_url) as am_wp_url
             ,any_value(web_page.wp_type) as am_wp_type
             ,any_value(web_page.wp_access_date_sk) as am_wp_access_date_sk
             ,any_value(household_demographics.hd_buy_potential) as am_buy_potential
             ,any_value(household_demographics.hd_income_band_sk) as am_education_status
      from web_sales, household_demographics , time_dim, web_page
      where ws_sold_time_sk = time_dim.t_time_sk
        and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
         and web_page.wp_type is not null
         and web_page.wp_char_count > 0) at,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
         and web_page.wp_type is not null
         and web_page.wp_char_count > 0) pt
 order by am_pm_ratio
 [_LIMITC];
