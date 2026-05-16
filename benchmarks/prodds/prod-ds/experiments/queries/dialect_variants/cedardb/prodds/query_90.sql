select cast(amc as decimal(15,4))/nullif(cast(pmc as decimal(15,4)), 0) am_pm_ratio
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
         and web_page.wp_char_count > 0) am_tbl,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and household_demographics.hd_buy_potential in ('1001-5000','501-1000','>10000')
         and web_page.wp_type is not null
         and web_page.wp_char_count > 0) pt
 order by am_pm_ratio
limit 50000;
