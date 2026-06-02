select w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
	,sum(jan_sales) as jan_sales
        ,w_country as any_country_label
        ,ship_carriers as max_ship_carriers
        ,year as min_year_label
        ,year as max_year_label
 from (
     select 
 	w_warehouse_name
 	,w_warehouse_id
 	,w_warehouse_sk
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,w_zip
 	,'RUPEKSA' || ',' || 'USPS' as ship_carriers
       ,d_year as year
 	,sum(case when d_moy = 1 
 		then ws_ext_list_price* ws_quantity else 0 end) as jan_sales
 	,sum(case when d_moy = 2 
 		then ws_ext_list_price* ws_quantity else 0 end) as feb_sales
 	,sum(case when d_moy = 3 
 		then ws_ext_list_price* ws_quantity else 0 end) as mar_sales
 	,sum(case when d_moy = 4 
 		then ws_ext_list_price* ws_quantity else 0 end) as apr_sales
 	,sum(case when d_moy = 5 
 		then ws_ext_list_price* ws_quantity else 0 end) as may_sales
 	,sum(case when d_moy = 6 
 		then ws_ext_list_price* ws_quantity else 0 end) as jun_sales
 	,sum(case when d_moy = 7 
 		then ws_ext_list_price* ws_quantity else 0 end) as jul_sales
 	,sum(case when d_moy = 8 
 		then ws_ext_list_price* ws_quantity else 0 end) as aug_sales
 	,sum(case when d_moy = 9 
 		then ws_ext_list_price* ws_quantity else 0 end) as sep_sales
 	,sum(case when d_moy = 10 
 		then ws_ext_list_price* ws_quantity else 0 end) as oct_sales
 	,sum(case when d_moy = 11
 		then ws_ext_list_price* ws_quantity else 0 end) as nov_sales
 	,sum(case when d_moy = 12
 		then ws_ext_list_price* ws_quantity else 0 end) as dec_sales
	,sum(case when d_moy = 1 
		then ws_net_profit * ws_quantity else 0 end) as jan_net
    from
         web_sales
        ,warehouse
        ,date_dim
        ,time_dim
 	  ,ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
 	and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2002
 	and sm_carrier in ('RUPEKSA','USPS')
        and w_country in ('United States','Canada')
        and w_city is not null
     group by 
        w_warehouse_name
 	,w_warehouse_id
 	,w_warehouse_sk
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,w_zip
       ,d_year
 union all
     select 
 	w_warehouse_name
 	,w_warehouse_id
 	,w_warehouse_sk
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,w_zip
 	,'RUPEKSA' || ',' || 'USPS' as ship_carriers
       ,d_year as year
	,sum(case when d_moy = 1 
		then cs_ext_sales_price* cs_quantity else 0 end) as jan_sales
	,sum(case when d_moy = 2 
		then cs_ext_sales_price* cs_quantity else 0 end) as feb_sales
	,sum(case when d_moy = 3 
		then cs_ext_sales_price* cs_quantity else 0 end) as mar_sales
	,sum(case when d_moy = 4 
		then cs_ext_sales_price* cs_quantity else 0 end) as apr_sales
	,sum(case when d_moy = 5 
		then cs_ext_sales_price* cs_quantity else 0 end) as may_sales
	,sum(case when d_moy = 6 
		then cs_ext_sales_price* cs_quantity else 0 end) as jun_sales
	,sum(case when d_moy = 7 
		then cs_ext_sales_price* cs_quantity else 0 end) as jul_sales
	,sum(case when d_moy = 8 
		then cs_ext_sales_price* cs_quantity else 0 end) as aug_sales
	,sum(case when d_moy = 9 
		then cs_ext_sales_price* cs_quantity else 0 end) as sep_sales
	,sum(case when d_moy = 10 
		then cs_ext_sales_price* cs_quantity else 0 end) as oct_sales
	,sum(case when d_moy = 11 
		then cs_ext_sales_price* cs_quantity else 0 end) as nov_sales
	,sum(case when d_moy = 12 
		then cs_ext_sales_price* cs_quantity else 0 end) as dec_sales
	,sum(case when d_moy = 1 
		then cs_net_profit * cs_quantity else 0 end) as jan_net
    from
         catalog_sales
        ,warehouse
        ,date_dim
        ,time_dim
 	 ,ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
 	and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2002
 	and sm_carrier in ('RUPEKSA','USPS')
        and w_country in ('United States','Canada')
        and w_city is not null
     group by 
        w_warehouse_name
 	,w_warehouse_id
 	,w_warehouse_sk
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,w_zip
       ,d_year
 ) x
 group by 
        w_warehouse_name
 	,w_warehouse_id
 	,w_warehouse_sk
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
 	,w_zip
 	,ship_carriers
       ,year
 order by w_warehouse_name
limit 50000;
