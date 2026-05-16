-- AUTO-GENERATED BASE (SALES)
-- The generator will inject LOD CTEs, joins, and filt copies at the markers.

WITH base AS (
  SELECT
    -- PK/Join keys
    ss.ss_ticket_number,
    ss.ss_item_sk,

    -- Measure
    ss.ss_net_paid,

    -- Date keys (from date_dim)
    d.d_year          AS sold_year,
    d.d_moy           AS sold_moy,
    d.d_dow           AS sold_dow,
    d.d_week_seq      AS sold_week_seq,

    -- Time (from time_dim)
    t.t_hour,
    t.t_am_pm,
    t.t_shift,

    -- Item
    i.i_brand,
    i.i_category,
    i.i_class,
    i.i_manufact,
    i.i_manufact_id,
    i.i_size,
    i.i_color,

    -- Customer + Demographics
    c.c_customer_id AS customer_id,
    cd.cd_education_status,
    cd.cd_credit_rating,
    cd.cd_dep_count,

    -- Household + Income band
    hd.hd_buy_potential,
    hd.hd_dep_count      AS hd_dep_count,
    hd.hd_vehicle_count,
    ib.ib_income_band_sk AS income_band,  -- robust proxy for band

    -- Address
    ca.ca_country     AS cust_country,
    ca.ca_state       AS cust_state,
    ca.ca_city        AS cust_city,
    ca.ca_county      AS cust_county,
    ca.ca_zip         AS cust_zip,

    -- Store
    s.s_store_name    AS store_name,
    s.s_state         AS store_state,
    s.s_company_name  AS store_company_name,
    s.s_division_name AS store_division_name,

    -- Promotion
    p.p_promo_name,
    p.p_channel_email,
    p.p_channel_catalog,
    p.p_channel_tv

  FROM store_sales ss
  LEFT JOIN date_dim               d  ON d.d_date_sk          = ss.ss_sold_date_sk
  LEFT JOIN time_dim               t  ON t.t_time_sk          = ss.ss_sold_time_sk
  LEFT JOIN item                   i  ON i.i_item_sk          = ss.ss_item_sk
  LEFT JOIN customer               c  ON c.c_customer_sk      = ss.ss_customer_sk
  LEFT JOIN customer_demographics  cd ON cd.cd_demo_sk        = c.c_current_cdemo_sk
  LEFT JOIN household_demographics hd ON hd.hd_demo_sk        = c.c_current_hdemo_sk
  LEFT JOIN income_band            ib ON ib.ib_income_band_sk = hd.hd_income_band_sk
  LEFT JOIN customer_address       ca ON ca.ca_address_sk     = c.c_current_addr_sk
  LEFT JOIN store                  s  ON s.s_store_sk         = ss.ss_store_sk
  LEFT JOIN promotion              p  ON p.p_promo_sk         = ss.ss_promo_sk
),

-- [[LOD_CTES_HERE]]

base_and_aggregates AS (
  SELECT
    b.*
    -- [[LOD_SELECT_LIST_HERE]]
  FROM base b
  -- [[LOD_JOINS_HERE]]
)

-- [[FILT_CTES_HERE]]

/* Final */
SELECT b.*
FROM base_and_aggregates b
-- [[FILT_JOINS_HERE]]
;
