ELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) AS total_revenue_2020
  FROM prod.fct_monthly_zone_revenue
  WHERE service_type = 'Green'
    AND revenue_month >= DATE '2020-01-01'
    AND revenue_month < DATE '2021-01-01'
  GROUP BY pickup_zone
  ORDER BY total_revenue_2020 DESC
  LIMIT 1;