SELECT
    SUM(total_monthly_trips) AS total_trips
  FROM prod.fct_monthly_zone_revenue
  WHERE service_type = 'Green'
    AND revenue_month >= DATE '2019-10-01'
    AND revenue_month < DATE '2019-11-01';