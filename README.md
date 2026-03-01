Data Engineering Zoomcamp 2026 - DLT workshop 

Queries used:
[{"query":"SELECT ROUND(SUM(tip_amt), 2) as total_tips FROM taxi_data.rides","row_count":1},{"query":"SELECT payment_type, COUNT(*) as cnt, ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER(), 2) as pct FROM taxi_data.rides GROUP BY payment_type ORDER BY cnt DESC","row_count":5},{"query":"SELECT MIN(trip_pickup_date_time) as start_date, MAX(trip_pickup_date_time) as end_date FROM taxi_data.rides","row_count":1}]