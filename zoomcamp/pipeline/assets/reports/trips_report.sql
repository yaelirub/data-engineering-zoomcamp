/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: Date the trip started
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow, green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment method
    primary_key: true
  - name: trip_count
    type: integer
    description: Number of trips
    checks:
      - name: not_null
      - name: positive
  - name: total_fare_amount
    type: float
    description: Sum of base fares
    checks:
      - name: not_null
      - name: non_negative
  - name: total_tip_amount
    type: float
    description: Sum of tips
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: Sum of total charges
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: Average trip distance in miles
    checks:
      - name: non_negative
  - name: avg_passenger_count
    type: float
    description: Average number of passengers per trip

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Filter using {{ start_datetime }} / {{ end_datetime }} for incremental runs

SELECT
    pickup_datetime::DATE                   AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown')  AS payment_type_name,
    COUNT(*)                                AS trip_count,
    SUM(fare_amount)                        AS total_fare_amount,
    SUM(tip_amount)                         AS total_tip_amount,
    SUM(total_amount)                       AS total_amount,
    AVG(trip_distance)                      AS avg_trip_distance,
    AVG(passenger_count)                    AS avg_passenger_count
FROM staging.trips
WHERE
    pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    pickup_datetime::DATE,
    taxi_type,
    COALESCE(payment_type_name, 'unknown')
