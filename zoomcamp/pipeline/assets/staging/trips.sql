/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the trip started
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: When the trip ended
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: TLC zone ID where the trip started
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC zone ID where the trip ended
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Base fare in USD
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: total_amount
    type: float
    description: Total charge including all fees and tips
    checks:
      - name: not_null
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure staging table is not empty
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips
    value: 1

@bruin */

-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (append strategy in ingestion means duplicates can appear on reruns)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)

WITH raw AS (
    SELECT
        pickup_datetime,
        dropoff_datetime,
        taxi_type,
        vendor_id,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id  AS pickup_location_id,
        do_location_id  AS dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        extracted_at
    FROM ingestion.trips
    WHERE
        pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
        AND pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND fare_amount >= 0
        AND total_amount >= 0
        AND trip_distance >= 0
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                fare_amount
            ORDER BY extracted_at DESC
        ) AS rn
    FROM raw
)

SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.taxi_type,
    d.vendor_id,
    d.passenger_count,
    d.trip_distance,
    d.ratecode_id,
    d.store_and_fwd_flag,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.payment_type,
    p.payment_type_name,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.improvement_surcharge,
    d.congestion_surcharge,
    d.airport_fee,
    d.total_amount,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
