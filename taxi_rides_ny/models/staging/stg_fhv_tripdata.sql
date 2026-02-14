with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast("Affiliated_base_number" as string) as affiliated_base_number,

        -- standardized location naming
        cast("PUlocationID" as integer) as pickup_location_id,
        cast("DOlocationID" as integer) as dropoff_location_id,

        -- standardized timestamp naming
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast("dropOff_datetime" as timestamp) as dropoff_datetime,

        -- flags
        cast("SR_Flag" as integer) as sr_flag

    from source

    -- data quality requirement
    where dispatching_base_num is not null
)

select * from renamed

{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01'
  and pickup_datetime < '2020-01-01'
{% endif %}