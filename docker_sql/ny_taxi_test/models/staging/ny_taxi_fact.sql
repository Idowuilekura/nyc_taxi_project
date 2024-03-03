{{config(materialized='incremental',alias = "ny_taxi_fact")}}


WITH orig_fact_table AS (
    SELECT
        datetime_id, airport_fee, congestion_surcharge, total_amount, improvement_surcharge, 
    tolls_amount, tip_amount, mta_tax, extra, fare_amount, trip_distance, passenger_count
    FROM
        {{source('ny_taxi_raw','fact_table_taxi_ride')}}
)


SELECT oft.*
FROM orig_fact_table oft
{% if is_incremental() %}
LEFT JOIN {{this}} dt
ON oft.datetime_id = dt.datetime_id
WHERE dt.datetime_id IS NULL
{% endif %}

