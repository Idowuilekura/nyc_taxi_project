{{config(materialized='incremental',alias = "ny_taxi_fact", schema = "staging")}}


WITH orig_fact_table AS (
    SELECT
        *
    FROM
        {{source('ny_taxi_raw','fact_table_taxi_ride')}}
)


SELECT datetime_id, airport_fee, congestion_surcharge, total_amount, improvement_surcharge, 
tolls_amount, tip_amount, mta_tax, extra, fare_amount, trip_distance, passenger_count

FROM orig_fact_table 
{% if is_incremental() %}
LEFT JOIN {{this}} dt
ON orig_fact_table.datetime_id = dt.datetime_id
WHERE dt.datetime_id IS NULL
{% endif %}

