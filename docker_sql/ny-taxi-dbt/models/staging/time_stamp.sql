{{config(materialized='table',alias = "time_stamp_table", schema = "staging")}}




WITH orig_time_stamp_table AS
(
    SELECT * FROM {{ source('ny_taxi_raw','datetime_trip_table') }}
)

SELECT * FROM orig_time_stamp_table
{% if is_incremental() %}
LEFT JOIN {{this}} dt
ON orig_time_stamp_table.datetime_id = dt.datetime_id
WHERE dt.datetime_id IS NULL
{% endif %}
