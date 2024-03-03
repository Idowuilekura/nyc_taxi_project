{{config(materialized='table', alias= "trip_per_day", schema="marts")}}
WITH ny_trip_data AS (
    SELECT datetime_id
    FROM {{ ref('ny_taxi_fact') }}
),
dimension_days AS (
    SELECT datetime_id, pickup_day_name 
    FROM {{ ref('time_stamp') }}
),
dimension_ny_trip AS (SELECT dd.pickup_day_name, COUNT(dt.datetime_id) AS trip_count_per_day
FROM ny_trip_data AS dt
INNER JOIN dimension_days AS dd
ON dt.datetime_id = dd.datetime_id
GROUP BY dd.pickup_day_name)


SELECT * 
FROM dimension_ny_trip

-- SELECT * 
-- FROM ny_trip_data
