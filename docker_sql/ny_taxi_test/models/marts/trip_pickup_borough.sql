{{config(materialized='table', alias= "trip_pickup_borough", schema="marts")}}

WITH ny_trip_data AS (
    SELECT datetime_id
    FROM {{ ref('ny_taxi_fact') }}
),

dimension_ny_trip AS (

    SELECT datetime_id, pickup_borough
    FROM {{ ref('ny_taxi_dimension_test') }}
)


SELECT dnt.pickup_borough, COUNT(ntd.datetime_id) AS trip_count_per_borough
FROM ny_trip_data AS ntd
INNER JOIN dimension_ny_trip AS dnt
ON ntd.datetime_id = dnt.datetime_id
GROUP BY dnt.pickup_borough