{{config(materialized='incremental',alias = "ny_taxi_dimension")}}


WITH orig_dimension_table AS (
    SELECT
        *
    FROM
        {{source('ny_taxi_raw','dimension_taxi_table')}}
),

seed_dimension AS (
    SELECT "LocationID" AS locationid, "Borough" AS borough, "Zone" AS zone, service_zone as service_zone
    FROM {{ref("taxi_zone_lookup")}}
),

pickup_dimension AS (
    SELECT odt.datetime_id,sd.locationid AS orig_location_id, sd.borough AS pickup_borough, sd.zone AS pickup_zone, sd.service_zone AS pickup_service_zone, 
    odt.pulocationid AS orig_pick_id 
    FROM 
    orig_dimension_table AS odt
    INNER JOIN seed_dimension AS sd
    ON odt.pulocationid = sd.locationid
),
dropoff_dimension AS (
				SELECT odt.datetime_id,sd.locationid AS orig_location_id, sd.borough AS dropoff_borough, 
	sd.zone AS dropoff_zone, sd.service_zone AS dropoff_service_zone, 
    odt.dolocationid AS orig_pick_id 
    FROM 
    orig_dimension_table AS odt
    INNER JOIN seed_dimension AS sd
    ON odt.dolocationid = sd.locationid
),

dimensions_joined AS (
SELECT odt.datetime_id, odt.ratecodeid, odt.store_and_fwd_flag, pd.pickup_borough, pd.pickup_zone, pd.pickup_service_zone,
dd.dropoff_borough, dd.dropoff_zone, dd.dropoff_service_zone
FROM pickup_dimension AS pd
INNER JOIN dropoff_dimension AS dd
ON pd.datetime_id = dd.datetime_id
INNER JOIN orig_dimension_table AS odt 
ON dd.datetime_id = odt.datetime_id
)

-- SELECT * FROM dimensions_joined
-- {% if is_incremental() %}
-- WHERE datetime_id NOT IN (SELECT (datetime_id) FROM {{this}})
-- {% endif %}
SELECT dj.* 
FROM dimensions_joined AS dj 
{% if is_incremental() %}
LEFT JOIN {{this}} dt
ON dj.datetime_id = dt.datetime_id
WHERE dt.datetime_id IS NULL
{% endif %}


-- SELE
-- FROM pickup_dimension
-- UNION ALL
-- SELECT *
-- FROM dropoff_dimension

-- SELECT * FROM pickup_dimension
-- UNION ALL
-- SELECT * FROM dropoff_dimension

-- pickup_dropoff_combined_orig AS (
--     SELECT odt.datetime_id, odt.ratecodeid, odt.store_and_fwd_flag, pd.pickup_borough, pd.pickup_zone, pd.pickup_service_zone, 
--     dd.dropoff_borough, dd.dropoff_zone, dd.dropoff_service_zone
--     FROM 
--     orig_dimension_table odt
--     INNER JOIN pickup_dimension pd
--     ON odt.pulocationid = pd.orig_pick_id
--     INNER JOIN dropoff_dimension dd
--     ON odt.dolocationid = dd.orig_drop_id)


-- SELECT * FROM pickup_dropoff_combined_orig



