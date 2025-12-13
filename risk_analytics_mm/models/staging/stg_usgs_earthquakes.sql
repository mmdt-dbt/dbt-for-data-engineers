{{
    config(
        materialized = 'incremental'
    )
}}

WITH USGS_Earthquake AS (
    SELECT * FROM {{ source('risk_analytics_mm', 'usgs_earthquakes') }}
)

SELECT id, magnitude, depth_km,
cast(epoch_ms(time) AS TIMESTAMP) + INTERVAL '6 hours 30 minutes' AS mm_timestamp
FROM USGS_Earthquake
{% if is_incremental() %}
where mm_timestamp > (select coalesce(max(mm_timestamp), '1900-01-01') from {{ this }})
{% endif %}