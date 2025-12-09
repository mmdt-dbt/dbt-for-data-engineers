WITH USGS_Earthquake AS (
    SELECT * FROM {{ source('risk_analytics_mm', 'usgs_earthquakes') }}
)

SELECT id,
        magnitude,
        depth_km
FROM USGS_Earthquake