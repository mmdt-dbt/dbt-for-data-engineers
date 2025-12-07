with source as (
    select * from {{ source('risk_analytics_mm','township_summary_updated') }}

),
cleaned as (
    SELECT
    lower(
    replace(trim(replace(replace(Township_Name, '(', ''), ')', '')), ' ','')
    ) AS township_name,
    Township_Code as township_code,
    Latitude as latitude,
    Longitude as longitude,
    Area_km2 as area_km2
FROM source

)
select * from cleaned