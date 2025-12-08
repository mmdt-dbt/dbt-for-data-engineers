WITH populationByTownship AS(
    SELECT * FROM {{ source('risk_analytics_mm', 'population') }}
),
Population_Township AS(
    SELECT
    CASE WHEN Township_ID = '' THEN 'NULL' ELSE Township_ID
    END AS Township_ID,
    Township AS Township_Name,
    population AS Population
FROM populationByTownship
)
SELECT * FROM Population_Township