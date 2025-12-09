WITH mm_infrastructure AS (
    SELECT * FROM {{ source('risk_analytics_mm', 'mm_infrastructure') }}
)
SELECT * FROM mm_infrastructure