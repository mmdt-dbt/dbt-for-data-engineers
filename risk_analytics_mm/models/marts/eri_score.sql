------calculate ERI score------
with eri_calculation AS (
select a.township_code, a.exposure_score, b.vulnerability_score, c.Hazard_score, 
COALESCE(0.4 * c.Hazard_score, 0) + COALESCE(0.4 * a.exposure_score, 0) + COALESCE(0.2 * b.vulnerability_score, 0) as eri
from {{ ref('exposure') }} a
left join {{ ref('vulnerability') }} b
on a.township_code = b.township_code
left join {{ ref('hazard') }} c
on a.township_code = c.township_code
),
normalized_eri_score as (
select township_code , ROUND((COALESCE((eri - MIN(eri) OVER ()) /NULLIF(MAX(eri) OVER () - MIN(eri) OVER (), 0),0)) * 100,2) AS eri_score
  from eri_calculation
),
eri_lable as(
select *,
    CASE
        WHEN eri_score >= 80 THEN 'Extreme'
        WHEN eri_score >= 60 THEN 'High'
        WHEN eri_score >= 40 THEN 'Moderate'
        WHEN eri_score >= 20 THEN 'Low'
        ELSE 'Minimal'
    END AS risk_level
FROM normalized_eri_score
)
select a.township_code, b.township_name, a.eri_score, a.risk_level
from eri_lable a
left join {{ ref('stg_township_summary') }} b
on a.township_code = b.township_code
