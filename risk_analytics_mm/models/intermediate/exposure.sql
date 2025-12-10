<<<<<<< HEAD
----infrastructure_score---
WITH infra_township_info AS (
select a.id, a.amenity, b.township_code
from 
(select id, amenity from  {{ ref('stg_mm_infrastructure') }}
where amenity in ('school','university','hospital','clinic')
) a
left join {{ ref('infra_with_township') }} b
on a.id = b.infra_id
),
infracount_per_township AS (
select township_code, count(*) as infra_count
from infra_township_info
group by township_code 
),
normalize_infra_score AS (
select township_code,
COALESCE(
(infra_count - MIN(infra_count) OVER ()) / 
NULLIF(MAX(infra_count) OVER () - MIN(infra_count) OVER (), 0),
0) AS infra_score
    from
        infracount_per_township
        where township_code is not null
),
-----population_score-----
population AS (
select Township_ID, max(Population) as population
from {{ ref('stg_population') }}
group by Township_ID
),
normalize_pop_score AS (
select Township_ID as township_code,
COALESCE(
  (population - MIN(population) OVER ()) /
  NULLIF(MAX(population) OVER () - MIN(population) OVER (), 0),
  0) AS pop_score
  from population
  where Township_ID <> 'NULL'
)
---calculate exposure---
select a.township_code, a.pop_score , b.infra_score,
COALESCE(0.6 * a.pop_score, 0) + COALESCE(0.4 * b.infra_score, 0) as exposure_score
from normalize_pop_score a
left join normalize_infra_score b
on a.township_code = b.township_code
=======
with popu_infra as(
    select P.Population,
            I.township_code,
            COUNT(infra_id) as infra_number
    from {{ ref('stg_population') }} as P
    join {{ ref('infra_with_township') }} as I
    on P.Township_ID = I.township_code
    group by I.township_code, P.Population
),

normalized as(
    select 
    (Population - MIN(Population) OVER())
        /
        NULLIF(MAX(Population) OVER() - MIN(Population) OVER(), 0)
        AS population_norm,
    (infra_number - MIN(infra_number) OVER())/NULLIF(MAX(infra_number) OVER() - MIN(infra_number) OVER(), 0) AS infra_norm,
    township_code
    from popu_infra
),
final as(
    select township_code,
            (0.6 * population_norm) + (0.4 * infra_norm) as exposure
    from normalized
)
select * from final
>>>>>>> a6762577709de747f7e098446b083e50a9e88f4f
