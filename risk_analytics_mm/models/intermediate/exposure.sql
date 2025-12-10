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

