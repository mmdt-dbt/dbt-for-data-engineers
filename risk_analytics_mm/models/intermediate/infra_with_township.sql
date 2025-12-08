with infra_with_township as(
select a.infra_id, a.latitude, a.longitude, a.township, b.township_name, b.township_code
from {{ ref('stg_infra_township') }} a
left join {{ ref('stg_township_summary') }} b
on a.township = b.township_name
)
select * from infra_with_township
order by township