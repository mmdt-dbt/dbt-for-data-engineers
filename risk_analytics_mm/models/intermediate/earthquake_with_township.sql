with earthquake_with_township as(
select a.earthquake_id, a.latitude, a.longitude, a.township, b.township_name, b.township_code
from {{ ref('stg_earthquake_township') }} a
left join {{ ref('stg_township_summary') }} b
on a.township = b.township_name
)
select * from earthquake_with_township
order by township