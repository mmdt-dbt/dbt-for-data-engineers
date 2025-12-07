WITH clean_step1 AS(
SELECT infra_id,
	latitude,
	longitude,
    trim(replace(replace(township, ' township', ''),' district', '')) AS township
FROM  {{ source('risk_analytics_mm','infrastructure_township') }}
),
clean_step2 AS (
SELECT infra_id,
latitude,
longitude,
    CASE 
        WHEN township LIKE 'hlaingtharya%' THEN 'hlaingtharyar'
        WHEN township LIKE 'north%dagon' THEN 'dagon myothit north'
        WHEN township LIKE 'east%dagon' THEN 'dagon myothit east'
        WHEN township LIKE 'dagon%seikkan' THEN 'dagon myothit seikkan'
        WHEN township LIKE 'south%dagon' THEN 'dagon myothit south'
        WHEN township LIKE '%ကြာအင်းဆိပ်ကြီး%' THEN 'kyainseikgyi'
        ELSE township
    END AS township
FROM clean_step1
),
clean_step3 AS (
SELECT infra_id,
latitude,
longitude,
replace(trim(township), ' ','') as township
FROM clean_step2
)
SELECT * from clean_step3