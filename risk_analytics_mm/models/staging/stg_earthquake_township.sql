WITH clean_step1 AS(
SELECT earthquake_id,
	latitude,
	longitude,
    trim(replace(replace(township, ' township', ''),' district', '')) AS township
FROM  {{ source('risk_analytics_mm','earthquake_township') }}
),
clean_step2 AS (
SELECT earthquake_id,
latitude,
longitude,
replace(trim(replace(replace(township, '(', ''), ')', '')), ' ','') as township
FROM clean_step1
),
clean_step3 AS (
SELECT earthquake_id,
latitude,
longitude,
    CASE 
        WHEN township LIKE 'hlaingtharya%' THEN 'hlaingtharya'
        WHEN township LIKE 'northdagon' THEN 'dagonmyothitnorth'
        WHEN township LIKE 'eastdagon' THEN 'dagonmyothiteast'
        WHEN township LIKE 'dagonseikkan' THEN 'dagonmyothitseikkan'
        WHEN township LIKE 'southdagon' THEN 'dagonmyothitsouth'
        WHEN township LIKE 'dekkhinathiri' THEN 'detkhinathiri'
        WHEN township LIKE 'hsenwi' THEN 'hseni'
        WHEN township LIKE 'kalay' THEN 'kale'
        WHEN township LIKE 'lai-hka' THEN 'laihka'
        WHEN township LIKE 'langhko' THEN 'langkho'
        WHEN township LIKE 'leshi' THEN 'layshi'
        WHEN township LIKE 'loilem' THEN 'loilen'
        WHEN township LIKE 'mingalataungnyunt' THEN 'mingalartaungnyunt'
        WHEN township LIKE 'moenyo' THEN 'monyo'
        WHEN township LIKE 'monghpayak' THEN 'monghpyak'
        WHEN township LIKE 'mongkung%' THEN 'mongkaing'
        WHEN township LIKE 'ottarathiri' THEN 'oketarathiri'
        WHEN township LIKE 'pandaung' THEN 'padaung'
        WHEN township LIKE 'putao' THEN 'puta-o'
        WHEN township LIKE 'tharrawaddy' THEN 'thayarwady'
        WHEN township LIKE 'yekyi' THEN 'yegyi'
        WHEN township LIKE 'yinmabin' THEN 'yinmarbin'
        WHEN township LIKE '%ကြာအင်းဆိပ်ကြီး%' THEN 'kyainseikgyi'
        ELSE township
    END AS township
FROM clean_step2
)

SELECT * from clean_step3