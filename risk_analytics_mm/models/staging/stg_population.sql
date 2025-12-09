WITH populationByTownship AS(
    SELECT * FROM {{ source('risk_analytics_mm', 'population') }}
),
Population_Township AS(
    SELECT
    CASE 
    WHEN Township like 'Depayin' THEN 'MMR005010' 
    WHEN Township like 'Hlinethaya (West)' THEN 'MMR013008'
    WHEN Township like 'Hsotlaw' THEN 'MMR001006'
    WHEN Township like 'Htantapin' THEN 'MMR007014'
    WHEN Township like 'Lechar' THEN 'MMR014012'
    WHEN Township like 'Leshi' THEN 'MMR005035'
    WHEN Township like 'Loilin' THEN 'MMR014011'
    WHEN Township like 'Minekat' THEN 'MMR004006'
    WHEN Township like 'Minelar' THEN 'MMR016005'
    WHEN Township like 'Minemaw' THEN 'MMR015008'  
    WHEN Township like 'Minepan' THEN 'MMR014021'
    WHEN Township like 'Minephyat' THEN 'MMR016010'
    WHEN Township like 'Minepyin' THEN 'MMR016007' 
    WHEN Township like 'Minesat' THEN 'MMR016006'
    WHEN Township like 'Mineshu' THEN 'MMR014017'
    WHEN Township like 'Minetung' THEN 'MMR016008'
    WHEN Township like 'Mineyan' THEN 'MMR016003'
    WHEN Township like 'Mineyaung' THEN 'MMR016011'
    WHEN Township like 'Mineye`' THEN 'MMR015003'
    WHEN Township like 'Myauk U' THEN 'MMR012003'                            
    WHEN Township like 'Parsaung' THEN 'MMR002006'
    WHEN Township like 'Phakant' THEN 'MMR001009'
    WHEN Township like 'Pharpon' THEN 'MMR017023'
    WHEN Township like 'Phruso' THEN 'MMR002003'
    WHEN Township like 'Tanatpin' THEN 'MMR007002'
    WHEN Township like 'Theinni' THEN 'MMR015002'
    WHEN Township like 'Yaedashe' THEN 'MMR007010'
    WHEN Township like 'Yanbye' THEN 'MMR012013'
    WHEN Township like 'Yathedaung' THEN 'MMR012008'
    WHEN Township like 'Yatsauk' THEN 'MMR014008'
    WHEN Township_ID = '' THEN 'NULL'
    ELSE Township_ID
    END AS Township_ID,
    Township AS Township_Name,
    population AS Population
FROM populationByTownship
)
SELECT * FROM Population_Township