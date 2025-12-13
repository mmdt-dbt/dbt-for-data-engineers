WITH base AS(
    SELECT UE.id as earthquake_id,
            depth_km,
            magnitude,
            ET.township_code
    FROM {{ ref('stg_usgs_earthquakes') }} AS UE
    LEFT JOIN {{ ref('earthquake_with_township') }} AS ET
    ON ET.earthquake_id = UE.id
),

with_township AS (
    SELECT
        township_code, 
        AVG(depth_km) AS depth_avg, 
        AVG(magnitude) AS magnitude_avg,
        COUNT(*) AS frequency
    FROM base
    GROUP BY township_code
),

normalized AS (
    SELECT
        township_code,
        magnitude_avg,
        depth_avg,
        frequency,

        (magnitude_avg - MIN(magnitude_avg) OVER())
        /
        NULLIF(MAX(magnitude_avg) OVER() - MIN(magnitude_avg) OVER(), 0)
        AS magnitude_norm,

        1 - (
            (depth_avg - MIN(depth_avg) OVER())
            /
            NULLIF(MAX(depth_avg) OVER() - MIN(depth_avg) OVER(), 0)
        ) AS depth_norm,

    
        (frequency - MIN(frequency) OVER())
        /
        NULLIF(MAX(frequency) OVER() - MIN(frequency) OVER(), 0)
        AS frequency_norm

    FROM with_township
),

final AS(
SELECT township_code,
        (0.5 * magnitude_norm) + (0.3 * depth_norm) + (0.2 * frequency_norm)
        AS Hazard_score
FROM normalized
WHERE township_code IS NOT NULL
) 

SELECT * FROM final
