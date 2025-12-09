WITH base AS(
    SELECT ET.earthquake_id,
            depth_km,
            magnitude,
            ET.township_code
    FROM {{ ref('stg_usgs_earthquakes') }} AS UE
    LEFT JOIN {{ ref('earthquake_with_township') }} AS ET
    ON ET.earthquake_id = UE.id
),

with_township AS (
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY township_code) AS frequency
    FROM base
),

normalized AS (
    SELECT
        township_code,
        magnitude,
        depth_km,
        frequency,

        (magnitude - MIN(magnitude) OVER())
        /
        NULLIF(MAX(magnitude) OVER() - MIN(magnitude) OVER(), 0)
        AS magnitude_norm,

        1 - (
            (depth_km - MIN(depth_km) OVER())
            /
            NULLIF(MAX(depth_km) OVER() - MIN(depth_km) OVER(), 0)
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
