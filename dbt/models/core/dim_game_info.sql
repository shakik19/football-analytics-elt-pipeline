WITH cte_temp AS( 
    SELECT
        game_id,
        competition_id,
        competition_type,
        date AS game_date,
        EXTRACT(MONTH FROM date) AS game_month,
        EXTRACT(YEAR FROM date) AS game_year,
        CASE 
            WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 12 THEN CONCAT(season, "/", season + 1)
            ELSE CONCAT(season - 1, "/", season)
        END AS season,
        CASE 
            WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 12 THEN 'first'
            ELSE 'second'
        END AS season_half,
        round,
        stadium,
        attendance,
        referee
    FROM
        {{ source("raw", "games") }}
    WHERE
        game_id IS NOT NULL
        AND
        date IS NOT NULL
)

SELECT
    *
FROM
    cte_temp
{% if is_incremental() %}
WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}