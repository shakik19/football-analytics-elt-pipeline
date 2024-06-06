WITH club_performance AS (
    SELECT
        season,
        own_id,
        SUM(CASE WHEN is_win = 1 THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN is_win = -1 THEN 1 ELSE 0 END) AS losses,
        SUM(CASE WHEN is_win = 0 THEN 1 ELSE 0 END) AS draws,
        SUM(points) / COUNT(*) AS ppm,
        COUNT(*) AS total_games
    FROM {{ ref('fact_club_games') }}
    JOIN {{ ref('dim_game_info') }} USING(game_id)
    GROUP BY season, own_id
)

SELECT
    cpf.season,
    dc.name,
    cpf.wins,
    cpf.losses,
    cpf.draws,
    ROUND((cpf.wins / cpf.total_games) * 100, 2) AS win_percentage,
    ROUND(cpf.ppm, 2) AS points_per_game
FROM 
    club_performance cpf
LEFT JOIN
    {{ ref('dim_clubs') }} dc
ON cpf.own_id = dc.club_id

