WITH cte_games AS(
    SELECT
        game_id,
        competition_id,
        season,
        src.round AS match_day,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} as game_date,
        EXTRACT(MONTH FROM date) AS game_month,
        CASE 
            WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 12 THEN CONCAT(season, "/", season + 1)
            ELSE CONCAT(season - 1, "/", season)
        END AS full_season,
        CASE 
            WHEN EXTRACT(MONTH FROM date) BETWEEN 6 AND 12 THEN 'First Half'
            ELSE 'Second Half'
        END AS season_half,
        home_club_id,
        away_club_id,
        home_club_goals,
        away_club_goals,
        home_club_position,
        away_club_position,
        home_club_manager_name,
        away_club_manager_name,
        stadium,
        attendance,
        referee,
        home_club_formation,
        away_club_formation,
        home_club_name,
        away_club_name,
        src.aggregate AS agg_score,
        competition_type
    FROM
        {{ source("raw", "games") }} src
)

SELECT
    *
FROM
    cte_games

{% if is_incremental() %}
WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}
