WITH cte_games AS(
    SELECT
        game_id,
        competition_id,
        season,
        round AS match_day,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} as game_date,
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
        aggregate,
        competition_type
    FROM
        {{ source("raw", "games") }}
)

SELECT
    *
FROM
    cte_games

{% if is_incremental() %}
WHERE game_date > (SELECT MAX(game_date) FROM {{ this }})
{% endif %}
