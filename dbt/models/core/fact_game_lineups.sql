WITH cte_game_lineups AS(
    SELECT
        game_lineups_id,
        game_id,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} AS match_date,
        player_id,
        club_id,
        player_name,
        src.type,
        src.position AS playing_position,
    FROM
        {{ source("raw", "game_lineups") }} src
)

SELECT
    *
FROM
    cte_game_lineups

{% if is_incremental() %}
WHERE match_date > (SELECT MAX(match_date) FROM {{ this }})
{% endif %}