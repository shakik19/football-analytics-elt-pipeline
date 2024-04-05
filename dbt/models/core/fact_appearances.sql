{{
    config(
        materialized = "incremental",
        cluster_by = ["player_id", "player_club_id"],
        partition_by = {
        "field": "match_date",
        "data_type": "date",
        "granularity": "month"
        }
    )
}}

WITH cte_appearances AS(
    SELECT
        appearance_id,
        game_id,
        player_id,
        player_club_id,
        player_current_club_id,
        competition_id,
        date AS match_date,
        player_name,
        yellow_cards,
        red_cards,
        goals,
        assists,
        minutes_played
    FROM
        {{ source("raw", "appearances") }}
)
SELECT
    *
FROM
    cte_appearances

{% if is_incremental() %}
WHERE match_date > (SELECT MAX(match_date) FROM {{ this }})
{% endif %}