WITH cte_game_events AS(
    SELECT
        game_event_id,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} AS ingestion_date,
        game_id,
        src.minute AS clock,
        src.type,
        club_id,
        player_id,
        description,
        player_in_id,
        player_assist_id
    FROM
        {{ source("raw", "game_events") }} src
)

SELECT
    *
FROM
    cte_game_events

{% if is_incremental() %}
WHERE ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
