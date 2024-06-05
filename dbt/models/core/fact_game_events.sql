WITH cte_game_events AS(
    SELECT
        game_event_id,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} AS ingestion_date,
        game_id,
        CASE 
            WHEN src.minute < 0 THEN 1 
            ELSE src.minute
        END as clock,
        src.type AS event_type,
        club_id,
        player_id,
        description,
        player_in_id,
        player_assist_id
    FROM
        {{ source("raw", "game_events") }} src
    WHERE
        type = "Substitutions" AND player_in_id IS NOT NULL
)

SELECT
    *
FROM
    cte_game_events

{% if is_incremental() %}
WHERE ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
