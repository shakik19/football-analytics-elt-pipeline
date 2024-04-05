SELECT
    game_event_id,
    date AS ingestion_date,
    game_id,
    minutes AS clock,
    type,
    club_id,
    player_id,
    description,
    player_in_id,
    player_assist_id
FROM
    {{ source("raw", "game_events") }}
{% if is_incremental() %}
WHERE ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
{% endif %}
