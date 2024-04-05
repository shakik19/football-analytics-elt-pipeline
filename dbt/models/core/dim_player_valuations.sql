{{
    config(
        materialized = "incremental",
        cluster_by = "current_club_id",
        partition_by = {
        "field": "ingestion_time",
        "data_type": "date",
        "granularity": "year"
        }
    )
}}

WITH cte_player_vals AS(
    SELECT
        player_id,
        date AS ingestion_time,
        market_value_in_eur,
        current_club_id,
        player_club_domestic_competition_id
    FROM
        {{ source("raw", "player_valuations") }}
)

SELECT
    *
FROM
    cte_player_vals

{% if is_incremental() %}
WHERE ingestion_time > (SELECT max(ingestion_time) FROM {{ this }})
{% endif %}