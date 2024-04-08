WITH
last_updates AS(
    SELECT
        player_id,
        MAX({{ dbt.safe_cast("date", api.Column.translate_type("date")) }}) AS last_updated_at
    FROM
        {{ source("raw", "player_valuations") }}
    GROUP BY
      1   
),
values_of_each AS(
    SELECT
        player_id,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} AS date,
        market_value_in_eur
    FROM
        {{ source("raw", "player_valuations") }}
),
current_values AS(
    SELECT
        lup.player_id,
        ve.market_value_in_eur
    FROM
        last_updates AS lup
        INNER JOIN
        values_of_each AS ve
        ON lup.last_updated_at = ve.date
    WHERE
        lup.player_id = ve.player_id
),
max_values AS(
    SELECT
        player_id,
        MAX(market_value_in_eur) AS market_value_in_eur
    FROM
        {{ source("raw", "player_valuations") }}
    GROUP BY
        1
),
players AS(
    SELECT
        player_id,
        first_name,
        last_name,
        name,
        last_season,
        current_club_id,
        {{ dbt.safe_cast("date_of_birth", api.Column.translate_type("date")) }} AS date_of_birth,
        country_of_birth,
        city_of_birth,
        country_of_citizenship,
        sub_position,
        position,
        foot,
        height_in_cm,
        agent_name,
        {{ dbt.safe_cast("contract_expiration_date", api.Column.translate_type("date")) }} as contract_expiration_date,
        current_club_domestic_competition_id,
        current_club_name
    FROM 
        {{ source("raw", "players") }}
)
SELECT
    players.player_id,
    players.first_name,
    players.last_name,
    players.name,
    players.last_season,
    players.current_club_id,
    DATE_DIFF(CURRENT_DATE(), players.date_of_birth, YEAR) AS age,
    players.date_of_birth,
    players.country_of_birth,
    players.city_of_birth,
    players.country_of_citizenship,
    players.sub_position,
    players.position,
    players.foot,
    players.height_in_cm,
    players.agent_name,
    players.contract_expiration_date,
    players.current_club_domestic_competition_id,
    players.current_club_name,
    cv.market_value_in_eur AS current_market_value_in_eur,
    mv.market_value_in_eur AS highest_market_value_in_eur
FROM
    players
    INNER JOIN
    current_values AS cv
    ON players.player_id = cv.player_id
    INNER JOIN
    max_values AS mv
    ON players.player_id = mv.player_id
