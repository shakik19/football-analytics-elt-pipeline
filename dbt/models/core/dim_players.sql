-- This SQL script defines a dbt model that calculates various player attributes and their market values.
-- It aggregates player valuation data to obtain the current and highest market values for each player.

WITH
last_updates AS (
    -- CTE to get the last update time for each player
    SELECT
        player_id,
        MAX({{ dbt.safe_cast("date", api.Column.translate_type("date")) }}) AS last_updated_at
    FROM
        {{ source("raw", "player_valuations") }}
    GROUP BY
        1
),

values_of_each AS (
    -- CTE to get the market values of each player for each date
    SELECT
        player_id,
        {{ dbt.safe_cast("date", api.Column.translate_type("date")) }} AS date,
        market_value_in_eur
    FROM
        {{ source("raw", "player_valuations") }}
),

current_values AS (
    -- CTE to get the current market value for each player
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

max_values AS (
    -- CTE to get the highest market value for each player
    SELECT
        player_id,
        MAX(market_value_in_eur) AS market_value_in_eur
    FROM
        {{ source("raw", "player_valuations") }}
    GROUP BY
        1
),

players AS (
    -- CTE to get player attributes
    SELECT
        player_id,
        first_name,
        last_name,
        name,
        last_season,
        CASE 
            WHEN last_season = (SELECT MAX(last_season) FROM {{ source("raw", "players") }}) 
                THEN current_club_id  
            ELSE NULL
        END AS current_club_id,
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
    FROM 
        {{ source("raw", "players") }}
)

-- Final SELECT statement to combine player attributes and market values
SELECT
    players.player_id,
    players.first_name,
    players.last_name,
    players.name,
    players.last_season,
    players.current_club_id,
    clubs.name AS current_club_name,
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
    LEFT JOIN
    {{ source("raw", "clubs") }} clubs
    ON clubs.club_id = players.current_club_id