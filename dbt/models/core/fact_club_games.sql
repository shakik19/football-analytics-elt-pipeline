WITH 
cte_home AS(
    SELECT
        fcg.game_id,
        fg.date AS game_date,
        fcg.club_id AS own_id,
        fcg.opponent_id,
        fcg.own_goals,	
        fcg.opponent_goals,	
        fcg.own_position,
        fcg.opponent_position,	
        fcg.own_manager_name,	
        fcg.opponent_manager_name,	
        fg.home_club_formation AS home_club_formation,	
        fg.away_club_formation AS opponent_club_formation,	
        {{ is_win('fcg.own_goals', 'fcg.opponent_goals') }} AS is_win,
        {{ get_points('fcg.own_goals', 'fcg.opponent_goals') }} AS points,
        "home" AS game_location
    FROM
        {{ source("raw", "club_games") }} fcg
    LEFT JOIN
        {{ source("raw", "games") }} fg
    ON
        fcg.game_id = fg.game_id
    WHERE
        fcg.club_id = fg.home_club_id
    {% if is_incremental() %}
    AND fg.date > (SELECT MAX(date) FROM {{ source("raw", "games") }})
    {% endif %}
),
cte_away AS(
    SELECT
        fcg.game_id,
        fg.date AS game_date,
        fcg.club_id AS own_id,
        fcg.opponent_id,
        fcg.own_goals,	
        fcg.opponent_goals,	
        fcg.own_position,
        fcg.opponent_position,	
        fcg.own_manager_name,	
        fcg.opponent_manager_name,
        fg.away_club_formation AS own_formation,	
        fg.home_club_formation AS opponent_formation,	
        {{ is_win('fcg.own_goals', 'fcg.opponent_goals') }} AS is_win,
        {{ get_points('fcg.own_goals', 'fcg.opponent_goals') }} AS points,
        "away" AS game_location	
    FROM
        {{ source("raw", "club_games") }} fcg
    LEFT JOIN
        {{ source("raw", "games") }} fg
    ON
        fcg.game_id = fg.game_id
    WHERE
        fcg.club_id = fg.away_club_id
    {% if is_incremental() %}
    AND fg.date > (SELECT MAX(date) FROM {{ source("raw", "games") }})
    {% endif %}
)
SELECT
    *
FROM
    cte_home
UNION ALL
SELECT
    *
FROM
    cte_away
