WITH cte_max AS (
  SELECT
    fcg.own_id AS club_id,
    dgi.season,
    dgi.competition_id,
    MAX(fcg.game_date) AS last_game
  FROM
    {{ ref("fact_club_games") }} fcg
    LEFT JOIN {{ ref("dim_game_info") }} dgi USING(game_id)
  GROUP BY
    1, 2, 3
)
SELECT
  cm.club_id,
  dc.name,
  cm.season,
  cm.competition_id,
  dgi.competition_type,
  CASE
    WHEN dgi.round = "Final" AND fcg.is_win = 1 THEN "Champion"
    WHEN dgi.round = "Final" AND fcg.is_win = -1 THEN "Runner-Up"
    WHEN dgi.competition_type = "domestic_league" AND fcg.own_position = 1 THEN "Champion"
    WHEN dgi.competition_type = "domestic_league" THEN CAST(fcg.own_position AS STRING)
    ELSE dgi.round
  END AS last_standing
FROM
  cte_max cm
  INNER JOIN {{ ref("fact_club_games") }} fcg 
    ON (fcg.game_date = cm.last_game AND fcg.own_id = cm.club_id)
  INNER JOIN {{ ref("dim_game_info") }} dgi USING(game_id)
  INNER JOIN {{ ref("dim_clubs") }} dc ON dc.club_id = fcg.own_id
















