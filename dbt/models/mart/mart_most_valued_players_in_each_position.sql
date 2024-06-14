WITH cte_add_rn AS (
  SELECT
    current_club_id AS club_id,
    current_club_name AS club_name,
    dp.position,
    name AS player_name,
    current_market_value_in_eur,
    row_number() OVER(PARTITION BY position ORDER BY current_market_value_in_eur DESC) AS rn
  FROM
    `transfermarkt_core.dim_players` dp
  WHERE current_club_id = 418
)
SELECT
  club_id,
  club_name,
  position,
  player_name,
  current_market_value_in_eur AS value_in_euro
FROM
  cte_add_rn
WHERE
  rn < 4