WITH cte_add_rn AS (
  SELECT
    current_club_id AS club_id,
    current_club_name AS club_name,
    dp.position,
    name AS player_name,
    current_market_value_in_eur,
    row_number() OVER(PARTITION BY position, current_club_id ORDER BY current_market_value_in_eur DESC) AS rn
  FROM
    {{ ref("dim_players") }} dp
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