SELECT
  current_club_id AS club_id,
  current_club_name AS club_name,
  position,
  COUNT(1) AS total_players,
  SUM(current_market_value_in_eur) AS value_in_euro,
  ROUND(AVG(age), 2) AS avg_age
FROM
  {{ ref("dim_players") }}
WHERE
  current_club_name IS NOT NULL
GROUP BY
  1, 2, 3