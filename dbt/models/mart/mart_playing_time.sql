SELECT
  fa.player_club_id AS club_id,
  dc.name AS club_name,
  dgi.season,
  fa.player_name,
  COUNT(1) AS total_appearances,
  SUM(fa.minutes_played) AS total_playing_time,
  ROUND(AVG(fa.minutes_played), 2) AS avg_playing_time,
FROM
  `transfermarkt_core.fact_appearances` fa
  LEFT JOIN `transfermarkt_core.dim_game_info` dgi USING(game_id)
  LEFT JOIN `transfermarkt_core.dim_clubs` dc ON dc.club_id = fa.player_club_id
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC