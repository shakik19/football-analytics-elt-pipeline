SELECT
  dgi.season,
  dc.name AS club_name,
  fa.player_name,
  dp.current_market_value_in_eur,
  COUNT(*) AS matches_played,
  SUM(fa.minutes_played) AS total_minutes_played,
  ROUND(AVG(fa.minutes_played), 2) AS avg_minutes_played,
  SUM(fa.goals) AS goals,
  SUM(fa.assists) AS assists,
  ROUND(
    (SUM(fa.goals) + SUM(fa.assists)) / 
    CASE 
      WHEN SUM(fa.minutes_played) = 0 THEN 1 
      ELSE (SUM(fa.minutes_played) / 90) 
    END, 2
  ) AS ga_per_90_minutes,
  SUM(fa.yellow_cards) AS yellow_cards,
  SUM(fa.red_cards) AS red_cards
FROM
  `transfermarkt_core.fact_appearances` fa
LEFT JOIN 
  `transfermarkt_core.dim_game_info` dgi USING(game_id)
LEFT JOIN 
  `transfermarkt_core.dim_clubs` dc ON dc.club_id = fa.player_club_id
LEFT JOIN `transfermarkt_core.dim_players` dp USING(player_id)
GROUP BY
  dgi.season,
  dc.name,
  fa.player_name,
  dp.current_market_value_in_eur
ORDER BY
  dgi.season DESC
