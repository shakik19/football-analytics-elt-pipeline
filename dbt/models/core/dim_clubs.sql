-- This model calculates the latest club details by combining data from multiple sources.
-- It includes the club's latest manager, the squad's total market value, and other club-specific details.

WITH latest_game_date AS (
  SELECT
    home_club_id,
    MAX(date) AS last_match_date
  FROM
    {{ source("raw", "games") }}
  GROUP BY
    1
),

manager_names AS (
  SELECT
    home_club_id,
    date AS match_date,
    home_club_manager_name
  FROM
    {{ source("raw", "games") }}
),

agg_manager_names AS (
  SELECT
    mn.home_club_id AS club_id,
    mn.home_club_manager_name AS manager_name
  FROM
    latest_game_date AS lgd
    INNER JOIN
    manager_names AS mn
    ON mn.match_date = lgd.last_match_date
  WHERE
    lgd.home_club_id = mn.home_club_id
),

squad_value AS (
  SELECT
    current_club_id,
    SUM(current_market_value_in_eur) AS sum_value
  FROM
    {{ ref("dim_players") }}
  GROUP BY
    1
),

clubs AS (
  SELECT 
    club_id,
    name,
    domestic_competition_id,
    squad_size,
    average_age,
    foreigners_number,
    foreigners_percentage,
    national_team_players,
    stadium_name,
    stadium_seats,
    net_transfer_record,
    last_season
  FROM 
    {{ source("raw", "clubs") }}
)

SELECT
  clubs.club_id,
  clubs.domestic_competition_id,
  clubs.name,
  amn.manager_name,
  clubs.squad_size,
  sv.sum_value AS squad_value,
  clubs.average_age,
  clubs.foreigners_number,
  clubs.foreigners_percentage,
  clubs.national_team_players,
  clubs.stadium_name,
  clubs.stadium_seats,
  REPLACE(REPLACE(clubs.net_transfer_record, "€", ""), "m", "") AS net_transfer_record,
  clubs.last_season
FROM
  clubs
  INNER JOIN
  squad_value sv
  ON clubs.club_id = sv.current_club_id
  INNER JOIN
  agg_manager_names AS amn
  ON clubs.club_id = amn.club_id
