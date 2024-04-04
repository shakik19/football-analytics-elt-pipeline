with
squad_value AS(
    SELECT
        current_club_id,
        SUM(current_market_value_in_eur) AS sum_value
    FROM
        {{ ref("dim_players") }}
    GROUP BY
        1
),
clubs AS(
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
    clubs.name,
    clubs.domestic_competition_id,
    clubs.squad_size,
    sv.sum_value AS squad_value,
    clubs.average_age,
    clubs.foreigners_number,
    clubs.foreigners_percentage,
    clubs.national_team_players,
    clubs.stadium_name,
    clubs.stadium_seats,
    clubs.net_transfer_record,
    clubs.last_season
FROM
    clubs
    INNER JOIN
    squad_value sv
    ON clubs.club_id = sv.current_club_id