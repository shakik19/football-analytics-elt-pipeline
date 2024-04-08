import pandas as pd


class Appearances:
    schema = {
    'appearance_id':pd.StringDtype(),
    'game_id':pd.Int64Dtype(),
    'player_id':pd.Int64Dtype(),    
    'player_club_id':pd.Int64Dtype(),
    'player_current_club_id':pd.Int64Dtype(),
    'date': pd.StringDtype(),
    'player_name':pd.StringDtype(),
    'competition_id':pd.StringDtype(),
    'yellow_cards':pd.Int64Dtype(),
    'red_cards':pd.Int64Dtype(),
    'goals':pd.Int64Dtype(),
    'assists':pd.Int64Dtype(),
    'minutes_played':pd.Int64Dtype(),
    }

class Club_games:
    schema = {
        'game_id':pd.Int64Dtype(),
        'club_id':pd.Int64Dtype(),
        'own_goals':pd.Int64Dtype(),
        'own_position':pd.Int64Dtype(),
        'own_manager_name':pd.StringDtype(),
        'opponent_id':pd.Int64Dtype(),
        'opponent_goals':pd.Int64Dtype(),
        'opponent_position':pd.Int64Dtype(),
        'opponent_manager_name':pd.StringDtype(),
        'hosting':pd.StringDtype(),
        'is_win':pd.Int64Dtype(),
    }

class Clubs:
    schema = {
    'club_id':pd.Int64Dtype(),
    'club_code':pd.StringDtype(),
    'name':pd.StringDtype(),
    'domestic_competition_id':pd.StringDtype(),
    'total_market_value':pd.Int64Dtype(),
    'squad_size':pd.Int64Dtype(),
    'average_age':pd.Float64Dtype(),
    'foreigners_number':pd.Int64Dtype(),
    'foreigners_percentage':pd.Float64Dtype(),
    'national_team_players':pd.Int64Dtype(),
    'stadium_name':pd.StringDtype(),
    'stadium_seats':pd.Int64Dtype(),
    'net_transfer_record':pd.StringDtype(),
    'coach_name':pd.StringDtype(),
    'last_season':pd.Int64Dtype(),
    'filename':pd.StringDtype(),
    'url':pd.StringDtype(),
    }

class Competitions:
    schema = {
    'competition_id':pd.StringDtype(),
    'competition_code':pd.StringDtype(),
    'name':pd.StringDtype(),
    'sub_type':pd.StringDtype(),
    'type':pd.StringDtype(),
    'country_id':pd.Int64Dtype(),
    'country_name':pd.StringDtype(),
    'domestic_league_code':pd.StringDtype(),
    'confederation':pd.StringDtype(),
    'url':pd.StringDtype(),
    'is_major_national_league':pd.BooleanDtype(),
    }

class Game_events:
    schema = {
    'game_event_id':pd.StringDtype(),
    'date': pd.StringDtype(),
    'game_id':pd.Int64Dtype(),
    'minute':pd.Int64Dtype(),
    'type':pd.StringDtype(),
    'club_id':pd.Int64Dtype(),
    'player_id':pd.Int64Dtype(),
    'description':pd.StringDtype(),
    'player_in_id':pd.Int64Dtype(),
    'player_assist_id':pd.Int64Dtype(),
    }

class Game_lineups:
    schema = {
    'game_lineups_id':pd.StringDtype(),
    'date': pd.StringDtype(),
    'game_id':pd.Int64Dtype(),
    'player_id':pd.Int64Dtype(),
    'club_id':pd.Int64Dtype(),
    'player_name':pd.StringDtype(),
    'type':pd.StringDtype(),
    'position':pd.StringDtype(),
    'number':pd.StringDtype(),
    'team_captain':pd.Int64Dtype(),
    }

class Games:
    schema = {
    'game_id':pd.Int64Dtype(),
    'competition_id':pd.StringDtype(),
    'season':pd.Int64Dtype(),
    'round':pd.StringDtype(),
    'date': pd.StringDtype(),
    'home_club_id':pd.Int64Dtype(),
    'away_club_id':pd.Int64Dtype(),
    'home_club_goals':pd.Int64Dtype(),
    'away_club_goals':pd.Int64Dtype(),
    'home_club_position':pd.Int64Dtype(),
    'away_club_position':pd.Int64Dtype(),
    'home_club_manager_name':pd.StringDtype(),
    'away_club_manager_name':pd.StringDtype(),
    'stadium':pd.StringDtype(),
    'attendance':pd.Int64Dtype(),
    'referee':pd.StringDtype(),
    'url':pd.StringDtype(),
    'home_club_formation':pd.StringDtype(),
    'away_club_formation':pd.StringDtype(),
    'home_club_name':pd.StringDtype(),
    'away_club_name':pd.StringDtype(),
    'aggregate':pd.StringDtype(),
    'competition_type':pd.StringDtype(),
    }

class Player_valuations:
    schema = {
    'player_id':pd.Int64Dtype(),
    'date': pd.StringDtype(),
    'market_value_in_eur':pd.Int64Dtype(),
    'current_club_id':pd.Int64Dtype(),
    'player_club_domestic_competition_id':pd.StringDtype(),
    }

class Players:
    schema = {
        'player_id': pd.Int64Dtype(),
        'first_name': pd.StringDtype(),
        'last_name': pd.StringDtype(),
        'name': pd.StringDtype(),
        'last_season': pd.Int64Dtype(),
        'current_club_id': pd.Int64Dtype(),
        'player_code': pd.StringDtype(),
        'country_of_birth': pd.StringDtype(),
        'city_of_birth': pd.StringDtype(),
        'country_of_citizenship': pd.StringDtype(),
        'date_of_birth': pd.StringDtype(),
        'sub_position': pd.StringDtype(),
        'position': pd.StringDtype(),
        'foot': pd.StringDtype(),
        'height_in_cm': pd.Float64Dtype(),
        'contract_expiration_date': pd.StringDtype(),
        'agent_name': pd.StringDtype(),
        'image_url': pd.StringDtype(),
        'url': pd.StringDtype(),
        'current_club_domestic_competition_id': pd.StringDtype(),
        'current_club_name': pd.StringDtype(),
        'market_value_in_eur': pd.Float64Dtype(),
        'highest_market_value_in_eur': pd.Float64Dtype()
    }
    
    