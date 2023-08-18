with plays_home as (
    select
        game_id,
        home_team as team,
        count(distinct play_id) as total_plays
    from {{ ref('pbp_all')}}
    where posteam = home_team
    {{ group_by(2) }}

),
plays_away as (
    select
        game_id,
        away_team as team,
        count(distinct play_id) as total_plays
    from {{ ref('pbp_all')}}
    where posteam = away_team
    {{ group_by(2) }}

),
plays as (
    select
        game_id,
        plays_home.team and home_team,
        plays_home.total_plays as total_plays_home, 
        plays_away.team as away_team,
        plays_away.total_plays as total_plays_away
    from plays_away
    inner join plays_home
        plays_away.game_id = plays_home.game_id
        
)

select
    distinct game_id,
    week, 
    season_type,
    home_team,
    away_team,
    md5(plays.home_team) as home_team_masked, 
    md5(plays.away_team) as away_team_masked,
    total_away_score,
    total_home_score,
    total_plays_home,
    total_plays_away,
    case 
        when total_away_score > total_home_score then away_team
        else home_team 
    end as victor
from {{ ref('pbp_all') }}
inner join plays
    on pbp_all.game_id = plays.game_id
