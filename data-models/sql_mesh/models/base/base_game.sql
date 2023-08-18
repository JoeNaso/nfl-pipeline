MODEL (
  name base_sqlmesh.base_game,
  kind FULL,
  grain game_id,
  audits [],
);

with plays_home as (
    select
        game_id,
        home_team,
        count(distinct play_id) as total_plays
    from raw.pbp_all
    where posteam = home_team
    group by 1, 2

),
plays_away as (
    select
        game_id,
        away_team,
        count(distinct play_id) as total_plays
    from raw.pbp_all
    where posteam = away_team
    group by 1, 2

),
plays as (
    select
        plays_home.game_id,
        plays_home.home_team,
        plays_home.total_plays as total_plays_home, 
        plays_away.away_team,
        plays_away.total_plays as total_plays_away
    from plays_away
    inner join plays_home
       on plays_away.game_id = plays_home.game_id
        
)

select
    distinct plays.game_id,
    week, 
    season_type,
    plays.home_team,
    plays.away_team,
    md5(plays.home_team) as home_team_masked, 
    md5(plays.away_team) as away_team_masked,
    total_away_score,
    total_home_score,
    total_plays_home,
    total_plays_away,
    case 
        when total_away_score > total_home_score then plays.away_team
        else plays.home_team 
    end as victor
from raw.pbp_all
inner join plays
    on pbp_all.game_id = plays.game_id
