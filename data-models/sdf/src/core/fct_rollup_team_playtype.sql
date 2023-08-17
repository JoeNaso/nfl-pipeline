with games as (
    select
        posteam as team,
        game_id,
        play_type,
        count(play_id) as play_type_count,
    from pbp_all
    group by 1, 2, 3
)

select
    team, 
    base_game.game_id,
    play_type,
    play_type_count,
    games.team = base_game.victor as team_is_victor
from games
inner join base_game
    on games.game_id = base_game.game_id