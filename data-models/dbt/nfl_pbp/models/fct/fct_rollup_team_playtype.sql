with games as (
    select
        posteam as team,
        game_id,
        play_type,
        count(play_id) as play_type_count,
    from {{ ref('pbp_all') }}
    {{ group_by(3) }}
)

select
    team, 
    game_id,
    play_type,
    play_type_count,
    games.team = base_game.victor as team_is_victor
from games
inner join {{ ref('base_game') }}
    on games.game_id = base_game.games_id