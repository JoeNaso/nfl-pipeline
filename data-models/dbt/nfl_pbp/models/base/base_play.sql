
select
    game_id,
    play_id,
    play_type,
    posteam,
    posteam_type,
    yards_gained,
    qtr::integer as qtr,
    down::integer as down,
    goal_to_go,
    time,
    yrdln,
    yardline_100,
    total_home_score::integer as total_home_score,
    total_away_score::integer as total_away_score
from {{ ref('pbp_all') }}
