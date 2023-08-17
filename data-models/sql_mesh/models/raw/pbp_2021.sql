MODEL (
    name raw.pbp_2021,
    kind FULL,
    grain [game_id]
);


select
    game_id::varchar,
    play_id::integer,
    week::integer, 
    game_id || play_id || week || '2021' as compound_key,
    play_type::varchar,
    posteam::varchar,
    posteam_type::varchar,
    yards_gained::integer,
    qtr::integer as qtr,
    down::integer as down,
    goal_to_go::boolean,
    time::varchar, -- this is a weird one bc it's minute:seconds, counting down from 15:00
    yrdln::integer,
    yardline_100::integer,
    total_home_score::integer as total_home_score,
    total_away_score::integer as total_away_score

from pbp_2021
