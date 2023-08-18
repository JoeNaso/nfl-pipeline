MODEL (
  name base_sqlmesh.base_plays,
  kind FULL,
  grain compound_key,
  audits [],
);

select
    compound_key,
    game_id,
    play_id,
    play_type,
    posteam,
    posteam_type,
    yards_gained,
    qtr,
    down,
    goal_to_go,
    time,
    yrdln,
    yardline_100,
    total_home_score,
    total_away_score
from raw.pbp_all
