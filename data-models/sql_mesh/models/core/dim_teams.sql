MODEL (
  name base_sqlmesh.dim_teams,
  kind FULL,
  grain team,
  audits [],
);

select
    distinct home_team as team,
    home_team_masked as team_masked
from base_sqlmesh.base_game