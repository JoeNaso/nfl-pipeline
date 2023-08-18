select
    distinct home_team as team,
    team_masked
from {{ ref('base_game') }}
