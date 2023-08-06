create external table pbp_2020
stored as csv 
with header row 
location 's3://purview-snowflake/raw/nfl/pbp-2020.csv';