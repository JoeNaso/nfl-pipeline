create external table pbp_2021
stored as parquet 
with header row 
location 's3://purview-snowflake/raw/nfl/pbp-2021.parquet'
;