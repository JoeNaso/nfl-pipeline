create external table pbp_2022
stored as parquet
with header row 
location 's3://purview-snowflake/raw/nfl/pbp-2022.parquet'
;