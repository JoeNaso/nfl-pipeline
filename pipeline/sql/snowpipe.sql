-- create storage integration
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<AWS-generated-ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>');
  
-- create a basic parquert file format
create file format parquet_simple
type = parquet
compression = auto
trim_space = FALSE
comment = "simple parquet format";

-- create a db and schema for the data 
create database raw;
create schema raw.nfl;

-- create a stage for the raw data just to have it
use database raw;
create stage s3_raw 
storage_integration = s3_integration
url = 's3://<bucket>'
file_format = parquet_simple;

-- create a stage for the data we want to work with
-- in this example
use schema raw.nfl;
create stage nfl_play_by_play 
storage_integration = s3_integration
url = "s3://<bucket>/raw/nfl/"
file_format = parquet_simple;

-- create a table to hold our ingested data 
create table raw.nfl.play_by_play (
    data VARIANT
);

-- create a pipe to copy the staged data to the target table
create pipe raw.nfl.pbp_pipe 
auto_ingest = TRUE
as copy into raw.nfl.play_by_play from @nfl_play_by_play;

--- Kickoff the pipe, check status, and check it loaded
alter pipe raw.nfl.pbp_pipe refresh;
select system$pipe_status('raw.nfl.pbp_pipe');
select count(*) from raw.nfl.play_by_play;
