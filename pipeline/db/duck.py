"""
TODO:
- python entrypoint to local DB (location: /tmp/duck-db/nfl_pbp.duckdb)
"""
import os
import pathlib
from typing import Tuple

import duckdb

RAW_SCHEMA = "raw"
DB_FILE = "nfl_pbp.duckdb"
TABLES = ("pbp_2020", "pbp_2021", "pbp_2022")
S3_PATH = 's3://purview-snowflake/raw/nfl/'

def connect(local_exists=False) -> Tuple[bool, duckdb.DuckDBPyConnection]:
    if not local_exists:
        conn = duckdb.connect(database=DB_FILE, read_only=False)
        return True, conn
    return False, duckdb.connect(database=DB_FILE)


def create_schema(conn: duckdb.DuckDBPyConnection):
    conn.sql(f"create schema if not exists {RAW_SCHEMA};")


def prep_seed(conn: duckdb.DuckDBPyConnection):
    conn.sql('INSTALL httpfs;')
    conn.sql('LOAD httpfs;')
    conn.sql(f"SET s3_region='{os.environ.get('AWS_DEFAULT_REGION')}';")
    conn.sql(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID')}';")
    conn.sql(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';")
    conn.commit()

def seed(conn: duckdb.DuckDBPyConnection):
    """
    These could all be loaded into a single file, but let's leave them in
    separate tables and handle with the data modeling tools
    """
    for table in TABLES:
        fn = table.replace('_', '-')
        s3_fn = f"{S3_PATH}{fn}.parquet"
        cmd = (
            f"create table if not exists {RAW_SCHEMA}.{table} as "
            f"select * from read_parquet('{s3_fn}');"
        )
        print(f"Executing following command:\n\t{cmd}")
        conn.sql(cmd)
        print(f"Completed: {RAW_SCHEMA}.{table}")


def load_duckdb():
    curr = pathlib.Path(os.path.abspath(".")).parent.parent.resolve()
    local_fn = os.path.join(os.sep, curr, DB_FILE)
    local_exists = os.path.exists(local_fn)
    created, conn = connect(local_exists=local_exists)
    if created:
        create_schema(conn)
        prep_seed(conn)
        seed(conn)
        conn.sql(f"EXPORT DATABASE '{local_fn}' (FORMAT PARQUET);")
