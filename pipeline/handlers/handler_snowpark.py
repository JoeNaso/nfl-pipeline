from typing import Optional

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, concat, lit

from pipeline.schema import SCHEMA_PLAY_BY_PLAY


COLUMNS_PER_PLAY = [
    "compound_key",
    "game_id",
    "play_id",
    "play_type",
    "posteam",
    "posteam_type",
    "yards_gained",
    "qtr",
    "down",
    "goal_to_go",
    "time",
    "yrdln",
    "yardline_100",
    "total_home_score",
    "total_away_score",
]


def get_session() -> snowpark.Session:
    creds = {
        "account": "<your snowflake account>",
        "user": "<your snowflake user>",
        "password": "<your snowflake password>",
        "warehouse": "<your snowflake warehouse>",  # optional
        "database": "<your snowflake database>",    # optional
    }
    return snowpark.Session.config(creds).create()


def get_raw_data(session, table="raw.nfl.play_by_play") -> snowpark.DataFrame:
    """
    Alternatively, this could read from a stage directly
    and the Snowpipe loads to table could be removed
    """
    session.sql("use database raw;").collect()
    session.sql("use schema raw.nfl;").collect()
    session.sql("use warehouse compute_wh;").collect()

    df = (
        session.read.options({"field_delimiter": "|", "skip_header": 1})
        .schema(SCHEMA_PLAY_BY_PLAY)
        .csv("@nfl_play_by_play")
    )
    df = df.with_column("compound_key", concat(df["game_id"], lit("|"), df["play_id"]))
    return df


def downselect_dataframe(df: snowpark.DataFrame) -> snowpark.DataFrame:
    return df.select([col(name) for name in COLUMNS_PER_PLAY])


def filter_data_by_type(df, column, value) -> snowpark.DataFrame:
    return df.filter(col(column) == value)


def run_sql(session: snowpark.Session, query) -> snowpark.DataFrame:
    """
    This could be replaced with df.create_or_replace_view() or something similar
    """
    res = session.sql(query)
    return res.collect()


def check_table_exists(session: snowpark.Session, table_name: str) -> bool:
    qry = f"""
        select 
            to_boolean(count(1))
        from information_schema.tables
        where table_schema = '{session.get_current_schema()}' 
            and table_name = '{table_name}';
    """
    return run_sql(session, qry)


def map_datatype(dtype: str) -> str:
    if dtype == "StringType()":
        return "VARCHAR"
    elif dtype in ("FloatType()", "DoubleType()"):
        return "NUMBER"


def build_create_table_command(
    session: snowpark.Session,
    df: snowpark.DataFrame,
    name: str
) -> str:
    """
    Dynamically build create table statement
    """
    db, schema = session.get_current_database(), session.get_current_schema()
    command = f"create table if not exists {db}.{schema}.{name}"
    columns = f""
    fields = df.schema.__dict__["fields"]
    for idx, column in enumerate(df.schema.__dict__["fields"]):
        as_dict = column.__dict__
        columns += f"{as_dict.name} AS {map_datatype(as_dict.datatype)}"
        if not idx == len(fields) - 1:
            columns += ",\n"
    return f"{command} ({columns});"


def main(session: Optional[snowpark.Session]):
    session = session or get_session()
    data = get_raw_data(session)
    cleaned = downselect_dataframe(data)
    for play_type in cleaned.select("PLAY_TYPE").distinct():
        filtered = filter_data_by_type(cleaned, "PLAY_TYPE", play_type)
        if not check_table_exists(session, play_type):
            cmd = build_create_table_command(session, filtered, play_type)
            run_sql(session, cmd)
        filtered.write.mode("append").save_as_table(f"transformed.nfl.{play_type}")

    session.close()
