import os

import snowflake.connector


BUCKET = "purview-snowflake"
PATH = "snowflake/python"


def get_snowflake_conn():
    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        session_parameters={
            "QUERY_TAG": "NFL-Snowflake",
        },
    )
    return conn


def get_stage_uri():
    return f"s3://{BUCKET}/{PATH}"


def create_handler_stage(
    conn: snowflake.connector.SnowflakeConnection, bucket_path: str
) -> None:
    """Create exteranl stage for handler script upload"""
    qry = (
        "create stage if not exists handler_snowpark "
        "storage_integration =  s3_integration"
        f"url = '{bucket_path}';"
    )

    conn.cursor().execute(qry)


def create_task():
    """create snwoflake task to execute Snowpark logic"""
    cmd = f"""
        create or replace task task_run_snowpark_handler
        warehouse = compute_wh
        schedule = '1 minute'
        when
            system$stream_has_data('rawstream2')
        as
        ...
    """
    pass
