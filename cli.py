import os
import pathlib

import boto3
import click

from pipeline.data import get_nfl_data, clean_data, get_filename
from pipeline.db import duck
from pipeline.db.snow import get_snowflake_conn, create_handler_stage, get_stage_uri


@click.group()
def cli():
    pass


@click.command()
@click.option("--val", help="Provide a value to echo")
def echo(val):
    click.echo(f"Input:\t{val}")


@click.command()
def upload_handler_to_stage():
    """
    Upload handler script to Snowflake external stage for use in Snowpark
    """
    handler = os.path.join(
        os.sep,
        str(pathlib.Path(os.path.abspath(__file__)).parent.resolve()),
        "handler_snowpark.py",
    )
    full_bucket = get_stage_uri()
    create_handler_stage(get_snowflake_conn(), full_bucket)
    client = boto3("s3")
    client.upload_file(handler, full_bucket, "handler_snowpark.py")


@click.command()
@click.option("--years", type=int, nargs=3)
@click.option("--bucket", type=str)
@click.option("--ext", type=str)
def load(years, bucket, ext):
    """
    Load specified NFL data into s3 for later use.
    Uses s3fs for direct upload to s3 in lieu of boto
    Extension options: csv, parquet
    """
    for year in years:
        data = get_nfl_data(year)
        data = clean_data(data)
        filename = get_filename(bucket, year, ext=ext)
        if filename.endswith('csv'):
            data.to_csv(filename, index=False, sep="|", encoding="utf-8")
        elif filename.endswith('parquet'):
            data.to_parquet(filename, index=False)
        else:
            raise ValueError("Provided extention not supported")

        print(f"Loaded to s3:\t{filename}")


@click.command()
def load_duck_db():
    """
    Load data into duckdb and create a local export
    """
    duck.load_duckdb()


if __name__ == "__main__":
    cli.add_command(echo)
    cli.add_command(upload_handler_to_stage)
    cli.add_command(load)
    cli.add_command(load_duck_db)

    cli()
