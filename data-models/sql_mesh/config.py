"""
This is a python representation of what would be a
YAML-based config like this: 

gateways:
    local:
        connection:
        type: duckdb
        database: nfl_pbp.duckdb

    default_gateway: local

    model_defaults:
    dialect: duckdb
"""
import os
import pathlib

from sqlmesh.core.config import (
    AutoCategorizationMode,
    Config,
    CategorizerConfig,
    DuckDBConnectionConfig,
    ModelDefaultsConfig,
)

DATA_MODELS = pathlib.Path(os.path.abspath(__file__)).parent.parent
ROOT = DATA_MODELS.parent.resolve()
LOCAL_DB = os.path.join(os.sep, ROOT, "nfl_pbp.duckdb")

# A configuration used for SQLMesh tests.
test_config = Config(
    default_connection=DuckDBConnectionConfig(),
    auto_categorize_changes=CategorizerConfig(sql=AutoCategorizationMode.SEMI),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)

# Persistent DuckDB instance
# Naming this something other than config requires specifying it on CLI
config = Config(
    default_connection=DuckDBConnectionConfig(database=LOCAL_DB),
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
